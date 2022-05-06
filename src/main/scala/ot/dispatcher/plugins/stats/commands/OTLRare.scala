package ot.dispatcher.plugins.stats.commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, lit, row_number}
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.core.extensions.StringExt._
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}

class OTLRare(sq: SimpleQuery, utils: PluginUtils) extends PluginCommand(sq, utils, seps = Set("by")) {
  override def transform (_df: DataFrame): DataFrame = {
    val limit = args.split(" ").headOption match {
      case Some(lim) => lim.toIntSafe match {
        case Some(v) => if (v == 0) _df.count else v
        case _ => 10
      }
      case _ => return _df
    }
    val fields = returns.flatFields.filter(ff => ff.stripBackticks() != limit.toString && ff.stripBackticks() != 0.toString)
    val groups = getPositional("by") match {
      case None | Some(List()) => List()
      case Some(l) => l.map(s => s.stripBackticks())
    }
    //Dataset, grouping by top-applying columns or by 'by'-param column + top-applying columns with adding column of count by each group
    val dfCount = groups ++ fields match {
      case head :: tail =>
        _df.groupBy(head, tail: _*).agg(count("*").alias("count"))
      case _ => return _df
    }
    //Windowed func spec for cases of 'by'-param existing and not existing
    val w = groups match {
      case h :: t => Window.partitionBy(h, t: _*).orderBy(col("count"))
      case _ => Window.orderBy(col("count"))
    }
    //Limiting of entries: if 'by'-param exists, limiting in each group
    val dfWindowed = dfCount.withColumn("rn", row_number.over(w))
    val dfLimit = dfWindowed.filter(col("rn") <= limit)
      .drop("rn")
    //Defining of total count of entries in dataset or in each group (if 'by-param exists') and joining limited dataset with totals-containing dataset
    val dfJoined = groups match {
      case h :: t =>
        val jdf = _df.groupBy(h, t: _*).agg(count("*").alias("total"))
        dfLimit.join(jdf, groups)
      case _ =>
        val jdf = _df.agg(count("*").alias("total"))
        dfLimit.crossJoin(jdf)
    }
    //Defining percents
    dfJoined.withColumn("percent", lit(100) * col("count") / col("total"))
      .drop("total")
  }
}