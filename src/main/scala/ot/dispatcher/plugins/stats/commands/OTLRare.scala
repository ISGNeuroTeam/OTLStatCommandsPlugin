package ot.dispatcher.plugins.stats.commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, lit, row_number}
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.core.extensions.StringExt._
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}

/** =Abstract=
 * This class provides support of __'''rare'''__ otl command.
 *
 * __'''rare'''__ finds the least common values in the selected fields of event,
 *                calculates the number and percentage of how often the selected values occur in events.
 *
 * __'''rare'''__ takes one required and two optional arguments:
 *
 * Required argument:
 *
 *    1.  _'''field-list'''_ - comma-separated field names to which the command will apply.
 *
 * Optional arguments:
 *
 *    1. __'''limit'''__ - the number of result rows to be output in the result. The default is 10.
 *    If value of this argument is 0, than all rows will be output in the result.
 *
 *    2.__'''by'''__ - defines the field by which grouping occurs in the table.
 *
 * =Usage examples=
 * * OTL 1:
 * {{{| makeresults | eval a = 10,b=200 | append [makeresults | eval a =20,b=300] | append [makeresults | eval a =40,b=400]
 * | append [makeresults | eval a =30,b=400] | append [makeresults | eval a =20,b=700] | append [makeresults | eval a =10,b=300]
 * | append [makeresults | eval a =30,b=200] | append [makeresults | eval a =10,b=500] | rare a}}}
 * Result:
 * {{{+------+--------+
|    a| count| percent|
+---  +------+--------+
|   40|     1|    12.5|
|   20|     2|      25|
|   30|     2|      25|
|   10|     3|    37.5|
+-----+------+--------+}}}
 * OTL 2:
 * {{{| makeresults | eval a = 10,b=200 | append [makeresults | eval a =20,b=300] | append [makeresults | eval a =40,b=400]
 * | append [makeresults | eval a =30,b=400] | append [makeresults | eval a =20,b=700] | append [makeresults | eval a =10,b=300]
 * | append [makeresults | eval a =30,b=200] | append [makeresults | eval a =10,b=500] | rare 2 a}}}
 * Result:
 * {{{+------+--------+
|    a| count| percent|
+---  +------+--------+
|   40|     1|    12.5|
|   20|     2|      25|
+-----+------+--------+}}}
 * OTL 3:
 * {{{| makeresults | eval a = 10,b=200 | append [makeresults | eval a =20,b=300] | append [makeresults | eval a =40,b=400]
 * | append [makeresults | eval a =30,b=400] | append [makeresults | eval a =20,b=700] | append [makeresults | eval a =10,b=300]
 * | append [makeresults | eval a =30,b=200] | append [makeresults | eval a =10,b=500] | rare 0 b}}}
 * Result:
 * {{{+------+--------+
|    b| count| percent|
+---  +------+--------+
|  500|     1|    12,5|
|  700|     1|    12,5|
|  300|     2|      25|
|  200|     2|      25|
|  400|     2|      25|
+-----+------+--------+}}}
 * OTL 4:
 * {{{| makeresults | eval a = 10,b=200 | append [makeresults | eval a =20,b=300] | append [makeresults | eval a =40,b=400]
 * | append [makeresults | eval a =30,b=400] | append [makeresults | eval a =20,b=700] | append [makeresults | eval a =10,b=300]
 * | append [makeresults | eval a =30,b=200] | append [makeresults | eval a =10,b=500] | rare 3 b}}}
 * Result:
 * {{{+------+--------+
|    b| count| percent|
+---  +------+--------+
|  500|     1|    12,5|
|  700|     1|    12,5|
|  300|     2|      25|
+-----+------+--------+}}}
 * @constructor creates new instance of [[OTLRare]]
 * @param sq [[SimpleQuery]]
 */
class OTLRare(sq: SimpleQuery, utils: PluginUtils) extends PluginCommand(sq, utils, seps = Set("by")) {
  override def transform (_df: DataFrame): DataFrame = {
    val dfView = _df.collect()
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
    //Dataset, grouping by rare-applying columns or by 'by'-param column + rare-applying columns with adding column of count by each group
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
    //Defining of total count of entries in dataset or in each group (if 'by-param' exists) and joining limited dataset with totals-containing dataset
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