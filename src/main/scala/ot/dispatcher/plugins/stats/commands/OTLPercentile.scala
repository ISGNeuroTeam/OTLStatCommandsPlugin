package ot.dispatcher.plugins.stats.commands

import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}
import ot.dispatcher.sdk.core.SimpleQuery
import org.apache.spark.sql.functions._
import ot.dispatcher.plugins.stats.parsers.ReplaceParser
import ot.dispatcher.sdk.core.CustomException.E00012
import ot.dispatcher.sdk.core.extensions.StringExt._

/** =Abstract=
 * This class provides support of __'''percentile'''__ otl command.
 *
 * __'''percentile'''__ calculates the percentile of values of defined column.
 *
 * __'''percentile'''__ takes one required and two optional arguments:
 *
 * Required argument:
 *
 *    1.  _'''field'''_ - name of field to which the command will apply.
 *
 * Optional arguments:
 *
 *    1. __'''value'''__ - the value of percentage. Must be between 0.0 and 1.0. Default value: 1.0.
 *
 *    2. __'''frequency'''__ - the value of frequency for percentile. Must be positive integer number. Default value: 1.
 *
 * =Usage examples=
 * OTL 1:
 * {{{| makeresults | eval a = 10,b=400 | append [makeresults | eval a =40,b=600] | append [makeresults | eval a =70,b=800]
  | append [makeresults | eval a =25,b=386] | append [makeresults | eval a =11,b=895] | append [makeresults | eval a =45,b=223]
  | append [makeresults | eval a =90,b=950] | append [makeresults | eval a =60,b=787] | percentile a as a_perc}}}
 * Result:
 *  {{{+
|a_perc|
+------+
|    90|
+------+}}}
 * OTL 2:
 * {{{| makeresults | eval a = 10,b=400 | append [makeresults | eval a =40,b=600] | append [makeresults | eval a =70,b=800]
  | append [makeresults | eval a =25,b=386] | append [makeresults | eval a =11,b=895] | append [makeresults | eval a =45,b=223]
  | append [makeresults | eval a =90,b=950] | append [makeresults | eval a =60,b=787] | percentile b as b_valuedPerc value = 0.72}}}
 * Result:
 *  {{{+----------+
|     b_valuedPerc|
+-----------------+
|803.8000000000001|
+-----------------+}}}
 * OTL 3:
 * {{{| makeresults | eval a = 10,b=400 | append [makeresults | eval a =40,b=600] | append [makeresults | eval a =70,b=800]
  | append [makeresults | eval a =25,b=386] | append [makeresults | eval a =11,b=895] | append [makeresults | eval a =45,b=223]
  | append [makeresults | eval a =90,b=950] | append [makeresults | eval a =60,b=787] | percentile b as b_PercWithFreq value = 0.48 frequency = 3}}}
 * Result:
 * {{{+-----------+
|   b_PercWithFreq|
+-----------------+
|607.4799999999998|
+-----------------+}}}
 * @constructor creates new instance of [[OTLRare]]
 * @param sq [[SimpleQuery]]
 */
class OTLPercentile(sq: SimpleQuery, utils: PluginUtils) extends PluginCommand(sq, utils) with ReplaceParser {

  val flatFields = returns.flatFields

  val flatNewFields = returns.flatNewFields

  val field: String = if (flatFields.nonEmpty && !Array("value".addSurroundedBackticks, "frequency".addSurroundedBackticks).contains(flatFields.head) )
    flatFields.head.stripBackticks()
  else
    throw E00012(sq.searchId, "percentile", "field")

  val percentage: Double = getKeyword("value").getOrElse(1.0).toString.toDoubleSafe match {
    case Some(value) => value match {
      case p if p >= 0.0 && p <= 1.0 => p
      case _ => throw new IllegalArgumentException(s"value should be from 0.0 to 1.0, but it is $value")
    }
    case _ => throw new IllegalArgumentException(s"value should has double type, but it has other type")
  }

  val frequencyDefault = 1

  val frequency: Int = getKeyword("frequency").getOrElse(frequencyDefault).toString.toIntSafe match {
    case Some(value) => value match {
      case f if f > 0 => f
      case _ => throw new IllegalArgumentException(s"frequency should be greater than 0, but it has value $value")
    }
    case _ => throw new IllegalArgumentException(s"frequency should has integer type, but it has other type")
  }

  val alias: String = createAlias()

  def transform(_df: DataFrame): DataFrame = {
    val fake = "__fake__"
    val dfWorked = _df.withColumn(fake, lit(fake))
    val exprText = if (frequency == frequencyDefault) {
      s"percentile($field, $percentage)"
    } else {
      s"percentile($field, $percentage, $frequency)"
    }
    val column = expr(exprText).as(alias)
    dfWorked.groupBy(fake).agg(column).drop(fake)
  }

  private def createAlias(): String = {
    if (flatNewFields.nonEmpty && flatFields.nonEmpty && flatNewFields.head != flatFields.head.stripBackticks)
      flatNewFields.head
    else
      s"percentile($field,$percentage${if (frequency > frequencyDefault) "," + frequency else ""})"
  }

}