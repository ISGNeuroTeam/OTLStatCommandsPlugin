package ot.dispatcher.plugins.stats.commands

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import ot.dispatcher.plugins.stats.parsers.ReplaceParser
import ot.dispatcher.sdk.core.CustomException.E00012
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.core.extensions.StringExt._

/** =Abstract=
 * This class provides support of __'''percentile_approx'''__ otl command.
 *
 * __'''percentile_approx'''__ calculates the approximated percentile of values of defined column.
 *
 * __'''percentile_approx'''__ takes one required and two optional arguments:
 *
 * Required argument:
 *
 *    1.  _'''field'''_ - name of field to which the command will apply.
 *
 * Optional arguments:
 *
 *    1. __'''value'''__ - the value of percentage. Must be between 0.0 and 1.0. Default value: 1.0.
 *
 *    2. __'''accuracy'''__ - the value of approximation accuracy at the cost of memory for percentile. Must be positive integer number. Default value: 10000.
 *
 * =Usage examples=
 * OTL 1:
 * {{{| makeresults | eval a = 90,b=700 | append [makeresults | eval a =10,b=445] | append [makeresults | eval a =12,b=320]
  | append [makeresults | eval a =60,b=500] | append [makeresults | eval a =99,b=200] | append [makeresults | eval a =35,b=869]
  | append [makeresults | eval a =81,b=623] | append [makeresults | eval a =55,b=108] | percentile_approx b as b_Approx}}}
 * Result:
 *  {{{--+
|b_Approx|
+--------+
|     869|
+--------+}}}
 * OTL 2:
 * {{{| makeresults | eval a = 90,b=700 | append [makeresults | eval a =10,b=445] | append [makeresults | eval a =12,b=320]
  | append [makeresults | eval a =60,b=500] | append [makeresults | eval a =99,b=200] | append [makeresults | eval a =35,b=869]
  | append [makeresults | eval a =81,b=623] | append [makeresults | eval a =55,b=108] | percentile_approx a as a_VPA value = 0.64}}}
 * Result:
 *  {{{+
| a_VPA|
+------+
|    81|
+------+}}}
 * OTL 3:
 * {{{| makeresults | eval a = 90,b=700 | append [makeresults | eval a =10,b=445] | append [makeresults | eval a =12,b=320]
  | append [makeresults | eval a =60,b=500] | append [makeresults | eval a =99,b=200] | append [makeresults | eval a =35,b=869]
  | append [makeresults | eval a =81,b=623] | append [makeresults | eval a =55,b=108] | percentile_approx a as a_realVPA value = 0.28 accuracy = 1}}}
 * Result:
 * {{{+----+
| a_realVPA|
+----------+
|        10|
+----------+}}}
 * @constructor creates new instance of [[OTLRare]]
 * @param sq [[SimpleQuery]]
 */
class OTLPercentileApprox (sq: SimpleQuery, utils: PluginUtils) extends PluginCommand(sq, utils) with ReplaceParser {

  val flatFields: List[String] = returns.flatFields

  val flatNewFields: List[String] = returns.flatNewFields

  val field: String = if (flatFields.nonEmpty && !Array("value".addSurroundedBackticks, "accuracy".addSurroundedBackticks).contains(flatFields.head))
    flatFields.head.stripBackticks()
  else
    throw E00012(sq.searchId, "percentile_approx", "field")

  val percentage: Double = getKeyword("value").getOrElse(1.0).toString.toDoubleSafe match {
    case Some(value) => value match {
      case p if p >= 0.0 && p <= 1.0 => p
      case _ => throw new IllegalArgumentException(s"value should be from 0.0 to 1.0, but it is $value")
    }
    case _ => throw new IllegalArgumentException(s"value should has double type, but it has other type")
  }

  val accuracyDefault: Int = 10000

  val accuracy: Int = getKeyword("accuracy").getOrElse(accuracyDefault).toString.toIntSafe match {
    case Some(value) => value match {
      case f if f > 0 => f
      case _ => throw new IllegalArgumentException(s"accuracy should be greater than 0, but it has value $value")
    }
    case _ => throw new IllegalArgumentException(s"accuracy should has integer type, but it has other type")
  }

  val alias: String = createAlias()

  def transform(_df: DataFrame): DataFrame = {
    val exprText = if (accuracy == accuracyDefault) {
      s"percentile_approx($field, $percentage)"
    } else {
      s"percentile_approx($field, $percentage, $accuracy)"
    }
    val column = expr(exprText).as(alias)
    _df.agg(column)
  }

  private def createAlias(): String = {
    if (flatNewFields.nonEmpty && flatFields.nonEmpty && flatNewFields.head != flatFields.head.stripBackticks)
      flatNewFields.head
    else
      s"percentile_approx($field,$percentage${if (accuracy != accuracyDefault) "," + accuracy else ""})"
  }
}