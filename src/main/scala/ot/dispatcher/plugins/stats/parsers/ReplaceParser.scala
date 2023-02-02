package ot.dispatcher.plugins.stats.parsers

import ot.dispatcher.sdk.core.{Return, ReturnField}
import ot.dispatcher.sdk.core.parsers.DefaultParser

import scala.util.matching.Regex
import ot.dispatcher.sdk.core.extensions.StringExt._

trait ReplaceParser extends DefaultParser {

  override def returnsParser: (String, Set[String]) => Return = (args: String, seps: Set[String]) => {
    val argsFiltered = excludeKeywords(excludePositionals(args, seps), keywordsParser(args))
    val splitSpaceCommaKeepQuotes: Regex = """(?:".*?"|[^,\s])+""".r
    val repl = splitSpaceCommaKeepQuotes
      .findAllIn(argsFiltered)
      .map(x => x.replace(" ", "&ph"))
      .toList
      .mkString(" ")
    val rexAsWith = """(\S+)( (as|with) (\S+))?(\s|,|$)""".r
    val fields = rexAsWith
      .findAllIn(repl)
      .matchData
      .map(x => ReturnField(Option(x.group(4)).getOrElse(x.group(1)), x.group(1)))
      .map { case ReturnField(nf, f) => ReturnField(
        nf.replace("&ph", " ").strip("'"),
        f.replace("&ph", " ").strip("'").addSurroundedBackticks)
      }
      .toList
    Return(fields)
  }
}