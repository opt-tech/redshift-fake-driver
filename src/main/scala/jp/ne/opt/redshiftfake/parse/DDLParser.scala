package jp.ne.opt.redshiftfake.parse

import scala.language.implicitConversions

object DDLParser extends BaseParser {

  val distStyleRegex = s"(?i)DISTSTYLE$space(EVEN|KEY|ALL)".r
  val distKeyRegex = s"(?i)DISTKEY$space\\($space$identifier$space\\)".r
  val sortKeyRegex = {
    val columnsRegex = s"$space$identifier($space,$space$identifier)*$space"

    s"(?i)(COMPOUND|INTERLEAVED)?${space}SORTKEY$space\\($columnsRegex\\)".r
  }

  def sanitize(ddl: String): String = {
    Seq(distStyleRegex, distKeyRegex, sortKeyRegex).foldLeft(ddl) { (current, regex) =>
      regex.replaceAllIn(current, "")
    }
  }
}
