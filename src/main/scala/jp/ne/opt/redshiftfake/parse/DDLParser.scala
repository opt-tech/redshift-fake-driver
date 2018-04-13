package jp.ne.opt.redshiftfake.parse

import scala.language.implicitConversions

object DDLParser extends BaseParser {

  val alterTableHandler = new AlterTableHandler

  val distStyleRegex = s"(?i)DISTSTYLE$space(EVEN|KEY|ALL)".r
  val distKeyRegex = s"(?i)DISTKEY$space\\($space$quotedIdentifier$space\\)".r
  val sortKeyRegex = {
    val columnsRegex = s"$space$quotedIdentifier($space,$space$quotedIdentifier)*$space"

    s"(?i)(COMPOUND|INTERLEAVED)?${space}SORTKEY$space\\($columnsRegex\\)".r
  }
  val encodeRegex = s"(?i)${space}ENCODE$space$identifier$space".r

  def sanitize(ddl: String): String = {

    var sanitized = Seq(distStyleRegex, distKeyRegex, sortKeyRegex, encodeRegex).foldLeft(ddl) { (current, regex) =>
      regex.replaceAllIn(current, "")
    }

    if(alterTableHandler.matches(sanitized)){
      alterTableHandler.handle(sanitized)
    }
    else {
      sanitized
    }
  }
}
