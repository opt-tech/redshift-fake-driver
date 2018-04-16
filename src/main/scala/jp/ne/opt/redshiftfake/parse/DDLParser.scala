package jp.ne.opt.redshiftfake.parse

import scala.language.implicitConversions

object DDLParser extends BaseParser {

  val distStyleRegex = s"(?i)DISTSTYLE$space(EVEN|KEY|ALL)".r
  val distKeyRegex = s"(?i)DISTKEY$space\\($space$quotedIdentifier$space\\)".r
  val sortKeyRegex = {
    val columnsRegex = s"$space$quotedIdentifier($space,$space$quotedIdentifier)*$space"

    s"(?i)(COMPOUND|INTERLEAVED)?${space}SORTKEY$space\\($columnsRegex\\)".r
  }
  val encodeRegex = s"(?i)${space}ENCODE$space$identifier$space".r

  val identityBigintRegexp = s"(?i)(BIGINT)${space}IDENTITY$space\\($space$number\\,$space$number\\)".r

  def sanitize(ddl: String): String = {
    val cleanedDdl = Seq(distStyleRegex, distKeyRegex, sortKeyRegex, encodeRegex).foldLeft(ddl) { (current, regex) =>
      regex.replaceAllIn(current, "")
    }

    Seq(identityBigintRegexp).foldLeft(cleanedDdl) { (current, regex) =>
      regex.replaceAllIn(current, "bigserial")
    }
  }
}
