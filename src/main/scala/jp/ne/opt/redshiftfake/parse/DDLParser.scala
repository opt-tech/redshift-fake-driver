package jp.ne.opt.redshiftfake.parse

import scala.language.implicitConversions
import scala.util.matching.Regex

class DDLParser extends BaseParser {

  val alterTableHandler = new AlterTableHandler

  val distStyleRegex = s"(?i)DISTSTYLE$space(EVEN|KEY|ALL)".r
  val distKeyRegex = s"(?i)DISTKEY$space\\($space$quotedIdentifier$space\\)".r
  val sortKeyRegex = {
    val columnsRegex = s"$space$quotedIdentifier($space,$space$quotedIdentifier)*$space"

    s"(?i)(COMPOUND|INTERLEAVED)?${space}SORTKEY$space\\($columnsRegex\\)".r
  }
  val encodeRegex = s"(?i)${space}ENCODE$space$identifier$space".r

  val identityRegex = s"(?i)(BIGINT|INT)${space}IDENTITY\\([0-9]*$space,$space[0-9]*\\)".r

  def sanitize(ddl: String): String = {

    var sanitized = Seq(distStyleRegex, distKeyRegex, sortKeyRegex, encodeRegex).foldLeft(ddl) { (current, regex) =>
      regex.replaceAllIn(current, "")
    }

    sanitized = identityRegex.replaceAllIn(sanitized, "BIGSERIAL")

    if(alterTableHandler.matches(sanitized)){
      val alterTableParsed = alterTableHandler.handle(sanitized)
      System.out.println("Alter table parsed " + alterTableParsed)
      alterTableParsed
    }
      //Replace default functions in create table statement
    else {

      val defaultRegex = "(?i)DEFAULT ([_a-zA-Z]\\w*(\\(\\))?)".r

      defaultRegex.findAllMatchIn(ddl).foldLeft(sanitized)((x: String, y: Regex.Match) => {

        val defaultValue = new DefaultParser().convert(y.group(1))
        val (original, parsedDefaultValue) = defaultValue.get

        if (parsedDefaultValue.nonEmpty) {
          System.out.println("Sanitised function with default: " + x.replace(original, parsedDefaultValue.get))
          x.replace(original, parsedDefaultValue.get)
        } else {
          x
        }
      })
    }
  }
}
