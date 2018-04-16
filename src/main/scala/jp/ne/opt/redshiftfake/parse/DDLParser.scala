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
      val alterTableParsed = alterTableHandler.handle(sanitized)
      System.out.println("Alter table parsed " + alterTableParsed)
      alterTableParsed
    }
      //Replace default functions in create table statement
    else if(DefaultParser.matches(ddl)) {
      if (DefaultParser.isFunction(ddl)) {
        val (original, parsedDefaultValue) = DefaultParser.convertFunction(ddl).get
        System.out.println("Sanitised function with default: " + sanitized.replace(original, parsedDefaultValue))
        sanitized.replace(original, parsedDefaultValue)
      }
      else {
        sanitized
      }
    }
    else{
      System.out.println("Sanitised function: " +sanitized)
      sanitized
    }
  }
}
