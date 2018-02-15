package jp.ne.opt.redshiftfake.parse

import scala.language.implicitConversions

object DDLParser extends BaseParser {

  val distStyleRegex = s"(?i)DISTSTYLE$space(EVEN|KEY|ALL)".r
  val distKeyRegex = s"(?i)DISTKEY$space\\($space$identifier$space\\)".r
  val sortKeyRegex = {
    val columnsRegex = s"$space$identifier($space,$space$identifier)*$space"

    s"(?i)(COMPOUND|INTERLEAVED)?${space}SORTKEY$space\\($columnsRegex\\)".r
  }
  val encodeRegex = s"(?i)${space}ENCODE$space$identifier$space".r

  //https://docs.aws.amazon.com/redshift/latest/dg/r_ALTER_TABLE_APPEND.html
  //ALTER TABLE target_table_name APPEND FROM source_table_name[ IGNOREEXTRA | FILLTARGET ]
  //It is just for moving data, so we can ignore it and the schema will not be affected
  //The closest postgres equivalent is WITH https://stackoverflow.com/questions/2974057/move-data-from-one-table-to-another-postgresql-edition
  //However, that requires the columns to match, whereas this allows one or other of the tables to have extra or ignored columns
  val alterTableAppendRegex = s"(?i)ALTER${space}TABLE${space}${identifier}${space}APPEND${space}FROM${space}${identifier}(${space}(IGNOREEXTRA|FILLTARGET))?".r ///

  def sanitize(ddl: String): String = {
    Seq(distStyleRegex, distKeyRegex, sortKeyRegex, encodeRegex, alterTableAppendRegex).foldLeft(ddl) { (current, regex) =>
      regex.replaceAllIn(current, "")
    }
  }
}
