package jp.ne.opt.redshiftfake.read

import com.github.tototoshi.csv._
import jp.ne.opt.redshiftfake.{Column, Row}

case class InvalidCsvException(message: String) extends RuntimeException

class CsvReader(csvRow: String) {

  private[this] object csvFormat extends CSVFormat {
    val delimiter: Char = '|'
    val quoteChar: Char = '"'
    val treatEmptyLineAsNil: Boolean = false
    val escapeChar: Char = '\\'
    val lineTerminator: String = "\r\n"
    val quoting: Quoting = QUOTE_ALL
  }
  private[this] val parser = new CSVParser(csvFormat)
  private[this] val emptyField = s"${csvFormat.quoteChar}${csvFormat.quoteChar}"

  val toRow: Row = {
    // to recognize last field when the field is empty.
    val parsed = parser.parseLine(csvRow + csvFormat.delimiter + emptyField)
    parsed.map { xs => Row(xs.map {
      column => Column(if (column.nonEmpty) Some(column) else None)
    })}.getOrElse(throw InvalidCsvException(s"invalid csv row : $csvRow"))
  }
}
