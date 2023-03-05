package jp.ne.opt.redshiftfake.read

import com.github.tototoshi.csv._
import jp.ne.opt.redshiftfake.{Column, Row}

case class InvalidCsvException(message: String) extends RuntimeException

class CsvReader(csvRow: String, delimiterChar: Char, nullAs: String) {

  private[this] object csvFormat extends CSVFormat {
    val delimiter: Char = delimiterChar
    val quoteChar: Char = '"'
    val treatEmptyLineAsNil: Boolean = false
    val escapeChar: Char = '\\'
    val lineTerminator: String = "\r\n"
    val quoting: Quoting = QUOTE_ALL
  }
  private[this] val parser = new CSVParser(csvFormat)
  private[this] val emptyField = s"${csvFormat.quoteChar}${csvFormat.quoteChar}"

  val toRow: Row = {
    val parsed = parser.parseLine(csvRow)
    parsed.map { xs => Row(xs.map {
      column => Column(if (column.nonEmpty && column != nullAs) Some(column) else None)
    })}.getOrElse(throw InvalidCsvException(s"invalid csv row : $csvRow"))
  }
}
