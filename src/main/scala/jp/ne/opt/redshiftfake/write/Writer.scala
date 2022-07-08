package jp.ne.opt.redshiftfake.write

import java.io.ByteArrayOutputStream

import com.github.tototoshi.csv._
import jp.ne.opt.redshiftfake.{Row, UnloadCommand}
import jp.ne.opt.redshiftfake.s3.S3Service
import jp.ne.opt.redshiftfake.util.Loan.using

/**
 * Provides features to write unloaded data to S3.
 */
class Writer(unloadCommand: UnloadCommand, s3Service: S3Service) {

  private[this] object csvFormat extends CSVFormat {
    val delimiter: Char = unloadCommand.delimiter
    val quoteChar: Char = '"'
    val treatEmptyLineAsNil: Boolean = false
    val escapeChar: Char = '\\'
    val lineTerminator: String = "\n"
    val quoting: Quoting = if (unloadCommand.addQuotes) QUOTE_ALL else QUOTE_NONE
  }

  def write(rows: Seq[Row]): Unit = {
    val stream = new ByteArrayOutputStream()

    using(CSVWriter.open(stream)(csvFormat)) { csvWriter =>
      rows.foreach { row =>
        csvWriter.writeRow(row.columns.map(_.rawValue.getOrElse("")))
      }
    }
    val result = stream.toString("UTF-8")
    val resultKey = s"${unloadCommand.destination.prefix}000"

    if (unloadCommand.createManifest) {
      val resultUrl = unloadCommand.destination.copy(prefix = resultKey).full
      val manifest = s"""{"entries":[{"url":"$resultUrl"}]}"""
      val manifestKey = s"${unloadCommand.destination.prefix}manifest"

      s3Service.uploadString(unloadCommand.destination.copy(prefix = manifestKey), manifest)(unloadCommand.credentials)
    }

    s3Service.uploadString(unloadCommand.destination.copy(prefix = resultKey), result)(unloadCommand.credentials)
  }
}
