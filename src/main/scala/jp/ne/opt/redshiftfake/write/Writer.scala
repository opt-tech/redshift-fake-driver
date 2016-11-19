package jp.ne.opt.redshiftfake.write

import java.io.ByteArrayOutputStream

import com.github.tototoshi.csv.CSVWriter
import jp.ne.opt.redshiftfake.{Global, Row, UnloadCommand}
import jp.ne.opt.redshiftfake.s3.S3Service
import jp.ne.opt.redshiftfake.util.Loan.using

class Writer(unloadCommand: UnloadCommand, s3Service: S3Service) {

  def write(rows: Seq[Row]): Unit = {
    val stream = new ByteArrayOutputStream()
    using(CSVWriter.open(stream)) { csvWriter =>
      rows.foreach { row =>
        csvWriter.writeRow(row.columns.map(_.rawValue.getOrElse("")))
      }
    }
    val result = stream.toString("UTF-8")
    val resultKey = s"${unloadCommand.destination.prefix}_0000_part_00"
    val resultUrl = s"${Global.s3Endpoint}${unloadCommand.destination.bucket}/$resultKey"
    val manifest = s"""{"entries":{"url":"$resultUrl"}}"""
    val manifestKey = s"${unloadCommand.destination.prefix}_manifest"

    s3Service.uploadString(unloadCommand.destination.copy(prefix = manifestKey), manifest)
    s3Service.uploadString(unloadCommand.destination.copy(prefix = resultKey), result)
  }
}
