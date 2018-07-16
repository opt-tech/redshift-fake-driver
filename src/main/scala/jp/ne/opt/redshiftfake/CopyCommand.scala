package jp.ne.opt.redshiftfake

import jp.ne.opt.redshiftfake.s3.S3Location

/**
 * Represents Redshift's COPY.
 */
case class CopyCommand(
  schemaName: Option[String],
  tableName: String,
  columnList: Option[Seq[String]],
  dataSource: CopyDataSource,
  credentials: Credentials,
  copyFormat: CopyFormat,
  dateFormatType: DateFormatType,
  timeFormatType: TimeFormatType,
  emptyAsNull: Boolean,
  delimiter: Char,
  nullAs: String,
  compression: FileCompressionParameter
) {
  val qualifiedTableName = schemaName match {
    case Some(schema) => s"$schema.$tableName"
    case _ => tableName
  }
}

sealed abstract class CopyDataSource
object CopyDataSource {
  case class S3(location: S3Location) extends CopyDataSource
}

sealed abstract class CopyFormat
object CopyFormat {
  case object Default extends CopyFormat
  case class Manifest(manifestLocation: S3Location) extends CopyFormat
  case class Json(jsonpathsLocation: Option[S3Location]) extends CopyFormat
}

sealed abstract class DateFormatType
object DateFormatType {
  case object Default extends DateFormatType
  case object Auto extends DateFormatType
  case class Custom(pattern: String) extends DateFormatType
}

sealed abstract class TimeFormatType
object TimeFormatType {
  case object Default extends TimeFormatType
  case object Auto extends TimeFormatType
  case object Epochsecs extends TimeFormatType
  case object EpochMillisecs extends TimeFormatType
  case class Custom(pattern: String) extends TimeFormatType
}

sealed abstract class FileCompressionParameter
object FileCompressionParameter {
  case object None extends FileCompressionParameter
  case object Gzip extends FileCompressionParameter
  case object Bzip2 extends FileCompressionParameter
}