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
  copyFormat: CopyFormat
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
  case class Json(jsonpathsLocation: Option[S3Location]) extends CopyFormat
}
