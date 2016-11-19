package jp.ne.opt.redshiftfake

import jp.ne.opt.redshiftfake.s3.S3Location

case class UnloadCommand(
  selectStatement: String,
  destination: S3Location,
  credentials: Credentials,
  manifestPath: Option[String],
  delimiter: String,
  addQuotes: Boolean
)
