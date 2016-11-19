package jp.ne.opt.redshiftfake

import jp.ne.opt.redshiftfake.s3.S3Location

case class UnloadCommand(
  selectStatement: String,
  destination: S3Location,
  credentials: Credentials,
  createManifest: Boolean,
  delimiter: Char,
  addQuotes: Boolean
)
