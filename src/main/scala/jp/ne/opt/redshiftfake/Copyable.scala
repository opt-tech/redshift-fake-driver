package jp.ne.opt.redshiftfake

import com.amazonaws.auth.BasicAWSCredentials

case class CopyAttribute(
  credentials: Option[BasicAWSCredentials],
  removeQuotes: Boolean,
  emptyAsNull: Boolean,
  blankAsNull: Boolean,
  delimiter: String = "|",
  timeFormat: String = "",
  gzip: Boolean
)

trait Copyable {

}
