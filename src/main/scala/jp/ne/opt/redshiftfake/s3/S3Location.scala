package jp.ne.opt.redshiftfake.s3

import jp.ne.opt.redshiftfake.Global

/**
 * Represents a specific path on S3.
 */
case class S3Location(bucket: String, prefix: String) {
  val path = s"$bucket/$prefix"
  val full = Global.s3Endpoint + path
}
