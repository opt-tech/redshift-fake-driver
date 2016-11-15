package jp.ne.opt.redshiftfake

import jp.ne.opt.redshiftfake.s3.Credentials

/**
 * Global variables should be avoided in general,
 * but s3 endpoint must be provided as global variable to parse datasource of COPY queries.
 */
object Global {
  val endpoint: String = ""
  val s3Credentials: Credentials = Credentials.WithKey("", "")
}
