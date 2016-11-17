package jp.ne.opt.redshiftfake

/**
 * S3 endpoint must be provided as global variable to parse datasource of COPY commands.
 */
object Global {
  val s3Endpoint: String = "http://localhost:9444/"
  val s3Credentials: Credentials = Credentials.WithKey("foo", "bar")
}
