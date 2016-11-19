package jp.ne.opt.redshiftfake

/**
 * S3 endpoint must be provided as global variable to parse datasource of COPY commands.
 */
object Global {
  def s3Endpoint: String = sys.props.getOrElse("fake.s3Endpoint", "s3://")
}
