package jp.ne.opt.redshiftfake

import software.amazon.awssdk.regions.Region

/**
 * S3 endpoint must be provided as global variable to parse datasource of COPY commands.
 */
object Global {
  def s3Endpoint: String = sys.props.getOrElse("fake.awsS3Endpoint", "s3://")
  def s3Scheme: String = sys.props.getOrElse("fake.awsS3Scheme", s3Endpoint)
  def region: Region = Region.of(sys.props.getOrElse("fake.awsRegion", Region.AP_NORTHEAST_1.id()))
}
