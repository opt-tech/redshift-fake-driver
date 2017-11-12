package jp.ne.opt.redshiftfake

import com.amazonaws.regions.{Regions, Region, RegionUtils}

/**
 * S3 endpoint must be provided as global variable to parse datasource of COPY commands.
 */
object Global {
  def s3Endpoint: String = sys.props.getOrElse("fake.awsS3Endpoint", "s3://")
  def s3Scheme: String = sys.props.getOrElse("fake.awsS3Scheme", s3Endpoint)
  def region: Region = RegionUtils.getRegion(sys.props.getOrElse("fake.awsRegion", Regions.AP_NORTHEAST_1.getName))
}
