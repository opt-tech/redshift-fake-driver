package jp.ne.opt.redshiftfake

import java.net.URI

import software.amazon.awssdk.auth.credentials.{
  StaticCredentialsProvider,
  AwsBasicCredentials,
}
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.regions.Region
import com.adobe.testing.s3mock.S3MockApplication
import org.scalatest.{BeforeAndAfterAll, Suite}

trait S3Sandbox extends BeforeAndAfterAll {this: Suite =>

  val dummyCredentials:  Credentials.WithKey
  val s3Endpoint: String
  var s3Mock: S3MockApplication = _

  override def beforeAll(): Unit = {
    s3Mock = S3MockApplication.start()
  }

  override def afterAll(): Unit = {
    s3Mock.stop()
  }

  def createS3Client(s3Region: String): S3Client = {
    val credentials = AwsBasicCredentials.create(dummyCredentials.accessKeyId, dummyCredentials.secretAccessKey)
    val client = S3Client.builder()
      .credentialsProvider(
        StaticCredentialsProvider.create(credentials)
      )
      .region(Region.of(s3Region))
      .endpointOverride(URI.create(s3Endpoint))
      .build()

    client
  }
}
