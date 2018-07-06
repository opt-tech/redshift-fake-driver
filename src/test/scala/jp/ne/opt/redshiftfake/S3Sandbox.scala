package jp.ne.opt.redshiftfake

import java.net.URI

import com.amazonaws.auth.{AWSCredentials, BasicAWSCredentials}
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.s3.AmazonS3Client
import org.gaul.s3proxy.{AuthenticationType, S3Proxy}
import org.jclouds.ContextBuilder
import org.jclouds.blobstore.BlobStoreContext
import org.scalatest.{BeforeAndAfterAll, Suite}

trait S3Sandbox extends BeforeAndAfterAll {this: Suite =>

  val dummyCredentials:  Credentials.WithKey
  val s3Endpoint: String

  var s3Proxy: S3Proxy = _

  override def beforeAll(): Unit = {
    val blobContext: BlobStoreContext = ContextBuilder
      .newBuilder("transient")
      .build(classOf[BlobStoreContext])

    s3Proxy = S3Proxy.builder
      .blobStore(blobContext.getBlobStore)
      .awsAuthentication(AuthenticationType.AWS_V4, dummyCredentials.accessKeyId, dummyCredentials.secretAccessKey)
      .endpoint(URI.create(s3Endpoint))
      .build
    s3Proxy.start()
  }

  override def afterAll(): Unit = {
    s3Proxy.stop()
  }

  def createS3Client(s3Region: String): AmazonS3Client = {
    val credentials: AWSCredentials = new BasicAWSCredentials(dummyCredentials.accessKeyId, dummyCredentials.secretAccessKey)
    val client = new AmazonS3Client(credentials)
    client.setRegion(RegionUtils.getRegion(s3Region))
    client.setEndpoint(s3Endpoint)

    client
  }
}
