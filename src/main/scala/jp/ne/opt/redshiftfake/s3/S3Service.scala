package jp.ne.opt.redshiftfake.s3

import java.io.ByteArrayInputStream

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.ServiceAbbreviations
import com.amazonaws.services.s3.{S3ClientOptions, AmazonS3Client}
import com.amazonaws.services.s3.model._
import jp.ne.opt.redshiftfake.{Global, Credentials}
import jp.ne.opt.redshiftfake.util.Loan._

import scala.annotation.tailrec
import scala.collection.JavaConverters._

/**
 * Provides features to access to Amazon S3.
 */
trait S3Service {
  /**
   * Returns a list of s3 objects have specified prefix.
   */
  def lsRecurse(location: S3Location)(credentials: Credentials): Seq[S3ObjectSummary]

  /**
   * Returns a content of s3 object as string for specified key.
   */
  def downloadAsString(location: S3Location)(credentials: Credentials): String

  /**
   * Upload a string content to specified location.
   */
  def uploadString(location: S3Location, content: String)(credentials: Credentials): Unit
}

class S3ServiceImpl(endpoint: String) extends S3Service {

  private[s3] def mkClient(credentials: Credentials) = {
    val client = credentials match {
      case Credentials.WithKey(accessKeyId, secretAccessKey) =>
        new AmazonS3Client(new BasicAWSCredentials(accessKeyId, secretAccessKey))
      case _ =>
        new AmazonS3Client()
    }

    client.setS3ClientOptions({
      val options = new S3ClientOptions
      options.setPathStyleAccess(true)

      // for compatibility
      val setChunkedEncodingDisabled = "setChunkedEncodingDisabled"
      val clazz = classOf[S3ClientOptions]
      if (clazz.getMethods.map(_.getName).contains(setChunkedEncodingDisabled)) {
        clazz.getMethod(setChunkedEncodingDisabled, classOf[Boolean]).invoke(options, java.lang.Boolean.valueOf(true))
      }

      options.setPathStyleAccess(true)
      options
    })

    client.setRegion(Global.region)
    if (endpoint != "s3://") {
      client.setEndpoint(endpoint)
    } else {
      client.setEndpoint(Global.region.getServiceEndpoint(ServiceAbbreviations.S3))
    }
    client
  }

  def lsRecurse(location: S3Location)(credentials: Credentials): Seq[S3ObjectSummary] = {
    val client = mkClient(credentials)

    @tailrec def iter(result: Seq[S3ObjectSummary], listing: ObjectListing): Seq[S3ObjectSummary] = {
      val summaries = listing.getObjectSummaries.asScala
      if (listing.isTruncated) {
        iter(result ++ summaries, client.listNextBatchOfObjects(listing))
      } else {
        result ++ summaries
      }
    }
    iter(Vector.empty, client.listObjects(location.bucket, location.prefix))
  }

  def downloadAsString(location: S3Location)(credentials: Credentials): String = {
    val client = mkClient(credentials)
    val request = new GetObjectRequest(location.bucket, location.prefix)
    using(client.getObject(request)) { obj =>
      io.Source.fromInputStream(obj.getObjectContent).mkString
    }
  }

  def uploadString(location: S3Location, content: String)(credentials: Credentials): Unit = {
    val bytes = content.getBytes("UTF-8")
    val stream = new ByteArrayInputStream(bytes)
    val metadata = new ObjectMetadata()
    metadata.setContentLength(bytes.length)

    val request = new PutObjectRequest(location.bucket, location.prefix, stream, metadata)
    mkClient(credentials).putObject(request)
  }
}
