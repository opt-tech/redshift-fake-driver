package jp.ne.opt.redshiftfake.s3

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.{S3ClientOptions, AmazonS3Client}
import com.amazonaws.services.s3.model.{GetObjectRequest, ObjectListing, S3ObjectSummary}
import jp.ne.opt.redshiftfake.Credentials
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
  def lsRecurse(location: S3Location): Seq[S3ObjectSummary]

  /**
   * Returns a content of s3 object as string for specified key.
   */
  def downloadAsString(location: S3Location): String

  /**
   * Upload a string content to specified location.
   */
  def uploadString(location: S3Location, content: String): Unit
}

class S3ServiceImpl(endpoint: String, credentials: Credentials) extends S3Service {

  private[this] def mkClient = {
    val client = credentials match {
      case Credentials.WithKey(accessKeyId, secretAccessKey) =>
        new AmazonS3Client(new BasicAWSCredentials(accessKeyId, secretAccessKey))
      case _ =>
        new AmazonS3Client()
    }
    client.setS3ClientOptions(
      S3ClientOptions.builder().setPathStyleAccess(true).disableChunkedEncoding().build()
    )
    client.setEndpoint(endpoint)
    client
  }

  def lsRecurse(location: S3Location): Seq[S3ObjectSummary] = {
    val client = mkClient

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

  def downloadAsString(location: S3Location): String = {
    val client = mkClient
    val request = new GetObjectRequest(location.bucket, location.prefix)
    using(client.getObject(request)) { obj =>
      io.Source.fromInputStream(obj.getObjectContent).mkString
    }
  }

  def uploadString(location: S3Location, content: String): Unit = {
    mkClient.putObject(location.bucket, location.prefix, content)
  }
}
