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
  def lsRecurse(bucket: String, prefix: String): Seq[S3ObjectSummary]

  /**
   * Returns a content of s3 object as string for specified key.
   */
  def downloadAsString(bucket: String, key: String): String
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

  def lsRecurse(bucket: String, prefix: String): Seq[S3ObjectSummary] = {
    val client = mkClient

    @tailrec def iter(result: Seq[S3ObjectSummary], listing: ObjectListing): Seq[S3ObjectSummary] = {
      val summaries = listing.getObjectSummaries.asScala
      if (listing.isTruncated) {
        iter(result ++ summaries, client.listNextBatchOfObjects(listing))
      } else {
        result ++ summaries
      }
    }
    iter(Vector.empty, client.listObjects(bucket, prefix))
  }

  def downloadAsString(bucket: String, key: String): String = {
    val client = mkClient
    val request = new GetObjectRequest(bucket, key)
    using(client.getObject(request)) { obj =>
      io.Source.fromInputStream(obj.getObjectContent).mkString
    }
  }

  def uploadString(bucket: String, key: String, content: String): Unit = {
    mkClient.putObject(bucket, key, content)
  }

  def createBucket(name: String): Unit = { mkClient.createBucket(name) }
}
