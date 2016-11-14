package jp.ne.opt.redshiftfake.s3

import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.{ObjectListing, S3ObjectSummary}

import scala.annotation.tailrec
import scala.collection.JavaConverters._

trait S3Service {
  def lsRecurse(bucket: String, prefix: String): Seq[S3ObjectSummary]
}

class S3ServiceImpl(endpoint: String) extends S3Service {
  private[this] val s3ClientBuilder = {
    val builder = AmazonS3ClientBuilder.standard()
    builder
  }

  private[this] def mkClient = {
    val client = s3ClientBuilder
      .build()
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
}
