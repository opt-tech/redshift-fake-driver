package jp.ne.opt.redshiftfake.s3

import java.io.ByteArrayInputStream
import java.util.zip.GZIPInputStream

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.ServiceAbbreviations
import com.amazonaws.services.s3.{AmazonS3Client, S3ClientOptions}
import com.amazonaws.services.s3.model._
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest
import com.amazonaws.auth.BasicSessionCredentials
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder
import jp.ne.opt.redshiftfake.{Credentials, FileCompressionParameter, Global}
import jp.ne.opt.redshiftfake.util.Loan._
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream

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
  def downloadAsString(location: S3Location, fileCompressionParameter: FileCompressionParameter = FileCompressionParameter.None)(credentials: Credentials): String

  /**
   * Upload a string content to specified location.
   */
  def uploadString(location: S3Location, content: String)(credentials: Credentials): Unit
}

class S3ServiceImpl(endpoint: String) extends S3Service {

  private[redshiftfake] def mkClient(credentials: Credentials) = {
    val client = credentials match {
      case Credentials.WithKey(accessKeyId, secretAccessKey) =>
        new AmazonS3Client(new BasicAWSCredentials(accessKeyId, secretAccessKey))

      case Credentials.WithRole(roleName) =>
        val sts = AWSSecurityTokenServiceClientBuilder.standard.build
        val assumeRoleRequest = new AssumeRoleRequest
        assumeRoleRequest.withRoleArn(roleName)
        val credentials = sts.assumeRole(assumeRoleRequest).getCredentials
        val sessionCredentials = new BasicSessionCredentials(credentials.getAccessKeyId, credentials.getSecretAccessKey, credentials.getSessionToken)
        new AmazonS3Client(sessionCredentials)

      case Credentials.WithTemporaryToken(accessKeyId, secretAccessKey, token) =>
        new AmazonS3Client(new BasicSessionCredentials(accessKeyId, secretAccessKey, token))

      case _ =>
        new AmazonS3Client()
    }

    client.setS3ClientOptions({
      val options = S3ClientOptions.builder()
        .setPathStyleAccess(true)
        .build()

      // for compatibility
      val setChunkedEncodingDisabled = "setChunkedEncodingDisabled"
      val clazz = classOf[S3ClientOptions]
      if (clazz.getMethods.map(_.getName).contains(setChunkedEncodingDisabled)) {
        clazz.getMethod(setChunkedEncodingDisabled, classOf[Boolean]).invoke(options, java.lang.Boolean.valueOf(true))
      }

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

  def downloadAsString(location: S3Location, compression: FileCompressionParameter)(credentials: Credentials): String = {
    val client = mkClient(credentials)
    val request = new GetObjectRequest(location.bucket, location.prefix)
    using(client.getObject(request)) { obj =>
      val objectContent = compression match {
        case FileCompressionParameter.Gzip => new GZIPInputStream(obj.getObjectContent)
        case FileCompressionParameter.Bzip2 => new BZip2CompressorInputStream(obj.getObjectContent)
        case _ => obj.getObjectContent
      }
      io.Source.fromInputStream(objectContent).mkString
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

class S3ServiceImplWithCustomClient(s3Client: AmazonS3Client, endpoint: String = "") extends S3ServiceImpl(endpoint) {

  override private[redshiftfake] def mkClient(credentials: Credentials) = {
    s3Client
  }
}
