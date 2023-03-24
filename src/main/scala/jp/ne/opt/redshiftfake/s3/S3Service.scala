package jp.ne.opt.redshiftfake.s3

import java.io.ByteArrayInputStream
import java.util.zip.GZIPInputStream
import java.util.stream.Collectors

import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials,
  AwsSessionCredentials,
  StaticCredentialsProvider,
}

import software.amazon.awssdk.core.sync.{
  RequestBody,
  ResponseTransformer
}

import software.amazon.awssdk.services.s3.{
  S3Configuration,
  S3Client,
}

import software.amazon.awssdk.services.s3.model.{
  GetObjectRequest,
  GetObjectResponse,
  PutObjectRequest,
  ListObjectsV2Request,
  S3Object,
}

import software.amazon.awssdk.services.sts.model.AssumeRoleRequest
import software.amazon.awssdk.services.sts.StsClient
import jp.ne.opt.redshiftfake.{Credentials, FileCompressionParameter, Global}
import jp.ne.opt.redshiftfake.util.Loan._
import java.net.URI
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
  def lsRecurse(location: S3Location)(credentials: Credentials): Seq[S3Object]

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
    var clientBuilder = credentials match {
      case Credentials.WithKey(accessKeyId, secretAccessKey) =>
        S3Client.builder()
          .credentialsProvider(
            StaticCredentialsProvider.create(
              AwsBasicCredentials.create(accessKeyId, secretAccessKey)
            )
          )
      case Credentials.WithRole(roleName) =>
        val sts = StsClient.builder().build()
        val assumeRoleRequest = AssumeRoleRequest.builder()
          .roleArn(roleName)
          .build()
        val credentials = sts.assumeRole(assumeRoleRequest).credentials()
        val sessionCredentials = AwsSessionCredentials.create(
          credentials.accessKeyId,
          credentials.secretAccessKey,
          credentials.sessionToken,
        )
        S3Client.builder
          .credentialsProvider(
            StaticCredentialsProvider.create(sessionCredentials)
          )

      case Credentials.WithTemporaryToken(accessKeyId, secretAccessKey, token) =>
        S3Client.builder()
          .credentialsProvider(
            StaticCredentialsProvider.create(
              AwsSessionCredentials.create(accessKeyId, secretAccessKey, token)
            )
          )

      case _ =>
        S3Client.builder()
    }

    clientBuilder = clientBuilder
      .forcePathStyle(true)
      .serviceConfiguration(S3Configuration.builder()
        .chunkedEncodingEnabled(true)
        .build())
      .region(Global.region)

    if (endpoint != "s3://") {
      clientBuilder = clientBuilder.endpointOverride(URI.create(endpoint))
    }
    clientBuilder.build()
  }

  def lsRecurse(location: S3Location)(credentials: Credentials): Seq[S3Object] = {
    val client = mkClient(credentials)
    val request = ListObjectsV2Request.builder()
      .bucket(location.bucket)
      .prefix(location.prefix)
      .build()
    client.listObjectsV2Paginator(request)
      .stream()
      .flatMap(r => r.contents().stream())
      .collect(Collectors.toList())
      .asScala.toList
  }

  def downloadAsString(location: S3Location, compression: FileCompressionParameter)(credentials: Credentials): String = {
    val client = mkClient(credentials)
    val request = GetObjectRequest.builder()
      .bucket(location.bucket)
      .key(location.prefix)
      .build()
    val response = client.getObject(request, ResponseTransformer.toInputStream[GetObjectResponse])
    val objectContent = compression match {
      case FileCompressionParameter.Gzip => new GZIPInputStream(response)
      case FileCompressionParameter.Bzip2 => new BZip2CompressorInputStream(response)
      case _ => response
    }
    scala.io.Source.fromInputStream(objectContent).mkString
  }

  def uploadString(location: S3Location, content: String)(credentials: Credentials): Unit = {

    val request = PutObjectRequest.builder()
      .bucket(location.bucket)
      .key(location.prefix)
      .build()
    mkClient(credentials).putObject(request, RequestBody.fromString(content))
  }
}

class S3ServiceImplWithCustomClient(s3Client: S3Client, endpoint: String = "") extends S3ServiceImpl(endpoint) {

  override private[redshiftfake] def mkClient(credentials: Credentials) = {
    s3Client
  }
}
