package jp.ne.opt.redshiftfake

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import java.util.zip.GZIPOutputStream

import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.core.sync.RequestBody

import jp.ne.opt.redshiftfake.util.Loan.using
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream

object S3Util {

   def loadGzippedDataToS3(s3Client: S3Client, data: String, bucket: String, key: String): Unit = {
    val arrayOutputStream = new ByteArrayOutputStream()
    using(new GZIPOutputStream(arrayOutputStream)) (gzipOutStream => {
      gzipOutStream.write(data.getBytes(StandardCharsets.UTF_8))
    })
    val buf = arrayOutputStream.toByteArray
    val request = PutObjectRequest.builder()
      .bucket(bucket)
      .key(key)
      .build()

    s3Client.putObject(request, RequestBody.fromBytes(buf))
  }

   def loadBzipped2DataToS3(s3Client: S3Client, data: String, bucket: String, key: String): Unit = {
    val arrayOutputStream = new ByteArrayOutputStream()
    using(new BZip2CompressorOutputStream(arrayOutputStream)) (bzip2OutStream => {
      bzip2OutStream.write(data.getBytes(StandardCharsets.UTF_8))
    })
    val buf = arrayOutputStream.toByteArray
    val request = PutObjectRequest.builder()
      .bucket(bucket)
      .key(key)
      .build()

    s3Client.putObject(request, RequestBody.fromBytes(buf))
  }

   def loadDataToS3(s3Client: S3Client, data: String, bucket: String, key: String): Unit = {
    val buf = data.getBytes
    val request = PutObjectRequest.builder()
      .bucket(bucket)
      .key(key)
      .build()

    s3Client.putObject(request, RequestBody.fromBytes(buf))
  }
}
