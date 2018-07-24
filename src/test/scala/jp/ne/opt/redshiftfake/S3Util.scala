package jp.ne.opt.redshiftfake

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import java.util.zip.GZIPOutputStream

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest}
import jp.ne.opt.redshiftfake.util.Loan.using
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream

object S3Util {

   def loadGzippedDataToS3(s3Client: AmazonS3, data: String, bucket: String, key: String): Unit = {
    val arrayOutputStream = new ByteArrayOutputStream()
    using(new GZIPOutputStream(arrayOutputStream)) (gzipOutStream => {
      gzipOutStream.write(data.getBytes(StandardCharsets.UTF_8))
    })
    val buf = arrayOutputStream.toByteArray
    val metadata = new ObjectMetadata
    metadata.setContentLength(buf.length)
    val request = new PutObjectRequest(bucket, key, new ByteArrayInputStream(buf), metadata)

    s3Client.putObject(request)
  }

   def loadBzipped2DataToS3(s3Client: AmazonS3, data: String, bucket: String, key: String): Unit = {
    val arrayOutputStream = new ByteArrayOutputStream()
    using(new BZip2CompressorOutputStream(arrayOutputStream)) (bzip2OutStream => {
      bzip2OutStream.write(data.getBytes(StandardCharsets.UTF_8))
    })
    val buf = arrayOutputStream.toByteArray
    val metadata = new ObjectMetadata
    metadata.setContentLength(buf.length)
    val request = new PutObjectRequest(bucket, key, new ByteArrayInputStream(buf), metadata)

    s3Client.putObject(request)
  }

   def loadDataToS3(s3Client: AmazonS3, data: String, bucket: String, key: String): Unit = {
    val buf = data.getBytes
    val metadata = new ObjectMetadata
    metadata.setContentLength(buf.length)
    val request = new PutObjectRequest(bucket, key, new ByteArrayInputStream(buf), metadata)

    s3Client.putObject(request)
  }
}
