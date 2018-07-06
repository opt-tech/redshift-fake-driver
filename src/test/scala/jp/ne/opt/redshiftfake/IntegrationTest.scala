package jp.ne.opt.redshiftfake

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.util.zip.GZIPOutputStream

import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest}
import jp.ne.opt.redshiftfake.s3.S3ServiceImplWithCustomClient
import org.h2.jdbc.FakeH2Driver
import org.scalatest.fixture
import jp.ne.opt.redshiftfake.util.Loan.using
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream

class IntegrationTest extends fixture.FlatSpec
  with H2Sandbox
  with S3Sandbox {

  val s3Endpoint = "http://127.0.0.1:7887"
  val s3Region = "ap-northeast-1"
  val dummyCredentials = Credentials.WithKey("AKIAXXXXXXXXXXXXXXX", "YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY")
  val s3Client = createS3Client(s3Region)
  val s3Service = new S3ServiceImplWithCustomClient(s3Client)

  FakeH2Driver.setS3Service(s3Service)

  it should "unload / copy via manifest" in { conn =>
    // create bucket
    s3Client.createBucket("foo")

    // create source table
    conn.createStatement().execute("create table foo(a int, b boolean, c date)")
    // create target table
    conn.createStatement().execute("create table bar(d date, cnt int, sum_a int)")

    // prepare data
    conn.createStatement().execute("insert into foo values(1, true, '2016-11-20')")
    conn.createStatement().execute("insert into foo values(2, false, '2016-11-20')")
    conn.createStatement().execute("insert into foo values(3, true, '2016-11-21')")
    conn.createStatement().execute("insert into foo values(4, false, '2016-11-21')")
    conn.createStatement().execute("insert into foo values(5, true, '2016-11-21')")

    conn.createStatement().execute(
      s"""unload ('select c, count(*), sum(a) from foo where b = true group by c order by c') to '${Global.s3Scheme}foo/unloaded_'
          |credentials 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
          |manifest
          |addquotes""".stripMargin
    )

    conn.createStatement().execute(
      s"""copy bar from '${Global.s3Scheme}foo/unloaded_manifest'
          |credentials 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
          |manifest
          |removequotes
          |dateformat 'auto'""".stripMargin
    )

    val resultSet = conn.createStatement().executeQuery("select * from bar order by d")
    val result = Iterator.continually(resultSet).takeWhile(_.next()).map { rs =>
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      (sdf.format(rs.getDate("d")), rs.getInt("cnt"), rs.getInt("sum_a"))
    }.toList

    assert(result == List(
      ("2016-11-20", 1, 1),
      ("2016-11-21", 2, 8)
    ))
  }

  it should "success copy from gzipped files with GZIP option" in { conn =>
    val bucket = "gzipped"
    val gzipped =
      """1,true,2016-11-20
        |4,false,2016-11-21
      """.stripMargin
    val fileKey = "gzipped.txt.gz"

    s3Client.createBucket("gzipped")
    loadGzippedDataToS3(gzipped, bucket, fileKey)
    conn.createStatement().execute("create table gzipped(a int, b boolean, c date)")

    conn.createStatement().execute(
      s"""copy gzipped from '${Global.s3Scheme}$bucket/$fileKey'
         |credentials 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
         |delimiter ','
         |dateformat 'auto'
         |gzip""".stripMargin
    )

    val resultSet = conn.createStatement().executeQuery("select * from gzipped order by c")
    val result = Iterator.continually(resultSet).takeWhile(_.next()).map { rs =>
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      (sdf.format(rs.getDate("c")), rs.getInt("a"), rs.getBoolean("b"))
    }.toList

    assert(result == List(
      ("2016-11-20", 1, true),
      ("2016-11-21", 4, false)
    ))
  }

  it should "success copy from bzipped2 files with GZIP option" in { conn =>
    val bucket = "bzipped2"
    val bzipped2 =
      """1,true,2016-11-20
        |4,false,2016-11-21
      """.stripMargin
    val fileKey = "bzipped2.txt.gz"

    s3Client.createBucket("bzipped2")
    loadBzipped2DataToS3(bzipped2, bucket, fileKey)
    conn.createStatement().execute("create table bzipped2(a int, b boolean, c date)")

    conn.createStatement().execute(
      s"""copy bzipped2 from '${Global.s3Scheme}$bucket/$fileKey'
         |credentials 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
         |delimiter ','
         |dateformat 'auto'
         |bzip2""".stripMargin
    )

    val resultSet = conn.createStatement().executeQuery("select * from bzipped2 order by c")
    val result = Iterator.continually(resultSet).takeWhile(_.next()).map { rs =>
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      (sdf.format(rs.getDate("c")), rs.getInt("a"), rs.getBoolean("b"))
    }.toList

    assert(result == List(
      ("2016-11-20", 1, true),
      ("2016-11-21", 4, false)
    ))
  }

  private def loadGzippedDataToS3(data: String, bucket: String, key: String): Unit = {
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

  private def loadBzipped2DataToS3(data: String, bucket: String, key: String): Unit = {
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
}
