package jp.ne.opt.redshiftfake

import java.text.SimpleDateFormat

import jp.ne.opt.redshiftfake.s3.S3ServiceImplWithCustomClient
import org.h2.jdbc.FakeH2Driver
import org.scalatest.fixture


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
    S3Util.loadGzippedDataToS3(s3Client, gzipped, bucket, fileKey)
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
    S3Util.loadBzipped2DataToS3(s3Client, bzipped2, bucket, fileKey)
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

  it should "throw exception when columnList in copyCommand contains unknown column" in { conn =>
    conn.createStatement().execute("create table unknown_column(a int, b boolean, c date)")

    val bucket = "unknown-column"
    val key = "unknownColumn.txt"
    val data = """1,true,2016-11-20
                 |4,false,2016-11-21
               """.stripMargin
    s3Client.createBucket(bucket)
    S3Util.loadDataToS3(s3Client, data, bucket, key)

    assertThrows[FakeAmazonSQLException] (
      conn.createStatement().execute(
        s"""copy unknown_column(a,b,unknownColumn) from '${Global.s3Scheme}$bucket/$key'
           |credentials 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
           |delimiter ','
           |dateformat 'auto'
           |""".stripMargin
      )
    )
  }

  it should "throw exception when inserted data has less values than columns in db" in { conn =>
    conn.createStatement().execute("create table less_values(a int, b boolean, c date)")

    val bucket = "less-values"
    val key = "lessValues.txt"
    val data = """1,true
                 |4,false
               """.stripMargin
    s3Client.createBucket(bucket)
    S3Util.loadDataToS3(s3Client, data, bucket, key)

    assertThrows[FakeAmazonSQLException] (
      conn.createStatement().execute(
        s"""copy less_values(a,b,c) from '${Global.s3Scheme}$bucket/$key'
           |credentials 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
           |delimiter ','
           |dateformat 'auto'
           |""".stripMargin
      )
    )
  }

  it should "execute copy command when inserted data has values for all inserted columns" in { conn =>
    conn.createStatement().execute("create table all_columns(a int, b boolean, c date)")

    val bucket = "all-columns"
    val key = "allColumns.txt"
    val data = """1,2016-11-20
                 |4,2016-11-21
               """.stripMargin
    s3Client.createBucket(bucket)
    S3Util.loadDataToS3(s3Client, data, bucket, key)

    conn.createStatement().execute(
      s"""copy all_columns(a,c) from '${Global.s3Scheme}$bucket/$key'
         |credentials 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
         |delimiter ','
         |dateformat 'auto'
         |""".stripMargin
    )

    val resultSet = conn.createStatement().executeQuery("select * from all_columns order by c")
    val result = Iterator.continually(resultSet).takeWhile(_.next()).map { rs =>
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      (sdf.format(rs.getDate("c")), rs.getInt("a"), rs.getBoolean("b"))
    }.toList

    assert(result == List(
      ("2016-11-20", 1, false),
      ("2016-11-21", 4, false)
    ))
  }
}
