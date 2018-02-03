package jp.ne.opt.redshiftfake

import java.text.SimpleDateFormat

import jp.ne.opt.redshiftfake.s3.S3ServiceImpl
import org.scalatest.fixture

class IntegrationTest extends fixture.FlatSpec with H2Sandbox with CIOnly {

  it should "unload / copy via manifest" in { conn =>
    skiplIfLocalEnvironment()

    // create bucket
    val dummyCredentials = Credentials.WithKey("AKIAXXXXXXXXXXXXXXX", "YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY")
    val s3Service = new S3ServiceImpl(Global.s3Endpoint)
    s3Service.mkClient(dummyCredentials).createBucket("foo")

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

  it should "copy from csv (delimiter = ',')" in { conn =>
    skiplIfLocalEnvironment()

    // create bucket
    val dummyCredentials = Credentials.WithKey("AKIAXXXXXXXXXXXXXXX", "YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY")
    val s3Service = new S3ServiceImpl(Global.s3Endpoint)
    val s3Client = s3Service.mkClient(dummyCredentials)

    s3Client.createBucket("bar")

    // create target table
    conn.createStatement().execute("create table bar(d date, cnt int, sum_a int)")

    // prepare data
    val csv = """"2017-11-28","42","43"
                |"2017-11-29","44","45"
                |""".stripMargin
    s3Client.putObject("bar", "unloaded_0000_part_00", csv)
    s3Client.putObject("bar", "unloaded_manifest", """{"entries":[{"url":"http://localhost:9444/bar/unloaded_0000_part_00"}]}""")

    conn.createStatement().execute(
      s"""copy bar from '${Global.s3Scheme}bar/unloaded_manifest'
         |credentials 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
         |manifest
         |delimiter as ','
         |removequotes
         |dateformat 'auto'""".stripMargin
    )

    val resultSet = conn.createStatement().executeQuery("select * from bar order by d")
    val result = Iterator.continually(resultSet).takeWhile(_.next()).map { rs =>
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      (sdf.format(rs.getDate("d")), rs.getInt("cnt"), rs.getInt("sum_a"))
    }.toList

    assert(result == List(
      ("2017-11-28", 42, 43),
      ("2017-11-29", 44, 45)
    ))
  }
}
