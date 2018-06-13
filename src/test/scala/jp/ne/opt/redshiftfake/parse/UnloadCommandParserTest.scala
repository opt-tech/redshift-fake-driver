package jp.ne.opt.redshiftfake.parse

import jp.ne.opt.redshiftfake._
import jp.ne.opt.redshiftfake.s3.S3Location
import org.scalatest.FlatSpec

class UnloadCommandParserTest extends FlatSpec {
  it should "parse minimal UNLOAD command" in {
    val command =
      s"""
         |UNLOAD ('${"""SELECT * FROM foo_bar WHERE baz = \'2016-01-01\'"""}') TO '${Global.s3Scheme}some-bucket/path/to/data'
         |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY';
         |""".stripMargin

    val expected = UnloadCommand(
      selectStatement = "SELECT * FROM foo_bar WHERE baz = '2016-01-01'",
      destination = S3Location("some-bucket", "path/to/data"),
      credentials = Credentials.WithKey(
        accessKeyId = "AKIAXXXXXXXXXXXXXXX",
        secretAccessKey = "YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY"
      ),
      createManifest = false,
      delimiter = '|',
      addQuotes = false
    )

    assert(new UnloadCommandParser().parse(command) == Some(expected))
  }

  it should "parse Manifest" in {
    val command =
      s"""
         |UNLOAD ('select * from foo_bar where baz = \'2016-01-01\'') TO '${Global.s3Scheme}some-bucket/path/to/data'
         |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
         |MANIFEST;
         |""".stripMargin

    assert(new UnloadCommandParser().parse(command).exists(_.createManifest))
  }

  it should "parse delimiter" in {
    val command =
      s"""
         |UNLOAD ('select * from foo_bar where baz = \'2016-01-01\'') TO '${Global.s3Scheme}some-bucket/path/to/data'
         |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
         |DELIMITER ',';
         |""".stripMargin

    assert(new UnloadCommandParser().parse(command).map(_.delimiter) == Some(','))
  }

  it should "parse addQuotes" in {
    val command =
      s"""
         |UNLOAD ('select * from foo_bar where baz = \'2016-01-01\'') TO '${Global.s3Scheme}some-bucket/path/to/data'
         |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
         |ADDQUOTES;
         |""".stripMargin

    assert(new UnloadCommandParser().parse(command).exists(_.addQuotes))
  }

  it should "parse multiple options" in {
    val command =
      s"""
         |UNLOAD ('select * from foo_bar where baz = \'2016-01-01\'') TO '${Global.s3Scheme}some-bucket/path/to/data'
         |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
         |MANIFEST
         |ADDQUOTES
         |DELIMITER ',';
         |""".stripMargin

    assert(new UnloadCommandParser().parse(command).exists(_.addQuotes))
    assert(new UnloadCommandParser().parse(command).exists(_.createManifest))
    assert(new UnloadCommandParser().parse(command).map(_.delimiter) == Some(','))
  }

  it should "parse UNLOAD command with iam_role_arn credentials" in {
    val command =
      s"""
         |UNLOAD ('${"""SELECT * FROM foo_bar WHERE baz = \'2016-01-01\'"""}') TO '${Global.s3Scheme}some-bucket/path/to/data'
         |CREDENTIALS 'aws_role_arn=arn:aws:iam::12345:role/some-role';
         |""".stripMargin

    val expected = UnloadCommand(
      selectStatement = "SELECT * FROM foo_bar WHERE baz = '2016-01-01'",
      destination = S3Location("some-bucket", "path/to/data"),
      credentials = Credentials.WithRole("arn:aws:iam::12345:role/some-role"),
      createManifest = false,
      delimiter = '|',
      addQuotes = false
    )

    assert(new UnloadCommandParser().parse(command) == Some(expected))
  }
}
