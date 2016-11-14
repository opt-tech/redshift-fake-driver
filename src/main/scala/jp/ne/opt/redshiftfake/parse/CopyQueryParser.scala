package jp.ne.opt.redshiftfake.parse

import jp.ne.opt.redshiftfake.s3.Credentials

import scala.util.parsing.combinator.RegexParsers

case class CopyQuery(
  tableName: String,
  columnList: Option[Seq[String]],
  dataSource: String,
  credentials: Credentials
)

sealed abstract class CopyFormat
object CopyFormat {
//  case object
}

object CopyQueryParser extends RegexParsers {
  def identifier = """[_a-zA-Z]\w*"""
  def space = """\s*"""
  def tableNameParser = s"""$identifier[.]?$identifier""".r ^^ identity
  def columnListParser = """\(\s*""".r ~> ((identifier.r <~ """\s*,\s*""".r).* ~ identifier.r) <~ """\s*\)""".r ^^ {
    case ~(init, last) => init :+ last
  }
  def dataSourceParser = "'" ~> """s3://[\w/:%#$&?()~.=+-]+""".r <~ "'"
  def awsAuthArgsParser = {
    def parserWithKey = ("aws_access_key_id=" ~> """\w+""".r) ~ (";aws_secret_access_key=" ~> """\w+""".r) ^^ {
      case ~(accessKeyId, secretAccessKey) => Credentials.WithKey(accessKeyId, secretAccessKey)
    }

    "'" ~> parserWithKey <~ "'"
  }

  def parse(query: String): Option[CopyQuery] = {
    val result = parse(
      (s"""$space(?i)COPY$space""".r ~> tableNameParser <~ space.r) ~
        (columnListParser.? <~ space.r) ~
        (s"""(?i)FROM$space""".r ~> dataSourceParser <~ space.r) ~
        (s"""(?i)WITH""".r.? ~> space.r ~> s"""(?i)CREDENTIALS$space""".r ~> s"""(?i)AS""".r.? ~> space.r ~> awsAuthArgsParser <~ space.r) <~
        s""".*""".r ^^ { case ~(~(~(tableName, columnList), dataSource), auth) =>

        CopyQuery(tableName, columnList, dataSource, auth)
      },
      query
    )
    if (result.isEmpty) None else Some(result.get)
  }
}
