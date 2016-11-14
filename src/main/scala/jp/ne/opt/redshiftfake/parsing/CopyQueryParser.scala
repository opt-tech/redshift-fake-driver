package jp.ne.opt.redshiftfake.parsing

import scala.util.parsing.combinator.RegexParsers

case class CopyQuery(
  tableName: String,
  columnList: Option[Seq[String]],
  dataSource: String
)

sealed abstract class CopyFormat
object CopyFormat {
//  case object
}

object CopyQueryParser extends RegexParsers {
  def identifier = """[_a-zA-Z]\w*"""
  def space = """[ \t\n]*"""
  def tableNameParser = s"""$identifier[.]?$identifier""".r ^^ identity
  def columnListParser = """\(\s*""".r ~> ((identifier.r <~ """\s*,\s*""".r).* ~ identifier.r) <~ """\s*\)""".r ^^ {
    case ~(init, last) => init :+ last
  }
  def dataSourceParser = "'" ~> s"""s3://[_a-zA-Z0-9/.]+""".r <~ "'"
  def awsAuthArgsParser = "'" ~> s"""s3://[_a-zA-Z0-9/.]+""".r <~ "'"

  def parse(query: String): Option[CopyQuery] = {
    val result = parse(
      (s"""$space(?i)COPY$space""".r ~> tableNameParser <~ space.r) ~
        (columnListParser.? <~ space.r) ~
        (s"""(?i)FROM$space""".r ~> dataSourceParser <~ space.r) <~
        (s"""(?i)WITH$space""".r.? ~> s"""(?i)CREDENTIALS$space""".r ~> s"""(?i)AS$space""" ~> awsAuthArgsParser <~ space.r) ~
        s""".*""".r ^^ { case ~(~(tableName, columnList), dataSource) =>
        CopyQuery(tableName, columnList, dataSource)
      },
      query
    )
    if (result.isEmpty) None else Some(result.get)
  }
}
