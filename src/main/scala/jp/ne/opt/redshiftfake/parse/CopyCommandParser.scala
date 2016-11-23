package jp.ne.opt.redshiftfake.parse

import jp.ne.opt.redshiftfake._

object CopyCommandParser extends BaseParser {
  case class TableAndSchemaName(schemaName: Option[String], tableName: String)

  val tableNameParser = ((identifier.r <~ ".").? ~ identifier.r) ^^ {
    case ~(schemaName, tableName) => TableAndSchemaName(schemaName, tableName)
  }

  val columnListParser = "(" ~> ((identifier.r <~ s",$space".r).* ~ identifier.r) <~ ")" ^^ {
    case ~(init, last) => init :+ last
  }

  val dataSourceParser = {
    def s3SourceParser = "'" ~> s3LocationParser <~ "'" ^^ CopyDataSource.S3
    s3SourceParser
  }

  val copyFormatParser = {
    def json = ("(?i)JSON".r ~> "(?i)AS".r.? ~> ("'" ~> s3LocationParser <~ "'").?) ^^ CopyFormat.Json

    "(?i)FORMAT".r.? ~> "(?i)AS".r.? ~> json.? ^^ (_.getOrElse(CopyFormat.Default))
  }

  val timeFormatParser: Parser[TimeFormatType] = {
    s"$any*(?i)TIMEFORMAT".r ~> "(?i)AS".r.? ~>
      ("'auto'" | "'epochsecs'" | "'epochmillisecs'" | "'" ~> """[ \w./:,-]+""".r <~ "'") <~
      s"$any*".r ^^ {
      case "'auto'" => TimeFormatType.Auto
      case "'epochsecs'" => TimeFormatType.Epochsecs
      case "'epochmillisecs'" => TimeFormatType.EpochMillisecs
      case pattern => TimeFormatType.Custom(pattern)
    }
  }

  val dateFormatParser: Parser[DateFormatType] = {
    s"$any*(?i)DATEFORMAT".r ~> "(?i)AS".r.? ~> ("'auto'" | "'" ~> """[ \w./:,-]+""".r <~ "'") <~ s"$any*".r ^^ {
      case "'auto'" => DateFormatType.Auto
      case pattern => DateFormatType.Custom(pattern)
    }
  }

  val manifestParser = {
    s"$any*(?i)MANIFEST$any*".r
  }

  val emptyAsNullParser = {
    s"$any*(?i)EMPTYASNULL$any*".r
  }

  def parse(query: String): Option[CopyCommand] = {
    val result = parse(
      ("(?i)COPY".r ~> tableNameParser) ~
        columnListParser.? ~
        ("(?i)FROM".r ~> dataSourceParser) ~
        ("(?i)WITH".r.? ~> "(?i)CREDENTIALS".r ~> "(?i)AS".r.? ~> awsAuthArgsParser) ~
        copyFormatParser ~
        s"$any*".r ^^ { case ~(~(~(~(~(TableAndSchemaName(schemaName, tableName), columnList), dataSource), auth), format), dataConversionParameters) =>
        val command = CopyCommand(
          schemaName,
          tableName,
          columnList,
          dataSource,
          auth,
          format,
          parse(dateFormatParser, dataConversionParameters).getOrElse(DateFormatType.Default),
          parse(timeFormatParser, dataConversionParameters).getOrElse(TimeFormatType.Default),
          parse(emptyAsNullParser, dataConversionParameters).successful
        )

        // handle manifest
        if (parse(manifestParser, dataConversionParameters).successful) {
          dataSource match {
            case CopyDataSource.S3(location) => command.copy(copyFormat = CopyFormat.Manifest(location))
            case _ => command
          }
        } else {
          command
        }

      },
      query
    )
    if (result.isEmpty) None else Some(result.get)
  }
}
