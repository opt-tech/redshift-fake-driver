package jp.ne.opt.redshiftfake.parse

import jp.ne.opt.redshiftfake._

object CopyCommandParser extends BaseParser {
  case class TableAndSchemaName(schemaName: Option[String], tableName: String)

  val tableNameParser = ((identifier.r <~ ".").? ~ identifier.r) ^^ {
    case ~(schemaName, tableName) => TableAndSchemaName(schemaName, tableName)
  }

  val columnListParser = "(" ~> space.r ~> ((identifier.r <~ s"""$space,$space""".r).* ~ identifier.r) <~ space.r <~ ")" ^^ {
    case ~(init, last) => init :+ last
  }

  val dataSourceParser = {
    def s3SourceParser = "'" ~> s3LocationParser <~ "'" ^^ CopyDataSource.S3
    s3SourceParser
  }

  val copyFormatParser = {
    def json = ("(?i)JSON".r ~> space.r ~> "(?i)AS".r.? ~> space.r ~> ("'" ~> s3LocationParser <~ "'").?) ^^ CopyFormat.Json

    "(?i)FORMAT".r.? ~> space.r ~> "(?i)AS".r.? ~> space.r ~> json.? ^^ (_.getOrElse(CopyFormat.Default))
  }

  val timeFormatParser: Parser[TimeFormatType] = {
    s"$any*(?i)TIMEFORMAT".r ~> space.r ~> "(?i)AS".r.? ~> space.r ~>
      ("'auto'" | "'epochsecs'" | "'epochmillisecs'" | "'" ~> """[ \w./:,-]+""".r <~ "'") <~
      s"$any*".r ^^ {
      case "'auto'" => TimeFormatType.Auto
      case "'epochsecs'" => TimeFormatType.Epochsecs
      case "'epochmillisecs'" => TimeFormatType.EpochMillisecs
      case pattern => TimeFormatType.Custom(pattern)
    }
  }

  val dateFormatParser: Parser[DateFormatType] = {
    s"$any*(?i)DATEFORMAT".r ~> space.r ~> "(?i)AS".r.? ~> space.r ~> ("'auto'" | "'" ~> """[ \w./:,-]+""".r <~ "'") <~ s"$any*".r ^^ {
      case "'auto'" => DateFormatType.Auto
      case pattern => DateFormatType.Custom(pattern)
    }
  }

  val manifestParser = {
    s"$any*(?i)MANIFEST$any*".r
  }

  def parse(query: String): Option[CopyCommand] = {
    val result = parse(
      (s"$space(?i)COPY$space".r ~> tableNameParser <~ space.r) ~
        (columnListParser.? <~ space.r) ~
        (s"(?i)FROM$space".r ~> dataSourceParser <~ space.r) ~
        ("(?i)WITH".r.? ~> space.r ~> s"(?i)CREDENTIALS$space".r ~> "(?i)AS".r.? ~> space.r ~> awsAuthArgsParser <~ space.r) ~
        (copyFormatParser <~ space.r) ~
        s"$any*".r ^^ { case ~(~(~(~(~(TableAndSchemaName(schemaName, tableName), columnList), dataSource), auth), format), dataConversionParameters) =>
        val command = CopyCommand(
          schemaName,
          tableName,
          columnList,
          dataSource,
          auth,
          format,
          parse(dateFormatParser, dataConversionParameters).getOrElse(DateFormatType.Default),
          parse(timeFormatParser, dataConversionParameters).getOrElse(TimeFormatType.Default)
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
