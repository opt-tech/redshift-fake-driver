package jp.ne.opt.redshiftfake.parse

import jp.ne.opt.redshiftfake._

class CopyCommandParser extends BaseParser {

  case class TableAndSchemaName(schemaName: Option[String], tableName: String)

  private[this] val tableNameParser = ((quotedIdentifierParser <~ ".").? ~ quotedIdentifierParser) ^^ {
    case ~(schemaName, tableName) => TableAndSchemaName(schemaName, tableName)
  }

  private[this] val columnListParser = "(" ~> ((quotedIdentifierParser <~ s",$space".r).* ~ quotedIdentifierParser) <~ ")" ^^ {
    case ~(init, last) => init :+ last
  }

  private[this] val dataSourceParser = {
    def s3SourceParser = "'" ~> s3LocationParser <~ "'" ^^ CopyDataSource.S3
    s3SourceParser
  }

  private[this] val copyFormatParser = {
    def json = ("(?i)JSON".r ~> "(?i)AS".r.? ~> ("'" ~> s3LocationParser <~ "'").?) ^^ CopyFormat.Json

    "(?i)FORMAT".r.? ~> "(?i)AS".r.? ~> json.? ^^ (_.getOrElse(CopyFormat.Default))
  }

  private[this] val nullAsParser = s"$any*(?i)NULL$space+AS".r ~> "'" ~> """[^']*""".r <~ "'" <~ s"$any*".r

  private[this] val timeFormatParser: Parser[TimeFormatType] = {
    s"$any*(?i)TIMEFORMAT".r ~> "(?i)AS".r.? ~>
      ("'auto'" | "'epochsecs'" | "'epochmillisecs'" | "'" ~> """[ \w./:,-]+""".r <~ "'") <~
      s"$any*".r ^^ {
      case "'auto'" => TimeFormatType.Auto
      case "'epochsecs'" => TimeFormatType.Epochsecs
      case "'epochmillisecs'" => TimeFormatType.EpochMillisecs
      case pattern => TimeFormatType.Custom(pattern)
    }
  }

  private[this] val dateFormatParser: Parser[DateFormatType] = {
    s"$any*(?i)DATEFORMAT".r ~> "(?i)AS".r.? ~> ("'auto'" | "'" ~> """[ \w./:,-]+""".r <~ "'") <~ s"$any*".r ^^ {
      case "'auto'" => DateFormatType.Auto
      case pattern => DateFormatType.Custom(pattern)
    }
  }

  private[this] val manifestParser = {
    s"$any*(?i)MANIFEST$any*".r
  }

  private[this] val emptyAsNullParser = {
    s"$any*(?i)EMPTYASNULL$any*".r
  }

  private[this] val gzipFileCompressionParser = {
    s"$any*(?i)GZIP".r
  }

  private[this] val bzip2FileCompressionParser = {
    s"$any*(?i)BZIP2".r
  }

  def parse(query: String): Option[CopyCommand] = {
    val result = parse(
      ("(?i)COPY".r ~> tableNameParser) ~
        columnListParser.? ~
        ("(?i)FROM".r ~> dataSourceParser) ~
        (("(?i)WITH".r.? ~> "(?i)CREDENTIALS".r ~> "(?i)AS".r.? ~> awsAuthArgsParser) | awsAuthTemporaryParser) ~
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
          parse(emptyAsNullParser, dataConversionParameters).successful,
          parse(delimiterParser, dataConversionParameters).getOrElse('|'),
          parse(nullAsParser, dataConversionParameters).getOrElse("\u000e"),
          parseFileCompression(dataConversionParameters)
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

  private def parseFileCompression(dataConversionParameters: String): FileCompressionParameter = {
    if (parse(gzipFileCompressionParser, dataConversionParameters).successful) {
      FileCompressionParameter.Gzip
    } else if (parse(bzip2FileCompressionParser, dataConversionParameters).successful) {
      FileCompressionParameter.Bzip2
    } else {
      FileCompressionParameter.None
    }
  }
}
