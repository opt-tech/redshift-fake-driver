package jp.ne.opt.redshiftfake.parse

import jp.ne.opt.redshiftfake.UnloadCommand

object UnloadCommandParser extends BaseParser {
  val addQuotesParser = s"$any*(?i)ADDQUOTES$any*".r

  val delimiterParser = s"$any*(?i)DELIMITER$space".r ~> "(?i)AS".r.? ~> space.r ~> "'" ~> """[|,]""".r <~ "'" <~ s"$any*".r ^^ { s => s.head }

  val manifestParser = s"$any*(?i)MANIFEST$any*".r

  val statementParser = s"$space(?i)UNLOAD$space".r ~> """\('.+'\)""".r <~ space.r ^^ { s =>
    val raw = s.drop(2).dropRight(2)

    // FIXME: Too naive implementation
    val unescaped = raw.replace("""\'""", "'")

    unescaped
  }

  def parse(query: String): Option[UnloadCommand] = {
    val result = parse(
      statementParser ~
        (s"(?i)TO$space".r ~> "'" ~> s3LocationParser <~ "'" <~ space.r) ~
        ("(?i)WITH".r.? ~> space.r ~> s"(?i)CREDENTIALS$space".r ~> "(?i)AS".r.? ~> space.r ~> awsAuthArgsParser <~ space.r) ~
        s"$any*".r ^^ { case ~(~(~(statement, s3Location), auth), unloadOptions) =>
        UnloadCommand(
          statement,
          s3Location,
          auth,
          parse(manifestParser, unloadOptions).successful,
          parse(delimiterParser, unloadOptions).getOrElse('|'),
          parse(addQuotesParser, unloadOptions).successful
        )
      },
      query
    )
    if (result.isEmpty) None else Some(result.get)
  }
}
