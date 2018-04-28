package jp.ne.opt.redshiftfake.parse

import jp.ne.opt.redshiftfake.UnloadCommand
import jp.ne.opt.redshiftfake.parse.compat.QueryCompatibility

class UnloadCommandParser extends BaseParser with QueryCompatibility {

  object selectStatementParser extends Parser[String] {
    def apply(in: Input): ParseResult[String] = {
      val source = in.source
      val offset = in.offset
      val spaceSkipped = handleWhiteSpace(source, offset)

      if (source.length() > spaceSkipped + 2 && source.subSequence(spaceSkipped, spaceSkipped + 2) == "('") {
        val start = spaceSkipped + 2

        var i = start

        var escape: Option[Char] = None
        var prev2: Option[Char] = None
        var prev: Option[Char] = None
        while (i < source.length() && !(escape != Some('\\') && prev2 == Some('\'') && prev == Some(')'))) {
          prev = Some(source.charAt(i))
          if (i > 0) { prev2 = Some(source.charAt(i - 1)) }
          if (i > 1) { escape = Some(source.charAt(i - 2)) }
          i += 1
        }
        if (source.length() > 2 && source.subSequence(i - 2, i) == "')") {
          Success(source.subSequence(start, i - 2).toString, in.drop(i - offset))
        } else {
          Failure("failed", in.drop(start - offset))
        }
      } else {
        Failure("failed", in.drop(spaceSkipped - offset))
      }
    }
  }

  val addQuotesParser = s"$any*(?i)ADDQUOTES$any*".r

  val manifestParser = s"$any*(?i)MANIFEST$any*".r

  val statementParser = "(?i)UNLOAD".r ~> selectStatementParser ^^ { s =>
    val unescaped = s.replace("""\'""", "'")
    unescaped
  }

  def parse(query: String): Option[UnloadCommand] = {
    val result = parse(
      statementParser ~
        ("(?i)TO".r ~> "'" ~> s3LocationParser <~ "'") ~
        ("(?i)WITH".r.? ~> "(?i)CREDENTIALS".r ~> "(?i)AS".r.? ~> awsAuthArgsParser) ~
        s"$any*".r ^^ { case ~(~(~(statement, s3Location), auth), unloadOptions) =>
        UnloadCommand(
          dropIncompatibilities(statement),
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
