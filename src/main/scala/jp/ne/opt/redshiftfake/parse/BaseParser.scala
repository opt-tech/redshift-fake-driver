package jp.ne.opt.redshiftfake.parse

import jp.ne.opt.redshiftfake.{Credentials, Global}
import jp.ne.opt.redshiftfake.s3.S3Location

import scala.util.parsing.combinator.RegexParsers

trait BaseParser extends RegexParsers {
  val identifier = """[_a-zA-Z]\w*"""

  val dataTypeIdentifier = """[_a-zA-Z]\w*(( )?\([0-9]+\))?"""
  val quotedIdentifier = s"""(?i)($identifier|"$identifier")"""
  val quotedIdentifierParser = "\"".? ~> identifier.r <~ "\"".? ^^ {
    _.replaceAll("\"", "")
  }

  val space = """\s*"""

  val any = """(.|\s)"""

  val s3LocationParser = Global.s3Scheme ~> """[\w-]+""".r ~ ("/" ~> """[\w/:%#$&?()~.=+-]+""".r).? ^^ {
    case ~(bucket, prefix) => S3Location(bucket, prefix.getOrElse(""))
  }

  val awsAuthArgsParser = {
    def parserWithKey = """[\w_]+=""".r ~ """[\w/+=:-]+""".r ~ (";aws_secret_access_key=" ~> """[\w/+=]+""".r).? ^^ {
      case "aws_access_key_id=" ~ accessKeyId ~ Some(secretAccessKey) => Credentials.WithKey(accessKeyId, secretAccessKey)
      case "aws_role_arn=" ~ awsIamRole ~ None => Credentials.WithRole(awsIamRole)
    }

    "'" ~> parserWithKey <~ "'"
  }

  val awsAuthTemporaryParser = {
    "ACCESS_KEY_ID" ~ """'[\w]*'""".r ~ "SECRET_ACCESS_KEY" ~ """'[\w]*'""".r ~ "SESSION_TOKEN" ~ """'[\w]*'""".r ^^ {
      case _ ~ access_key_id ~ _ ~ secret_access_key ~ _ ~ session_token =>
        Credentials.WithTemporaryToken(
          fromStringLiteral(access_key_id),
          fromStringLiteral(secret_access_key),
          fromStringLiteral(session_token)
        )
    }
  }

  private def fromStringLiteral(s: String) = s.drop(1).dropRight(1)

  val delimiterParser = s"$any*(?i)DELIMITER".r ~> "(?i)AS".r.? ~> "'" ~> """[|,]""".r <~ "'" <~ s"$any*".r ^^ { s => s.head }
}
