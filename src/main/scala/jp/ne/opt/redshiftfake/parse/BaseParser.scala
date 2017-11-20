package jp.ne.opt.redshiftfake.parse

import jp.ne.opt.redshiftfake.{Credentials, Global}
import jp.ne.opt.redshiftfake.s3.S3Location

import scala.util.parsing.combinator.RegexParsers

trait BaseParser extends RegexParsers {
  val identifier = """[_a-zA-Z]\w*"""
  val quotedIdentifier = """\"?[_a-zA-Z]\w*\"?""".r ^^ {
    _.replaceAll("\"", "")
  }

  val space = """\s*"""

  val any = """(.|\s)"""

  val s3LocationParser = Global.s3Scheme ~> """[\w-]+""".r ~ ("/" ~> """[\w/:%#$&?()~.=+-]+""".r).? ^^ {
    case ~(bucket, prefix) => S3Location(bucket, prefix.getOrElse(""))
  }

  // TODO: support other auth types
  val awsAuthArgsParser = {
    def parserWithKey = ("aws_access_key_id=" ~> """[\w/+=]+""".r) ~ (";aws_secret_access_key=" ~> """[\w/+=]+""".r) ^^ {
      case ~(accessKeyId, secretAccessKey) => Credentials.WithKey(accessKeyId, secretAccessKey)
    }

    "'" ~> parserWithKey <~ "'"
  }
}

object BaseParser extends BaseParser
