package jp.ne.opt.redshiftfake.read

import jawn.ast.{JArray, JParser}
import jp.ne.opt.redshiftfake.parse.BaseParser
import jp.ne.opt.redshiftfake.s3.S3Location

import scala.util.Success

case class InvalidManifestException(message: String) extends RuntimeException(message)

class Manifest(rawManifest: String) {
  val files: Seq[S3Location] = JParser.parseFromString(rawManifest).map(_.get("entries")) match {
    case Success(JArray(arr)) => arr.map { j =>
      val url = j.get("url").asString
      BaseParser.parse(BaseParser.s3LocationParser, url).getOrElse(throw InvalidManifestException(s"invalid url : $url"))
    }
    case _ => throw InvalidManifestException(s"invalid manifest : $rawManifest")
  }
}
