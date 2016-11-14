package jp.ne.opt.redshiftfake.read

import jawn.ast.JParser
import jp.ne.opt.redshiftfake.parse.Row

case class InvalidJsonException(message: String) extends RuntimeException(message)

case class JsonReader(json: String) {
  private[this] val jValue = JParser.parseFromString(json).getOrElse(
    throw InvalidJsonException(s"failed to parse json : $json")
  )

  def read(): Seq[Row] = {
    ???
  }
}
