package jp.ne.opt.redshiftfake.read.json

import jawn.ast.{JValue, JArray, JParser}

import scala.annotation.tailrec

class Jsonpaths(rawJsonpaths: String) {
  sealed abstract class Path
  object Path {
    case class Property(name: String) extends Path
    case class Index(n: Int) extends Path
  }

  class Jsonpath(rawJsonpath: String) {
    /**
     * FIXME: Too naive and inefficient implementation.
     */
    private[this] val parsedJsonpath: Seq[Path] = rawJsonpath.drop(1).split(']').map(_.drop(1)).map { path =>
      val singleQuoteLiteral = """'(\w*)'""".r
      val doubleQuoteLiteral = """"(\w*)"""".r
      path match {
        case singleQuoteLiteral(string) => Path.Property(string)
        case doubleQuoteLiteral(string) => Path.Property(string)
        case _ => Path.Index(path.toInt)
      }
    }

    def jValueOf(json: String): JValue = {
      @tailrec def loop(j: JValue, paths: Seq[Path]): JValue = paths match {
        case Nil => j
        case h +: t => h match {
          case Path.Property(name) => loop(j.get(name), t)
          case Path.Index(n) => loop(j.get(n), t)
        }
      }
      loop(JParser.parseFromString(json).get, parsedJsonpath)
    }
  }

  private[this] val parsedJsonpaths: Vector[Jsonpath] = JParser.parseFromString(rawJsonpaths).toOption.map(_.get("jsonpaths")).collect {
    case JArray(array) => array.map(p => new Jsonpath(p.asString))
  }.getOrElse(throw InvalidJsonException(s"invalid jsonpaths : $rawJsonpaths")).toVector

  val columnSize = parsedJsonpaths.size

  def bigDecimalOf(json: String, index: Int): BigDecimal = parsedJsonpaths(index).jValueOf(json).asBigDecimal
  def bigIntOf(json: String, index: Int): BigInt = parsedJsonpaths(index).jValueOf(json).asBigInt
  def booleanOf(json: String, index: Int): Boolean = parsedJsonpaths(index).jValueOf(json).asBoolean
  def doubleOf(json: String, index: Int): Double = parsedJsonpaths(index).jValueOf(json).asDouble
  def intOf(json: String, index: Int): Int = parsedJsonpaths(index).jValueOf(json).asInt
  def longOf(json: String, index: Int): Long = parsedJsonpaths(index).jValueOf(json).asLong
  def stringOf(json: String, index: Int): String = parsedJsonpaths(index).jValueOf(json).asString
}
