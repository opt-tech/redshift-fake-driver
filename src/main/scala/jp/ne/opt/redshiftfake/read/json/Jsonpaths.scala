package jp.ne.opt.redshiftfake.read.json

import jawn.ast.{JValue, JArray, JParser}

import scala.annotation.tailrec

class Jsonpaths(rawJsonpaths: String) {
  sealed abstract class Path
  object Path {
    case class Property(name: String) extends Path
    case class Index(n: Int) extends Path
  }

  private[this] class Jsonpath(rawJsonpath: String) {
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

    def jValueOf(parsedDocument: JValue): JValue = {
      @tailrec def loop(j: JValue, paths: Seq[Path]): JValue = paths match {
        case Nil => j
        case h +: t => h match {
          case Path.Property(name) => loop(j.get(name), t)
          case Path.Index(n) => loop(j.get(n), t)
        }
      }
      loop(parsedDocument, parsedJsonpath)
    }
  }

  class IndexedReader(document: String) {
    private[this] val parsedDocument = JParser.parseFromString(document).get

    def numberOf(index: Int): BigDecimal = parsedJsonpaths(index).jValueOf(parsedDocument).asBigDecimal
    def booleanOf(index: Int): Boolean = parsedJsonpaths(index).jValueOf(parsedDocument).asBoolean
    def stringOf(index: Int): String = parsedJsonpaths(index).jValueOf(parsedDocument).asString
  }

  private[this] val parsedJsonpaths: Vector[Jsonpath] = JParser.parseFromString(rawJsonpaths).toOption.map(_.get("jsonpaths")).collect {
    case JArray(array) => array.map(p => new Jsonpath(p.asString))
  }.getOrElse(throw InvalidJsonException(s"invalid jsonpaths : $rawJsonpaths")).toVector

  val columnSize = parsedJsonpaths.size

  def mkReader(document: String): IndexedReader = new IndexedReader(document)
}
