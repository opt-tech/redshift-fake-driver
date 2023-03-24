package jp.ne.opt.redshiftfake.read

import org.typelevel.jawn.ast._

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

    def valueAt(index: Int): Option[String] = {
      val jValue = parsedJsonpaths(index).jValueOf(parsedDocument)
      jValue match {
        case JNull => None
        case JString(s) => Some(s)
        case _ => Some(jValue.render())
      }
    }
  }

  private[this] val parsedJsonpaths: Vector[Jsonpath] = JParser.parseFromString(rawJsonpaths).toOption.map(_.get("jsonpaths")).collect {
    case JArray(array) => array.map(p => new Jsonpath(p.asString))
  }.get.toVector

  val columnSize = parsedJsonpaths.size

  def mkReader(document: String): IndexedReader = new IndexedReader(document)
}
