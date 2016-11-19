package jp.ne.opt.redshiftfake

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

import scala.util.Try

case class InvalidFormatException(message: String) extends RuntimeException(message)

sealed abstract class RedshiftDateFormat {
  def parseSqlDate(string: String): java.sql.Date
}
object RedshiftDateFormat {
  case class FixedDateFormat(pattern: String) extends RedshiftDateFormat {
    def parseSqlDate(string: String): java.sql.Date = {
      val sdf = new SimpleDateFormat(pattern)

      val millis = sdf.parse(string).getTime
      val cal = Calendar.getInstance()
      cal.setTimeInMillis(millis)
      cal.set(Calendar.HOUR_OF_DAY, 0)
      cal.set(Calendar.MINUTE, 0)
      cal.set(Calendar.SECOND, 0)
      cal.set(Calendar.MILLISECOND, 0)
      new java.sql.Date(millis)
    }
  }

  case class AttemptFormats(formats: FixedDateFormat*) extends RedshiftDateFormat {
    def parseSqlDate(string: String): java.sql.Date = ParseUtil.attempt(formats)(_.parseSqlDate)(string)
  }

  def forType(formatType: DateFormatType): RedshiftDateFormat = formatType match {
    case DateFormatType.Default => FixedDateFormat("yyyy-MM-dd")
    case DateFormatType.Custom(pattern) => FixedDateFormat(ParseUtil.toStandardDatePart(pattern))
    case DateFormatType.Auto =>
      // FIXME: incomplete implementation
      AttemptFormats(
        FixedDateFormat("yyyy-MM-dd"),
        FixedDateFormat("yyyyMMdd"),
        FixedDateFormat("yyyy/MM/dd"),
        FixedDateFormat("MM/dd/yyyy"),
        FixedDateFormat("dd.MM.yyyy")
      )
  }
}

sealed abstract class RedshiftTimeFormat {
  def parseSqlTimestamp(string: String): Timestamp
}
object RedshiftTimeFormat {
  case class FixedTimeFormat(pattern: String) extends RedshiftTimeFormat {
    def parseSqlTimestamp(string: String): Timestamp = {
      val sdf = new SimpleDateFormat(pattern)
      new Timestamp(sdf.parse(string).getTime)
    }
  }

  case class AttemptFormats(formats: FixedTimeFormat*) extends RedshiftTimeFormat {
    def parseSqlTimestamp(string: String): Timestamp = ParseUtil.attempt(formats)(_.parseSqlTimestamp)(string)
  }

  def forType(formatType: TimeFormatType): RedshiftTimeFormat = formatType match {
    case TimeFormatType.Default => FixedTimeFormat("yyyy-MM-dd HH:mm:ss.SSS")
    case TimeFormatType.Custom(pattern) => FixedTimeFormat(ParseUtil.toStandardDatePart(pattern))
    case TimeFormatType.Epochsecs => new RedshiftTimeFormat {
      def parseSqlTimestamp(string: String): Timestamp = new Timestamp(1000L * string.toLong)
    }
    case TimeFormatType.EpochMillisecs => new RedshiftTimeFormat {
      def parseSqlTimestamp(string: String): Timestamp = new Timestamp(string.toLong)
    }
    case TimeFormatType.Auto =>
      // FIXME: incomplete implementation
      AttemptFormats(
        FixedTimeFormat("yyyy-MM-dd HH:mm:ss.SSS"),
        FixedTimeFormat("yyyy-MM-dd HH:mm:ss"),
        FixedTimeFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"),
        FixedTimeFormat("yyyy-MM-dd'T'HH:mm:ss"),
        FixedTimeFormat("yyyyMMdd HHmmss.SSS"),
        FixedTimeFormat("yyyyMMdd HHmmss"),
        FixedTimeFormat("yyyy/MM/dd HH:mm:ss.SSS"),
        FixedTimeFormat("yyyy/MM/dd HH:mm:ss"),
        FixedTimeFormat("MM/dd/yyyy HH:mm:ss.SSS"),
        FixedTimeFormat("MM/dd/yyyy HH:mm:ss")
      )
  }
}

object ParseUtil {
   def toStandardDatePart(pattern: String): String = {
    val partMappings = Seq(
      ("YYYY", "yyyy"),
      ("YY", "yy"),
      ("MM", "MM"),
      ("MON", "MMMM"),
      ("DD", "dd"),
      ("HH24", "HH"),
      ("HH12", "hh"),
      ("HH", "HH"),
      ("MI", "mm"),
      ("SS", "ss.SSS"),
      ("OF", "'Z'")
    )

    def replaceLoop(current: String, mappings: Seq[(String, String)]): String = mappings match {
      case (before, after) +: rest => replaceLoop(current.replace(before, after), rest)
      case _ => current
    }

    replaceLoop(pattern, partMappings)
  }

  def attempt[A, B](formats: Seq[A])(parse: A => String => B)(string: String): B = {
    def loop(xs: Seq[A]): B = xs match {
      case h +: t => Try(parse(h)(string)).getOrElse(loop(t))
      case _ => throw InvalidFormatException(s"failed to parse : $string")
    }
    loop(formats)
  }
}
