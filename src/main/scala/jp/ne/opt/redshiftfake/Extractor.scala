package jp.ne.opt.redshiftfake

import java.sql.ResultSet
import java.text.SimpleDateFormat

sealed abstract class Extractor {
  def extract(resultSet: ResultSet, columnIndex: Int): String
  def extractOpt(resultSet: ResultSet, columnIndex: Int): Option[String] =
    if (resultSet.getObject(columnIndex) == null) None else Some(extract(resultSet, columnIndex))
}

object Extractor {

  case object TinyInt extends Extractor {
    def extract(resultSet: ResultSet, columnIndex: Int) = resultSet.getShort(columnIndex).toString
  }
  case object SmallInt extends Extractor {
    def extract(resultSet: ResultSet, columnIndex: Int) = resultSet.getShort(columnIndex).toString
  }
  case object Integer extends Extractor {
    def extract(resultSet: ResultSet, columnIndex: Int) = resultSet.getInt(columnIndex).toString
  }
  case object BigInt extends Extractor {
    def extract(resultSet: ResultSet, columnIndex: Int) = resultSet.getLong(columnIndex).toString
  }
  case object Float extends Extractor {
    def extract(resultSet: ResultSet, columnIndex: Int) = resultSet.getDouble(columnIndex).toString
  }
  case object Real extends Extractor {
    def extract(resultSet: ResultSet, columnIndex: Int) = resultSet.getFloat(columnIndex).toString
  }
  case object Double extends Extractor {
    def extract(resultSet: ResultSet, columnIndex: Int) = resultSet.getDouble(columnIndex).toString
  }
  case object Numeric extends Extractor {
    def extract(resultSet: ResultSet, columnIndex: Int) = resultSet.getBigDecimal(columnIndex).toString
  }
  case object Decimal extends Extractor {
    def extract(resultSet: ResultSet, columnIndex: Int) = resultSet.getBigDecimal(columnIndex).toString
  }
  case object Char extends Extractor {
    def extract(resultSet: ResultSet, columnIndex: Int) = resultSet.getString(columnIndex)
  }
  case object Varchar extends Extractor {
    def extract(resultSet: ResultSet, columnIndex: Int) = resultSet.getString(columnIndex)
  }
  case object LongVarchar extends Extractor {
    def extract(resultSet: ResultSet, columnIndex: Int) = resultSet.getString(columnIndex)
  }
  case object Date extends Extractor {
    def extract(resultSet: ResultSet, columnIndex: Int) = {
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      sdf.format(resultSet.getDate(columnIndex))
    }
  }
  case object Timestamp extends Extractor {
    def extract(resultSet: ResultSet, columnIndex: Int) = {
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      sdf.format(resultSet.getTimestamp(columnIndex))
    }
  }
  case object Boolean extends Extractor {
    def extract(resultSet: ResultSet, columnIndex: Int) = if (resultSet.getBoolean(columnIndex)) "t" else "f"
  }
  case object NChar extends Extractor {
    def extract(resultSet: ResultSet, columnIndex: Int) = resultSet.getString(columnIndex)
  }
  case object NVarchar extends Extractor {
    def extract(resultSet: ResultSet, columnIndex: Int) = resultSet.getString(columnIndex)
  }
  case object LongNVarchar extends Extractor {
    def extract(resultSet: ResultSet, columnIndex: Int) = resultSet.getString(columnIndex)
  }
  case object TimestampWithTimezone extends Extractor {
    def extract(resultSet: ResultSet, columnIndex: Int) = {
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ")
      sdf.format(resultSet.getTimestamp(columnIndex))
    }
  }

  def apply(jdbcType: JdbcType): Extractor = jdbcType match {
    case JdbcType.Bit => throw new UnsupportedOperationException(s"Redshift does not support $jdbcType")
    case JdbcType.TinyInt => TinyInt
    case JdbcType.SmallInt => SmallInt
    case JdbcType.Integer => Integer
    case JdbcType.BigInt => BigInt
    case JdbcType.Float => Float
    case JdbcType.Real => Real
    case JdbcType.Double => Double
    case JdbcType.Numeric => Numeric
    case JdbcType.Decimal => Decimal
    case JdbcType.Char => Char
    case JdbcType.Varchar => Varchar
    case JdbcType.LongVarchar => LongVarchar
    case JdbcType.Date => Date
    case JdbcType.Time => throw new UnsupportedOperationException(s"Redshift does not support $jdbcType")
    case JdbcType.Timestamp => Timestamp
    case JdbcType.Binary => throw new UnsupportedOperationException(s"Redshift does not support $jdbcType")
    case JdbcType.VarBinary => throw new UnsupportedOperationException(s"Redshift does not support $jdbcType")
    case JdbcType.LongVarBinary => throw new UnsupportedOperationException(s"Redshift does not support $jdbcType")
    case JdbcType.Null => throw new UnsupportedOperationException(s"Redshift does not support $jdbcType")
    case JdbcType.Other => throw new UnsupportedOperationException(s"Redshift does not support $jdbcType")
    case JdbcType.JavaObject => throw new UnsupportedOperationException(s"Redshift does not support $jdbcType")
    case JdbcType.Distinct => throw new UnsupportedOperationException(s"Redshift does not support $jdbcType")
    case JdbcType.Struct => throw new UnsupportedOperationException(s"Redshift does not support $jdbcType")
    case JdbcType.Array => throw new UnsupportedOperationException(s"Redshift does not support $jdbcType")
    case JdbcType.Blob => throw new UnsupportedOperationException(s"Redshift does not support $jdbcType")
    case JdbcType.Clob => throw new UnsupportedOperationException(s"Redshift does not support $jdbcType")
    case JdbcType.Ref => throw new UnsupportedOperationException(s"Redshift does not support $jdbcType")
    case JdbcType.DataLink => throw new UnsupportedOperationException(s"Redshift does not support $jdbcType")
    case JdbcType.Boolean => Boolean
    case JdbcType.RowId => throw new UnsupportedOperationException(s"Redshift does not support $jdbcType")
    case JdbcType.NChar => NChar
    case JdbcType.NVarchar => NVarchar
    case JdbcType.LongNVarchar => LongNVarchar
    case JdbcType.NClob => throw new UnsupportedOperationException(s"Redshift does not support $jdbcType")
    case JdbcType.SqlXml => throw new UnsupportedOperationException(s"Redshift does not support $jdbcType")
    case JdbcType.RefCursor => throw new UnsupportedOperationException(s"Redshift does not support $jdbcType")
    case JdbcType.TimeWithTimezone => throw new UnsupportedOperationException(s"Redshift does not support $jdbcType")
    case JdbcType.TimestampWithTimezone => TimestampWithTimezone
  }
}
