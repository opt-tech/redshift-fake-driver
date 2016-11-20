package jp.ne.opt.redshiftfake

import java.sql.PreparedStatement

sealed abstract class ParameterBinder {
  def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int): Unit
}

object ParameterBinder {
  case object Bit extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setBoolean(parameterIndex, rawValue match {
        case "t" | "1" => true
        case "f" | "0" => false
        case _ => rawValue.toBoolean
      })
    }
  }
  case object TinyInt extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setShort(parameterIndex, BigDecimal(rawValue).toShort)
    }
  }
  case object SmallInt extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setShort(parameterIndex, BigDecimal(rawValue).toShort)
    }
  }
  case object Integer extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setInt(parameterIndex, BigDecimal(rawValue).toInt)
    }
  }
  case object BigInt extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setLong(parameterIndex, BigDecimal(rawValue).toLong)
    }
  }
  case object Float extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setDouble(parameterIndex, BigDecimal(rawValue).toDouble)
    }
  }
  case object Real extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setFloat(parameterIndex, BigDecimal(rawValue).toFloat)
    }
  }
  case object Double extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setDouble(parameterIndex, BigDecimal(rawValue).toDouble)
    }
  }
  case object Numeric extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setBigDecimal(parameterIndex, BigDecimal(rawValue).bigDecimal)
    }
  }
  case object Decimal extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setBigDecimal(parameterIndex, BigDecimal(rawValue).bigDecimal)
    }
  }
  case object Char extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setString(parameterIndex, rawValue)
    }
  }
  case object Varchar extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setString(parameterIndex, rawValue)
    }
  }
  case object LongVarchar extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setString(parameterIndex, rawValue)
    }
  }
  case class Date(dateFormatType: DateFormatType) extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setDate(
        parameterIndex,
        RedshiftDateFormat.forType(dateFormatType).parseSqlDate(rawValue))
    }
  }
  case class Timestamp(timeFormatType: TimeFormatType) extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setTimestamp(
        parameterIndex,
        RedshiftTimeFormat.forType(timeFormatType).parseSqlTimestamp(rawValue))
    }
  }
  case object Boolean extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      Bit.bind(rawValue, statement, parameterIndex)
    }
  }
  case object NChar extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setString(parameterIndex, rawValue)
    }
  }
  case object NVarchar extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setString(parameterIndex, rawValue)
    }
  }
  case object LongNVarchar extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setString(parameterIndex, rawValue)
    }
  }
  case class TimestampWithTimezone(timeFormatType: TimeFormatType) extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setTimestamp(
        parameterIndex,
        RedshiftTimeFormat.forType(timeFormatType).parseSqlTimestamp(rawValue))
    }
  }

  def apply(jdbcType: JdbcType, dateFormatType: DateFormatType, timeFormatType: TimeFormatType): ParameterBinder = jdbcType match {
    case JdbcType.Bit => Bit
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
    case JdbcType.Date => Date(dateFormatType)
    case JdbcType.Time => throw new UnsupportedOperationException(s"Redshift does not support $jdbcType")
    case JdbcType.Timestamp => Timestamp(timeFormatType)
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

    // since 4.2
    // case JdbcType.RefCursor => throw new UnsupportedOperationException(s"Redshift does not support $jdbcType")
    // case JdbcType.TimeWithTimezone => throw new UnsupportedOperationException(s"Redshift does not support $jdbcType")
    // case JdbcType.TimestampWithTimezone => TimestampWithTimezone(timeFormatType)
  }
}
