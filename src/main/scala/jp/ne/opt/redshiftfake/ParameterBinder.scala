package jp.ne.opt.redshiftfake

import java.sql.PreparedStatement

sealed abstract class ParameterBinder {
  def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int): Unit
}

object ParameterBinder {

  class TinyInt extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setShort(parameterIndex, BigDecimal(rawValue).toShort)
    }
  }

  class SmallInt extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setShort(parameterIndex, BigDecimal(rawValue).toShort)
    }
  }

  class Integer extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setInt(parameterIndex, BigDecimal(rawValue).toInt)
    }
  }

  class BigInt extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setLong(parameterIndex, BigDecimal(rawValue).toLong)
    }
  }

  class Float extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setDouble(parameterIndex, BigDecimal(rawValue).toDouble)
    }
  }

  class Real extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setFloat(parameterIndex, BigDecimal(rawValue).toFloat)
    }
  }

  class Double extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setDouble(parameterIndex, BigDecimal(rawValue).toDouble)
    }
  }

  class Numeric extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setBigDecimal(parameterIndex, BigDecimal(rawValue).bigDecimal)
    }
  }

  class Decimal extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setBigDecimal(parameterIndex, BigDecimal(rawValue).bigDecimal)
    }
  }

  class Char extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setString(parameterIndex, rawValue)
    }
  }

  class Varchar extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setString(parameterIndex, rawValue)
    }
  }

  class LongVarchar extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setString(parameterIndex, rawValue)
    }
  }

  class Date extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      ???
    }
  }

  class Timestamp extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      ???
    }
  }

  class Boolean extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setBoolean(parameterIndex, rawValue.toBoolean)
    }
  }

  class NChar extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setString(parameterIndex, rawValue)
    }
  }

  class NVarchar extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setString(parameterIndex, rawValue)
    }
  }

  class LongNVarchar extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      statement.setString(parameterIndex, rawValue)
    }
  }

  class TimestampWithTimezone extends ParameterBinder {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int) = {
      ???
    }
  }

  def apply(jdbcType: JdbcType): ParameterBinder = jdbcType match {
    case JdbcType.Bit => throw new UnsupportedOperationException(s"Redshift does not support $jdbcType")
    case JdbcType.TinyInt => new TinyInt
    case JdbcType.SmallInt => new SmallInt
    case JdbcType.Integer => new Integer
    case JdbcType.BigInt => new BigInt
    case JdbcType.Float => new Float
    case JdbcType.Real => new Real
    case JdbcType.Double => new Double
    case JdbcType.Numeric => new Numeric
    case JdbcType.Decimal => new Decimal
    case JdbcType.Char => new Char
    case JdbcType.Varchar => new Varchar
    case JdbcType.LongVarchar => new LongVarchar
    case JdbcType.Date => new Date
    case JdbcType.Time => throw new UnsupportedOperationException(s"Redshift does not support $jdbcType")
    case JdbcType.Timestamp => new Timestamp
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
    case JdbcType.Boolean => new Boolean
    case JdbcType.RowId => throw new UnsupportedOperationException(s"Redshift does not support $jdbcType")
    case JdbcType.NChar => new NChar
    case JdbcType.NVarchar => new NVarchar
    case JdbcType.LongNVarchar => new LongNVarchar
    case JdbcType.NClob => throw new UnsupportedOperationException(s"Redshift does not support $jdbcType")
    case JdbcType.SqlXml => throw new UnsupportedOperationException(s"Redshift does not support $jdbcType")
    case JdbcType.RefCursor => throw new UnsupportedOperationException(s"Redshift does not support $jdbcType")
    case JdbcType.TimeWithTimezone => throw new UnsupportedOperationException(s"Redshift does not support $jdbcType")
    case JdbcType.TimestampWithTimezone => new TimestampWithTimezone
  }
}
