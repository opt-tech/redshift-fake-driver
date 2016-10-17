package com.mayreh.redshift

import java.sql.Types._

case class UnknownJdbcTypeException(value: Int) extends RuntimeException(s"unknown type. value: $value")

sealed abstract class JdbcType(val rawType: Int)

object JdbcType {
  case object Bit extends JdbcType(BIT)
  case object TinyInt extends JdbcType(TINYINT)
  case object SmallInt extends JdbcType(SMALLINT)
  case object Integer extends JdbcType(INTEGER)
  case object BigInt extends JdbcType(BIGINT)
  case object Float extends JdbcType(FLOAT)
  case object Real extends JdbcType(REAL)
  case object Double extends JdbcType(DOUBLE)
  case object Numeric extends JdbcType(NUMERIC)
  case object Decimal extends JdbcType(DECIMAL)
  case object Char extends JdbcType(CHAR)
  case object Varchar extends JdbcType(VARCHAR)
  case object LongVarchar extends JdbcType(LONGVARCHAR)
  case object Date extends JdbcType(DATE)
  case object Time extends JdbcType(TIME)
  case object Timestamp extends JdbcType(TIMESTAMP)
  case object Binary extends JdbcType(BINARY)
  case object VarBinary extends JdbcType(VARBINARY)
  case object LongVarBinary extends JdbcType(LONGVARBINARY)
  case object Null extends JdbcType(NULL)
  case object Other extends JdbcType(OTHER)
  case object JavaObject extends JdbcType(JAVA_OBJECT)
  case object Distinct extends JdbcType(DISTINCT)
  case object Struct extends JdbcType(STRUCT)
  case object Array extends JdbcType(ARRAY)
  case object Blob extends JdbcType(BLOB)
  case object Clob extends JdbcType(CLOB)
  case object Ref extends JdbcType(REF)
  case object DataLink extends JdbcType(DATALINK)
  case object Boolean extends JdbcType(BOOLEAN)
  case object RowId extends JdbcType(ROWID)
  case object NChar extends JdbcType(NCHAR)
  case object NVarchar extends JdbcType(NVARCHAR)
  case object LongNVarchar extends JdbcType(LONGNVARCHAR)
  case object NClob extends JdbcType(NCLOB)
  case object SqlXml extends JdbcType(SQLXML)
  case object RefCursor extends JdbcType(REF_CURSOR)
  case object TimeWithTimezone extends JdbcType(TIME_WITH_TIMEZONE)
  case object TimestampWithTimezone extends JdbcType(TIMESTAMP_WITH_TIMEZONE)

  val values = Set(
    Bit,
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    Float,
    Real,
    Double,
    Numeric,
    Decimal,
    Char,
    Varchar,
    LongVarchar,
    Date,
    Time,
    Timestamp,
    Binary,
    VarBinary,
    LongVarBinary,
    Null,
    Other,
    JavaObject,
    Distinct,
    Struct,
    Array,
    Blob,
    Clob,
    Ref,
    DataLink,
    Boolean,
    RowId,
    NChar,
    NVarchar,
    LongNVarchar,
    NClob,
    SqlXml,
    RefCursor,
    TimeWithTimezone,
    TimestampWithTimezone
  )

  private[this] val valueMap = values.map(v => (v.rawType, v)).toMap

  def valueOf(rawType: Int): JdbcType = valueMap.getOrElse(rawType, throw UnknownJdbcTypeException(rawType))
}
