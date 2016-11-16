package jp.ne.opt.redshiftfake.parse

import java.sql.PreparedStatement

abstract class Column[A] {
  def value: A
  def bindToStatement(statement: PreparedStatement): Unit
}

//case class Column(value: Any)

case class Row(columns: Seq[Column])
