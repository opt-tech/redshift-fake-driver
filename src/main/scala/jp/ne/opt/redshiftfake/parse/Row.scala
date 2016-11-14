package jp.ne.opt.redshiftfake.parse

case class Column(value: Any)
case class Row(columns: Seq[Column])
