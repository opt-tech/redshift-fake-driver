package jp.ne.opt.redshiftfake.parsing

case class Column(value: Any)
case class Row(columns: Seq[Column])
