package jp.ne.opt.redshiftfake

case class Column(rawValue: String)

case class Row(columns: Seq[Column])
