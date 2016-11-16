package jp.ne.opt.redshiftfake

/**
 * Represents a row to be inserted.
 */
case class Row(columns: Seq[Column])

case class Column(rawValue: Option[String])
