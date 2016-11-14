package jp.ne.opt.redshiftfake

import java.sql.PreparedStatement

import jp.ne.opt.redshiftfake.parsing.CopyQueryParser

trait QueryInterceptor { self: PreparedStatement =>
  def interceptCopy[A](sql: String)(otherwise: => A) = {
    CopyQueryParser.parse(sql) match {
      case None => otherwise
      case Some(copyQuery) => copyQuery
    }
  }
}
