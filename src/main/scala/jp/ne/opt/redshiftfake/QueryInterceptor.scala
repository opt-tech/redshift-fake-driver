package jp.ne.opt.redshiftfake

import java.sql.Connection

import jp.ne.opt.redshiftfake.parse.CopyQueryParser
import jp.ne.opt.redshiftfake.util.using

class QueryInterceptor(connection: Connection) {
  def interceptCopy[A](sql: String)(result: => A, otherwise: => A) = {
    CopyQueryParser.parse(sql) match {
      case None =>
        otherwise
      case Some(copyQuery) =>
        using(connection.createStatement()) { stmt =>
          stmt.execute("select * from foo_bar")
          println("--------------------------------")
          println(stmt.getResultSet)
          println("--------------------------------")

          result
        }
    }
  }
}
