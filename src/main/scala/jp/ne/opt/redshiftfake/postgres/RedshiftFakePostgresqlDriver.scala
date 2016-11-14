package jp.ne.opt.redshiftfake.postgres

import java.sql.Connection
import java.util.Properties

import org.postgresql.Driver

class RedshiftFakePostgresqlDriver extends Driver {
  override def connect(url: String, info: Properties): Connection = {
    super.connect(url, info)
  }
}
