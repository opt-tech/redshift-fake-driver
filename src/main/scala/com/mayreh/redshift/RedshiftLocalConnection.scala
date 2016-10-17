package com.mayreh.redshift

import java.sql.{ResultSet, PreparedStatement}
import java.util.Properties

import org.postgresql.jdbc.PgConnection
import org.postgresql.util.HostSpec

class RedshiftLocalConnection(
  hostSpecs: Array[HostSpec],
  user: String,
  database: String,
  info: Properties,
  url: String
) extends PgConnection(hostSpecs, user, database, info, url) {
  override def prepareStatement(sql: String): PreparedStatement = {
    super.prepareStatement(sql)
  }

  override def execSQLQuery(s: String): ResultSet = {
    super.execSQLQuery(s)
  }

  override def execSQLQuery(s: String, resultSetType: Int, resultSetConcurrency: Int): ResultSet = {
    super.execSQLQuery(s, resultSetType, resultSetConcurrency)
  }
}
