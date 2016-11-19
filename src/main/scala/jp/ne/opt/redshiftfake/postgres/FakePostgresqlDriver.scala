package jp.ne.opt.redshiftfake.postgres

import java.sql.{DriverManager, Connection}
import java.util.Properties

import jp.ne.opt.redshiftfake.{Global, FakeConnection}
import jp.ne.opt.redshiftfake.s3.S3ServiceImpl
import org.postgresql.Driver

class FakePostgresqlDriver extends Driver {
  import FakePostgresqlDriver._

  override def connect(url: String, info: Properties): Connection = {
    new FakeConnection(
      DriverManager.getConnection(url.replaceFirst(urlPrefix, "jdbc:postgresql"), info),
      new S3ServiceImpl(Global.s3Endpoint))
  }

  override def acceptsURL(url: String): Boolean = url.startsWith(urlPrefix)
}

object FakePostgresqlDriver {
  private[this] val driverInstance = new FakePostgresqlDriver
  DriverManager.registerDriver(driverInstance)

  private val urlPrefix = "jdbc:postgresqlredshift"

  def createConnection(url: String, prop: java.util.Properties): Connection = {
    DriverManager.getConnection(url, prop)
  }
}
