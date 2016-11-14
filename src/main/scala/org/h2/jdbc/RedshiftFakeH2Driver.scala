package org.h2.jdbc

import java.sql.{DriverManager, Connection}
import java.util.Properties

import org.h2.Driver

class RedshiftFakeH2Driver extends Driver {
  import RedshiftFakeH2Driver._

  override def connect(url: String, info: Properties): Connection = {
    new RedshiftFakeH2Connection(url.replaceFirst(urlPrefix, "jdbc:h2"), info)
  }

  override def acceptsURL(url: String): Boolean = url.startsWith(urlPrefix)
}

object RedshiftFakeH2Driver {
  private[this] val driverInstance = new RedshiftFakeH2Driver
  DriverManager.registerDriver(driverInstance)

  private val urlPrefix = "jdbc:h2redshift"

  def createConnection(url: String, prop: java.util.Properties): Connection = {
    DriverManager.getConnection(url, prop)
  }
}
