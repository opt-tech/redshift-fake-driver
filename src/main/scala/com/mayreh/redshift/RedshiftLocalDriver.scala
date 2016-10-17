package com.mayreh.redshift

import java.sql.Connection
import java.util.Properties

import org.postgresql.Driver

class RedshiftLocalDriver extends Driver {
  override def connect(url: String, info: Properties): Connection = {
    super.connect(url, info)
  }
}
