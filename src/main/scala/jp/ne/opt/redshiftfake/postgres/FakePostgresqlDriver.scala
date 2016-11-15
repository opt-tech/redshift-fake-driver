package jp.ne.opt.redshiftfake.postgres

import java.sql.Connection
import java.util.Properties

import jp.ne.opt.redshiftfake.{Global, FakeConnection}
import jp.ne.opt.redshiftfake.s3.S3ServiceImpl
import org.postgresql.Driver

class FakePostgresqlDriver extends Driver {
  override def connect(url: String, info: Properties): Connection = {
    new FakeConnection(super.connect(url, info), new S3ServiceImpl(Global.endpoint, Global.s3Credentials))
  }
}
