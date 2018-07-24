package jp.ne.opt.redshiftfake

case class FakeAmazonSQLException(msg: String) extends RuntimeException(msg)
