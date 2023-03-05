package jp.ne.opt.redshiftfake

import java.sql.{DriverManager, Connection}
import java.util.Properties

import jp.ne.opt.redshiftfake.util.Loan.using
import org.scalatest.{Outcome, FixtureTestSuite}

trait H2Sandbox { self: FixtureTestSuite =>

  type FixtureParam = Connection

  override def withFixture(test: OneArgTest): Outcome = {
    val url = "jdbc:redshifth2:mem:redshift;MODE=PostgreSQL;DATABASE_TO_UPPER=false"
    val prop = new Properties()
    prop.setProperty("driver", "jp.ne.opt.redshiftfake.h2.Driver")
    prop.setProperty("user", "sa")

    Class.forName("jp.ne.opt.redshiftfake.h2.Driver")
    using(DriverManager.getConnection(url, prop))(test)
  }
}
