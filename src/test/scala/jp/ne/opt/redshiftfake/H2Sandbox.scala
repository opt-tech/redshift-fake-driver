package jp.ne.opt.redshiftfake

import java.sql.{DriverManager, Connection}
import java.util.Properties

import jp.ne.opt.redshiftfake.util.Loan.using
import org.scalatest.{Outcome, fixture}

trait H2Sandbox { self: fixture.TestSuite =>

  type FixtureParam = Connection

  override def withFixture(test: OneArgTest): Outcome = {
    val url = "jdbc:h2redshift:mem:redshift;MODE=PostgreSQL;DATABASE_TO_UPPER=false"
    val prop = new Properties()
    prop.setProperty("driver", "org.h2.jdbc.FakeH2Driver")
    prop.setProperty("user", "sa")

    Class.forName("org.h2.jdbc.FakeH2Driver")
    using(DriverManager.getConnection(url, prop))(test)
  }
}
