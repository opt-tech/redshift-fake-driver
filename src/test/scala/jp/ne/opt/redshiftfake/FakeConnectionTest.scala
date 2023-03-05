package jp.ne.opt.redshiftfake

import org.scalatest.flatspec.FixtureAnyFlatSpec

class FakeConnectionTest extends FixtureAnyFlatSpec with H2Sandbox {

  it should "convert and execute alter table add column with default and null to postgres equivalents" in { conn =>
    val stmt = conn.createStatement()

    stmt.execute("CREATE TABLE testDb(id int)")
    stmt.execute("ALTER TABLE testDb ADD COLUMN loadTimestamp TIMESTAMP DEFAULT GETDATE() NOT NULL")
    stmt.execute("INSERT INTO testDb (id) VALUES (55301)")

    stmt.execute("SELECT id, loadTimestamp FROM testDb")
    val rs = stmt.getResultSet
    rs.next()
    assert(rs.getInt("id") == 55301)
    assert(rs.getTimestamp("loadTimestamp") != null)
  }
}
