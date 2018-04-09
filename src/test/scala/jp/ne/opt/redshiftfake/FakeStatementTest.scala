package jp.ne.opt.redshiftfake

import java.lang.String
import java.sql.{Connection, Statement}

import jp.ne.opt.redshiftfake.s3.S3Service
import org.scalatest.FlatSpec
import org.scalamock.scalatest
import org.scalamock.scalatest.MockFactory

/**
  * Created by frankfarrell on 01/03/2018.
  */
class FakeStatementTest extends FlatSpec with MockFactory {

  val mockStatement = mock[Statement]
  val mockConnection = mock[Connection]
  val mockStatementType = StatementType.Plain
  val mockS3Service = mock[S3Service]

  val fakeStatementUnderTest = new FakeStatement(mockStatement, mockConnection, mockStatementType, mockS3Service)

  it should "convert convert listagg to string_agg" in {
    val alterTableWithRedshiftFunction = "ALTER TABLE transportUsageFact ADD COLUMN loadTimestamp TIMESTAMP DEFAULT GETDATE() NOT NULL"

    val create = "ALTER TABLE transportUsageFact ADD COLUMN loadTimestamp TIMESTAMP"
    val default = "ALTER TABLE transportUsageFact ALTER COLUMN loadTimestamp SET DEFAULT GETDATE()"
    val notnunll = "ALTER TABLE transportUsageFact ALTER COLUMN loadTimestamp SET NOT NULL"


    ((x: String) => mockStatement.execute(x)).expects("ALTER TABLE transportUsageFact ADD COLUMN loadTimestamp TIMESTAMP")

    fakeStatementUnderTest.execute(create)

    //fakeStatementUnderTest.execute(default)

    fakeStatementUnderTest.execute(notnunll)


  }

}
