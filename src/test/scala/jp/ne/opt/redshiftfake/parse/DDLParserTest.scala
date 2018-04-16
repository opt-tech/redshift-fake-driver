package jp.ne.opt.redshiftfake.parse

import org.scalatest.FlatSpec

class DDLParserTest extends FlatSpec {
  it should "sanitize Redshift specific DDL" in {
    val ddl =
      """
        |CREATE TABLE foo_bar(a int ENCODE ZSTD, b boolean)
        |DISTSTYLE ALL
        |DISTKEY(a)
        |INTERLEAVED SORTKEY(a, b);
        |""".stripMargin

    val expected =
      """
        |CREATE TABLE foo_bar(a int, b boolean)
        |
        |
        |;
        |""".stripMargin

    assert(DDLParser.sanitize(ddl) == expected)
  }

  it should "sanitize DDL with quoted identifier" in {
    val ddl =
      """CREATE TEMPORARY TABLE test_table
          |(
          |  "test_identifier" INT NOT NULL,
          |)
          |DISTKEY("test_identifier")
          |SORTKEY("test_identifier");
          |""".

        stripMargin
    val expected =
      """CREATE TEMPORARY TABLE test_table
          |(
          |  "test_identifier" INT NOT NULL,
          |);
          |""".stripMargin

    assert(DDLParser.
      sanitize(ddl) == expected)
  }

  it should "Convert default functions in create table statements" in {
    val ddl =
      """
        |CREATE TABLE foo_bar(
        |"installed_by" VARCHAR(100) NOT NULL,
        |"installed_on" TIMESTAMP NOT NULL DEFAULT getdate()
        |)
        |DISTSTYLE ALL
        |DISTKEY(a)
        |INTERLEAVED SORTKEY(a, b);
        |""".stripMargin

    val expected =
      """
        |CREATE TABLE foo_bar(
        |"installed_by" VARCHAR(100) NOT NULL,
        |"installed_on" TIMESTAMP NOT NULL DEFAULT now()
        |)
        |
        |
        |;
        |""".stripMargin

    assert(DDLParser.sanitize(ddl) == expected)
  }

  it should "convert alter table add column with default to postgres equivalent" in {
    val alterTableAddColumn = "ALTER TABLE transportUsageFact ADD COLUMN name VARCHAR(1000) DEFAULT 'anonymous'"

    assert(DDLParser.sanitize(alterTableAddColumn)
      == "ALTER TABLE transportUsageFact ADD COLUMN name VARCHAR(1000);" +
      "ALTER TABLE transportUsageFact ALTER COLUMN name SET DEFAULT 'anonymous'")
  }

  it should "convert alter table add column with null to postgres equivalent" in {
    val alterTableAddColumn = "ALTER TABLE transportUsageFact ADD COLUMN name VARCHAR (1000) NULL"

    assert(DDLParser.sanitize(alterTableAddColumn)
      == "ALTER TABLE transportUsageFact ADD COLUMN name VARCHAR (1000);" +
      "ALTER TABLE transportUsageFact ALTER COLUMN name SET NULL")
  }

  it should "convert alter table add column with default and null to postgres equivalents" in {
    val alterTableAddColumn = "ALTER TABLE transportUsageFact ADD COLUMN loadTimestamp TIMESTAMP DEFAULT GETDATE() NOT NULL"

    assert(DDLParser.sanitize(alterTableAddColumn)
      == "ALTER TABLE transportUsageFact ADD COLUMN loadTimestamp TIMESTAMP;" +
        "ALTER TABLE transportUsageFact ALTER COLUMN loadTimestamp SET DEFAULT now();" +
        "ALTER TABLE transportUsageFact ALTER COLUMN loadTimestamp SET NOT NULL")
  }
}
