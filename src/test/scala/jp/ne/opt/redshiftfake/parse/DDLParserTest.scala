package jp.ne.opt.redshiftfake.parse

import org.scalatest.flatspec.AnyFlatSpec

class DDLParserTest extends AnyFlatSpec {
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

    assert(new DDLParser().sanitize(ddl) == expected)
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

    assert(new DDLParser().sanitize(ddl) == expected)
  }

  it should "replace identity with serial" in {
    val ddl =
      """
        |CREATE TABLE foo_bar(a BIGINT IDENTITY(6, 99))
        |DISTSTYLE ALL
        |DISTKEY(a)
        |INTERLEAVED SORTKEY(a, b);
        |""".stripMargin

    val expected =
      """
        |CREATE TABLE foo_bar(a BIGSERIAL)
        |
        |
        |;
        |""".stripMargin

    assert(new DDLParser().sanitize(ddl) == expected)
  }

  it should "Convert default functions in create table statements" in {
    val ddl =
      """
        |CREATE TABLE foo_bar(
        |"installed_by" VARCHAR(100) NOT NULL,
        |"installed_on" TIMESTAMP NOT NULL DEFAULT getdate()
        |"freetext" TEXT NOT NULL DEFAULT 'blah'
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
        |"freetext" TEXT NOT NULL DEFAULT 'blah'
        |)
        |
        |
        |;
        |""".stripMargin

    assert(new DDLParser().sanitize(ddl) == expected)
  }

  it should "convert alter table add column with default to postgres equivalent" in {
    val alterTableAddColumn = "ALTER TABLE testDb ADD COLUMN name VARCHAR(1000) DEFAULT 'anonymous'"

    assert(new DDLParser().sanitize(alterTableAddColumn)
      == "ALTER TABLE testDb ADD COLUMN name VARCHAR(1000);" +
      "ALTER TABLE testDb ALTER COLUMN name SET DEFAULT 'anonymous'")
  }

  it should "handle boolean defaults" in {
    val alterTableAddColumn = "ALTER TABLE testDb ADD COLUMN booleanColumn BOOLEAN DEFAULT false"

    assert(new DDLParser().sanitize(alterTableAddColumn)
      == "ALTER TABLE testDb ADD COLUMN booleanColumn BOOLEAN;" +
      "ALTER TABLE testDb ALTER COLUMN booleanColumn SET DEFAULT false")
  }

  it should "convert alter table add column with null to postgres equivalent" in {
    val alterTableAddColumn = "ALTER TABLE testDb ADD COLUMN name VARCHAR (1000) NULL"

    assert(new DDLParser().sanitize(alterTableAddColumn)
      == "ALTER TABLE testDb ADD COLUMN name VARCHAR (1000);" +
      "ALTER TABLE testDb ALTER COLUMN name DROP NOT NULL")
  }

  it should "convert alter table add column with default and null to postgres equivalents" in {
    val alterTableAddColumn = "ALTER TABLE testDb ADD COLUMN loadTimestamp TIMESTAMP DEFAULT GETDATE() NOT NULL"

    assert(new DDLParser().sanitize(alterTableAddColumn)
      == "ALTER TABLE testDb ADD COLUMN loadTimestamp TIMESTAMP;" +
        "ALTER TABLE testDb ALTER COLUMN loadTimestamp SET DEFAULT now();" +
        "ALTER TABLE testDb ALTER COLUMN loadTimestamp SET NOT NULL")
  }

  it should "drop add foreign key constraints" in {
    val alterTableAddColumn = "ALTER TABLE testDb ADD FOREIGN KEY (name) REFERENCES names (id);"

    assert(new DDLParser().sanitize(alterTableAddColumn)
      == "")
  }
}
