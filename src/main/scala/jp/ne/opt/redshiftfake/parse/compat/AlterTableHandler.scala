package jp.ne.opt.redshiftfake.parse.compat

/**
  * Created by frankfarrell on 01/03/2018.
  *
  * Converts redshift alter table to postgres equivalents
  *
  * https://docs.aws.amazon.com/redshift/latest/dg/r_ALTER_TABLE.html#r_ALTER_TABLE-synopsis
  * =>
  * https://www.postgresql.org/docs/9.1/static/sql-altertable.html
  *
  * Cases
  * 1) Add column => can jsql handle this? No
  * 2) Add column with default and/or not null => multiple postgresql statements, drop encoding.
  *   i) If NOT NULL and no default -> throw
  *   ii) If default is function with params => throw
  * 3) Rename table => jsql?
  * 4) Rename column => jsql?
  * 5) Drop column => jsql?
  */
class AlterTableHandler {
  def handle(sql: String): String ={
    // Do stuff above and return concatenated statements
    // Can execute handle multiple statements in 1?
    sql
    //TODO

    /*
    Parse out any function in DEFAULT
    DEFAULT has to be a variable free expression
    So, it can be a value or an argumentless function -> Call FunctionVisitor.visit()
     */
  }

  def matches(sql: String): Boolean ={
    //some regex to determine if we need to do something jsql cant do
    true
    //TODO
  }
}
