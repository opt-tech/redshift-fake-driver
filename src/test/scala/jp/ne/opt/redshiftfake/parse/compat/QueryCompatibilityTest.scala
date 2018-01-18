package jp.ne.opt.redshiftfake.parse.compat

import org.scalatest.FlatSpec

/**
  * Created by frankfarrellfarrell on 18/01/2018.
  */

class QueryCompatibilityTest extends FlatSpec {

  object QueryCompatibilityUnderTest extends QueryCompatibility {

  }

  it should "convert redshift functions to postgres equivalents" in {
    val selectStatementWithNVL = "select nvl(name, 'bob') from names"
    assert(QueryCompatibilityUnderTest.dropIncompatibilities(selectStatementWithNVL)
      .equalsIgnoreCase("select coalesce(name, 'bob') from names"))

    val selectStatementWithListAgg = "select listagg(name, ',') from names"
    assert(QueryCompatibilityUnderTest.dropIncompatibilities(selectStatementWithListAgg)
      .equalsIgnoreCase("select string_agg(name, ',') from names"))

    val selectStatementWithListAggWithDistinct = "select listagg(DISTINCT name, ',') from names"
    assert(QueryCompatibilityUnderTest.dropIncompatibilities(selectStatementWithListAggWithDistinct)
      .equalsIgnoreCase("select string_agg(name, ',') from names"))

    val selectStatementWithListAggWithOrdering =
      "select listagg(name, ',') WITHIN GROUP (ORDER BY name) from names group by age"
    assert(QueryCompatibilityUnderTest.dropIncompatibilities(selectStatementWithListAggWithOrdering)
      .equalsIgnoreCase("select string_agg(name, ',') from names group by age"))


    val selectStatementWithGetdate = "select getdate()"
    assert(QueryCompatibilityUnderTest.dropIncompatibilities(selectStatementWithGetdate)
      .equalsIgnoreCase("select now()"))
  }
}

