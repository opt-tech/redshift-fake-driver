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

    val selectStatementWithMedian = "select median(age)"
    assert(QueryCompatibilityUnderTest.dropIncompatibilities(selectStatementWithMedian)
      .equalsIgnoreCase("select percentile_cont(0.5) within group (order by age)"))

    val selectStatementWithNVL2 = "select nvl2(name, 'a', 'b')from sales"
    assert(QueryCompatibilityUnderTest.dropIncompatibilities(selectStatementWithNVL2)
      .equalsIgnoreCase("select case name is not null 'a' else 'b' end from sales"))

  }

  it should "remove the appoximate keyword" in {
    val selectStatementWithApproximatePercentileDisc = "select approximate percentile_disc(0.5) within group (order by totalprice) from sales"
    assert(QueryCompatibilityUnderTest.dropIncompatibilities(selectStatementWithApproximatePercentileDisc)
      .equalsIgnoreCase("select percentile_disc(0.5) within group (order by totalprice) from sales"))

    val selectStatementWithApproximateCount = "select approximate count(*) from sales"
    assert(QueryCompatibilityUnderTest.dropIncompatibilities(selectStatementWithApproximateCount)
      .equalsIgnoreCase("select count(*) from sales"))
  }
}

