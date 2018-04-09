package jp.ne.opt.redshiftfake.parse.compat

import net.sf.jsqlparser.expression.Function

/**
  * Created by frankfarrell on 09/04/2018.
  */
object FunctionVisitor {
  def visit(function: Function): Unit = {
    function.getName.toLowerCase match {
      case "getdate" =>
        function.setName("now")
      //https://www.postgresql.org/docs/9.5/static/functions-conditional.html
      case "nvl" =>
        function.setName("coalesce")
      //https://docs.aws.amazon.com/redshift/latest/dg/r_LISTAGG.html
      case "listagg" =>
        //https://www.postgresql.org/docs/current/static/functions-aggregate.html
        function.setName("string_agg")
        function.setDistinct(false)
      case _ =>;
    }
  }
}
