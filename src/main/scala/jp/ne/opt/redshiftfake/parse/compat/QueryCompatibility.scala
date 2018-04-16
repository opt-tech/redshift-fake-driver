package jp.ne.opt.redshiftfake.parse.compat

import net.sf.jsqlparser.parser.CCJSqlParserUtil

trait QueryCompatibility {

  def dropIncompatibilities(selectStatement: String): String = {
    /*
    jsqlparser does not support approximate:
    https://github.com/JSQLParser/JSqlParser/issues/570
     */
    val selectStatementWithoutAppoximate = selectStatement.replaceAll("(?i)[, ][ ]*approximate ", " ")
    try {
      val parsed = CCJSqlParserUtil.parse(selectStatementWithoutAppoximate)
      val handler = new CompatibilityHandler
      parsed.accept(handler)

      parsed.toString
    } catch {
      case e :Exception => selectStatementWithoutAppoximate
    }
  }
}
