package jp.ne.opt.redshiftfake.parse.compat

import net.sf.jsqlparser.parser.CCJSqlParserUtil

trait Compatible {

  def dropIncompatibilities(selectStatement: String): String = {
    val parsed = CCJSqlParserUtil.parse(selectStatement)
    val handler = new CompatibilityHandler
    parsed.accept(handler)

    parsed.toString
  }
}
