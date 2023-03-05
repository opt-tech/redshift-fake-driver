package jp.ne.opt.redshiftfake.parse.compat
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil

trait QueryCompatibility {

  def dropIncompatibilities(statement: String): String = {
    val unsupportedStmtsDrop = List(
      "(?i)[, ][ ]*approximate",
      "(?i)with no schema binding"
    )
    var stmt = statement
    for (s <- unsupportedStmtsDrop) {
      stmt = stmt.replaceAll(s, " ")
    }
    try {
      val parsed = CCJSqlParserUtil.parse(stmt)
      val handler = new CompatibilityHandler
      val _ = parsed.accept(handler)
      parsed.toString
    } catch {
      case e :JSQLParserException => stmt
    }
  }
}
