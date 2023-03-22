package jp.ne.opt.redshiftfake.parse.compat
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil

trait QueryCompatibility {

  def dropIncompatibilities(statement: String): String = {
    val stmtsReplacements = Map[String, String](
      """(?i)(.*)[,\r\n\s]*approximate([\s\S]*)""" -> """$1$2""",
      """(?i)[\r\n\s]*create[\s\r\n]+(or[\n\r\s]+replace[\n\r\s]+)?view[\n\r\s]+(\w+)[\n\r\s]+([\s\S]*)[\n\r\s]+with[\n\r\s]+no[\n\r\s]+schema[\n\r\s]+binding[\n\r\s]*""" -> """
      | create or replace procedure drop_table_or_view(inout name text)
      | as \$\$
      | declare
      |   entity_type text;
      | begin
      |   select case lower(table_type) when 'view' then 'view' else 'table' end from information_schema.tables where lower(table_name) = lower(name) into entity_type;
      |   if entity_type != '' then
      |     execute format('drop %s if exists %s', entity_type, name);
      |   end if;
      | end; 
      | \$\$ 
      | LANGUAGE plpgsql; 
      | call drop_table_or_view('$2'); 
      | create view $2 $3""".stripMargin,
    )
    var stmt = statement
    for ((k, v) <- stmtsReplacements) {
      stmt = stmt.replaceAll(k, v)
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
