package jp.ne.opt.redshiftfake.parse

import jp.ne.opt.redshiftfake.parse.compat.CompatibilityHandler
import net.sf.jsqlparser.parser.CCJSqlParserUtil

/**
 * Created by frankfarrell on 16/04/2018.
 *
 * Parse "DEFAULT" constraint in DDL
 */
class DefaultParser extends BaseParser {

  private[this] val defaultMatcher = s"(?i)$any*DEFAULT$space".r ~> s"$identifier(\\(\\))|'$identifier'|$identifier".r <~ s"$any*".r

  def handle(sql: String) : Option[(String, Option[String])] = {
    parse(defaultMatcher, sql) match {
      case Success(result, _) => convert(result)
      case _ => None
    }
  }

  def convert(defaultValue: String) : Option[(String, Option[String])] ={
    if(isFunction(defaultValue)) {
      //Parse the function and convert if necessary
      val parsed = CCJSqlParserUtil.parse ("SELECT " + defaultValue)
      parsed.accept(new CompatibilityHandler)
      Option(defaultValue, Option.apply(parsed.toString.replace ("SELECT ", "")))
    } else {
      Option((defaultValue, Option.empty))
    }
  }

  private[this] def isFunction(defaultOperand: String): Boolean = {
    defaultOperand.contains("(")
  }
}
