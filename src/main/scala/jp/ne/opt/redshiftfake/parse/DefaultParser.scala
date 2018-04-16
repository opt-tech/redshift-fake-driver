package jp.ne.opt.redshiftfake.parse

import jp.ne.opt.redshiftfake.parse.compat.CompatibilityHandler
import net.sf.jsqlparser.parser.CCJSqlParserUtil

/**
  * Created by frankfarrell on 16/04/2018.
  */
object DefaultParser extends BaseParser {
  val defaultMatcher = s"(?i)$any*DEFAULT$space".r ~> s"$identifier(\\(\\))|'$identifier'".r <~ s"$any*".r
  val defaultFunctionMatcher = s"(?i)$any*DEFAULT$space".r ~> s"$identifier(\\(\\))".r <~ s"$any*".r

  def matches(sql: String): Boolean = {
    parse(defaultMatcher, sql).successful
  }

  def isFunction(sql: String): Boolean = {
    parse(defaultFunctionMatcher, sql).successful
  }

  def getOperand(sql: String): String = {
     parse(defaultMatcher, sql).get
  }

  def convertFunction(sql: String) : Option[(String, String)]= {

    if(!isFunction(sql)){
      Option.empty
    }
    else{
      val defaultValue = parse(defaultFunctionMatcher, sql).get

      //Parse the function and convert if necessary
      val parsed = CCJSqlParserUtil.parse ("SELECT " + defaultValue)
      parsed.accept (new CompatibilityHandler)
      Option.apply(defaultValue, parsed.toString.replace ("SELECT ", ""))
    }
  }
}
