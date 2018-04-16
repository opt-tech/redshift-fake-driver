package jp.ne.opt.redshiftfake.parse

import jp.ne.opt.redshiftfake.parse.compat.CompatibilityHandler
import net.sf.jsqlparser.parser.CCJSqlParserUtil

/**
  * Created by frankfarrell on 16/04/2018.
  */
object DefaultParser extends BaseParser {
  val defaultMatcher = s"(?i)$any*DEFAULT$space".r ~> s"$identifier(\\(\\))|'$identifier'".r <~ s"$any*".r

  def isFunction(defaultOperand: String): Boolean = {
    defaultOperand.contains("(")
  }

  def handle(sql: String) : Option[(String, Option[String])]= {

    val defaultValue = parse(defaultMatcher, sql)

    if(defaultValue.successful){
      convert(defaultValue.get)
    }else{
      Option.empty
    }
  }

  def convert(defaultValue: String) : Option[(String, Option[String])] ={
    if(isFunction(defaultValue)){
      //Parse the function and convert if necessary
      val parsed = CCJSqlParserUtil.parse ("SELECT " + defaultValue)
      parsed.accept (new CompatibilityHandler)
      Option.apply(defaultValue, Option.apply(parsed.toString.replace ("SELECT ", "")))
    }
      else{
      Option.apply((defaultValue, Option.empty))
    }
  }
}
