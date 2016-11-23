package jp.ne.opt.redshiftfake.parse.compat

import net.sf.jsqlparser.expression.Expression
import net.sf.jsqlparser.statement.select.{PlainSelect, FromItem, Join}

import scala.collection.JavaConverters._

object Ops {

  implicit class RichJoin(val self: Join) extends AnyVal {
    def on(expression: Expression): Join = {
      self.setOnExpression(expression)
      self
    }

    def withRightItem(fromItem: FromItem): Join = {
      self.setRightItem(fromItem)
      self
    }
  }

  implicit class RichPlainSelect(val self: PlainSelect) extends AnyVal {
    def withJoins(joins: Join*): PlainSelect = {
      self.setJoins(joins.asJava)
      self
    }
  }

  def mkLeftJoin: Join = {
    val join = new Join
    join.setLeft(true)
    join
  }

  def mkRightJoin: Join = {
    val join = new Join
    join.setRight(true)
    join
  }
}
