package jp.ne.opt.redshiftfake.parse.compat

import Ops._
import net.sf.jsqlparser.expression.operators.arithmetic._
import net.sf.jsqlparser.expression._
import net.sf.jsqlparser.expression.operators.conditional.{AndExpression, OrExpression}
import net.sf.jsqlparser.expression.operators.relational._
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.schema.{Table, Column}
import net.sf.jsqlparser.statement.create.index.CreateIndex
import net.sf.jsqlparser.statement.create.table.CreateTable
import net.sf.jsqlparser.statement.delete.Delete
import net.sf.jsqlparser.statement.drop.Drop
import net.sf.jsqlparser.statement.execute.Execute
import net.sf.jsqlparser.statement.insert.Insert
import net.sf.jsqlparser.statement.replace.Replace
import net.sf.jsqlparser.statement.truncate.Truncate
import net.sf.jsqlparser.statement.update.Update
import net.sf.jsqlparser.statement.{Statements, SetStatement, StatementVisitor}
import net.sf.jsqlparser.statement.alter.Alter
import net.sf.jsqlparser.statement.create.view.{CreateView, AlterView}
import net.sf.jsqlparser.statement.merge.Merge
import net.sf.jsqlparser.statement.select._

import scala.collection.JavaConverters._

class CompatibilityHandler extends SelectVisitor
  with FromItemVisitor
  with ExpressionVisitor
  with ItemsListVisitor
  with SelectItemVisitor
  with StatementVisitor {

  def visit(withItem: WithItem): Unit = withItem.getSelectBody.accept(this)

  def visit(setOpList: SetOperationList): Unit = {
    setOpList.getSelects.asScala.foreach(_.accept(this))
  }

  def visit(plainSelect: PlainSelect): Unit = {
    Option(plainSelect.getSelectItems).foreach { items =>
      items.asScala.foreach(_.accept(this))
    }

    Option(plainSelect.getFromItem).foreach(_.accept(this))

    Option(plainSelect.getJoins).foreach { joins =>
      joins.asScala.foreach(_.getRightItem.accept(this))
    }

    Option(plainSelect.getWhere).foreach(_.accept(this))

    Option(plainSelect.getOracleHierarchical).foreach(_.accept(this))
  }

  def visit(tableFunction: TableFunction): Unit = {
  }

  def visit(valuesList: ValuesList): Unit = {
  }

  def visit(lateralSubSelect: LateralSubSelect): Unit = {
    lateralSubSelect.getSubSelect.getSelectBody.accept(this)
  }

  def visit(subjoin: SubJoin): Unit = {
    subjoin.getLeft.accept(this)
    subjoin.getJoin.getRightItem.accept(this)
  }

  def visit(tableName: Table): Unit = {
  }

  def visit(literal: DateTimeLiteralExpression): Unit = {
  }

  def visit(timeKeyExpression: TimeKeyExpression): Unit = {
  }

  def visit(hint: OracleHint): Unit = {
  }

  def visit(doubleValue: DoubleValue): Unit = {
  }

  def visit(longValue: LongValue): Unit = {
  }

  def visit(hexValue: HexValue): Unit = {
  }

  def visit(dateValue: DateValue): Unit = {
  }

  def visit(timeValue: TimeValue): Unit = {
  }

  def visit(timestampValue: TimestampValue): Unit = {
  }

  def visit(stringValue: StringValue): Unit = {
  }

  def visit(parenthesis: Parenthesis): Unit = {
    parenthesis.getExpression.accept(this)
  }

  def visit(likeExpression: LikeExpression): Unit = {
    visitBinaryExpression(likeExpression)
  }

  def visit(minorThan: MinorThan): Unit = {
    visitBinaryExpression(minorThan)
  }

  def visit(minorThanEquals: MinorThanEquals): Unit = {
    visitBinaryExpression(minorThanEquals)
  }

  def visit(notEqualsTo: NotEqualsTo): Unit = {
    visitBinaryExpression(notEqualsTo)
  }

  def visit(tableColumn: Column): Unit = {
  }

  def visit(castExpression: CastExpression): Unit = {
    castExpression.getLeftExpression.accept(this)
  }

  def visit(caseExpression: CaseExpression): Unit = {
    Option(caseExpression.getElseExpression).foreach(_.accept(this))

    Option(caseExpression.getSwitchExpression).foreach(_.accept(this))

    Option(caseExpression.getWhenClauses).foreach { clauses =>
      clauses.asScala.foreach(_.accept(this))
    }
  }

  def visit(wgexpr: WithinGroupExpression): Unit = {
  }

  def visit(eexpr: ExtractExpression): Unit = {
  }

  def visit(iexpr: IntervalExpression): Unit = {
  }

  def visit(oexpr: OracleHierarchicalExpression): Unit = {
    Option(oexpr.getStartExpression).foreach(_.accept(this))
    Option(oexpr.getConnectExpression).foreach(_.accept(this))
  }

  def visit(rexpr: RegExpMatchOperator): Unit = {
    visitBinaryExpression(rexpr)
  }

  def visit(aexpr: AnalyticExpression): Unit = {
  }

  def visit(modulo: Modulo): Unit = {
    visitBinaryExpression(modulo)
  }

  def visit(rowConstructor: RowConstructor): Unit = {
    rowConstructor.getExprList.getExpressions.asScala.foreach(_.accept(this))
  }

  def visit(groupConcat: MySQLGroupConcat): Unit = {
  }

  def visit(aexpr: KeepExpression): Unit = {
  }

  def visit(bind: NumericBind): Unit = {
  }

  def visit(`var`: UserVariable): Unit = {
  }

  def visit(regExpMySQLOperator: RegExpMySQLOperator): Unit = {
    visitBinaryExpression(regExpMySQLOperator)
  }

  def visit(jsonExpr: JsonExpression): Unit = {
  }

  def visit(isNullExpression: IsNullExpression): Unit = {
  }

  def visit(inExpression: InExpression): Unit = {
    Option(inExpression.getLeftExpression).fold {
      Option(inExpression.getLeftItemsList).foreach(_.accept(this))
    } { _.accept(this) }

    inExpression.getRightItemsList.accept(this)
  }

  def visit(greaterThanEquals: GreaterThanEquals): Unit = {
    visitBinaryExpression(greaterThanEquals)
  }

  def visit(greaterThan: GreaterThan): Unit = {
    visitBinaryExpression(greaterThan)
  }

  def visit(jdbcNamedParameter: JdbcNamedParameter): Unit = {
  }

  def visit(jdbcParameter: JdbcParameter): Unit = {
  }

  def visit(signedExpression: SignedExpression): Unit = {
    signedExpression.getExpression.accept(this)
  }

  def visit(function: Function): Unit = {
    Option(function.getParameters).foreach(visit)
  }

  def visit(nullValue: NullValue): Unit = {
  }

  def visit(addition: Addition): Unit = {
    visitBinaryExpression(addition)
  }

  def visit(division: Division): Unit = {
    visitBinaryExpression(division)
  }

  def visit(multiplication: Multiplication): Unit = {
    visitBinaryExpression(multiplication)
  }

  def visit(subtraction: Subtraction): Unit = {
    visitBinaryExpression(subtraction)
  }

  def visit(andExpression: AndExpression): Unit = {
    visitBinaryExpression(andExpression)
  }

  def visit(orExpression: OrExpression): Unit = {
    visitBinaryExpression(orExpression)
  }

  def visit(between: Between): Unit = {
    between.getLeftExpression.accept(this)
    between.getBetweenExpressionStart.accept(this)
    between.getBetweenExpressionEnd.accept(this)
  }

  def visit(equalsTo: EqualsTo): Unit = {
    visitBinaryExpression(equalsTo)
  }

  def visit(whenClause: WhenClause): Unit = {
    Option(whenClause.getThenExpression).foreach(_.accept(this))

    Option(whenClause.getWhenExpression).foreach(_.accept(this))
  }

  def visit(existsExpression: ExistsExpression): Unit = {
    existsExpression.getRightExpression.accept(this)
  }

  def visit(allComparisonExpression: AllComparisonExpression): Unit = {
    allComparisonExpression.getSubSelect.getSelectBody.accept(this)
  }

  def visit(anyComparisonExpression: AnyComparisonExpression): Unit = {
    anyComparisonExpression.getSubSelect.getSelectBody.accept(this)
  }

  def visit(concat: Concat): Unit = {
    visitBinaryExpression(concat)
  }

  def visit(matches: Matches): Unit = {
    visitBinaryExpression(matches)
  }

  def visit(bitwiseAnd: BitwiseAnd): Unit = {
    visitBinaryExpression(bitwiseAnd)
  }

  def visit(bitwiseOr: BitwiseOr): Unit = {
    visitBinaryExpression(bitwiseOr)
  }

  def visit(bitwiseXor: BitwiseXor): Unit = {
    visitBinaryExpression(bitwiseXor)
  }

  def visit(multiExprList: MultiExpressionList): Unit = {
    multiExprList.getExprList.asScala.foreach(_.accept(this))
  }

  def visit(expressionList: ExpressionList): Unit = {
    expressionList.getExpressions.asScala.foreach(_.accept(this))
  }

  def visit(subSelect: SubSelect): Unit = {
    Option(subSelect.getWithItemsList).foreach { itemList =>
      itemList.asScala.foreach(_.accept(this))
    }
    subSelect.getSelectBody.accept(this)
  }

  def visit(selectExpressionItem: SelectExpressionItem): Unit = {
    selectExpressionItem.getExpression.accept(this)
  }

  def visit(allTableColumns: AllTableColumns): Unit = {
  }

  def visit(allColumns: AllColumns): Unit = {
  }

  def visit(stmts: Statements): Unit = {
    stmts.getStatements.asScala.foreach(_.accept(this))
  }

  def visit(createView: CreateView): Unit = {
  }

  def visit(createTable: CreateTable): Unit = {
    Option(createTable.getSelect).foreach(_.accept(this))
  }

  def visit(createIndex: CreateIndex): Unit = {
  }

  def visit(truncate: Truncate): Unit = {
  }

  def visit(drop: Drop): Unit = {
  }

  def visit(replace: Replace): Unit = {
    Option(replace.getExpressions).foreach { expressions =>
      expressions.asScala.foreach(_.accept(this))
    }
    Option(replace.getItemsList).foreach(_.accept(this))
  }

  def visit(insert: Insert): Unit = {
    Option(insert.getItemsList).foreach(_.accept(this))
    Option(insert.getSelect).foreach(_.accept(this))
  }

  def visit(update: Update): Unit = {
    Option(update.getExpressions).foreach { expressions =>
      expressions.asScala.foreach(_.accept(this))
    }

    Option(update.getFromItem).foreach(_.accept(this))

    Option(update.getJoins).foreach { joins =>
      joins.asScala.foreach(_.getRightItem.accept(this))
    }

    Option(update.getWhere).foreach(_.accept(this))
  }

  def visit(delete: Delete): Unit = {
    Option(delete.getWhere).foreach(_.accept(this))
  }

  def visit(select: Select): Unit = {
    Option(select.getWithItemsList).foreach { itemList =>
      itemList.asScala.foreach(_.accept(this))
    }

    select.getSelectBody match {
      case plainSelect: PlainSelect =>
        val joins = Option(plainSelect.getJoins).map(_.asScala).getOrElse(Nil)

        def mkSelect: PlainSelect = {
          val select = new PlainSelect
          select.setSelectItems(plainSelect.getSelectItems)
          select.setFromItem(plainSelect.getFromItem)
          select
        }

        //================================================
        // Redshift supports FULL JOINs with non-merge-joinable or non-hash-joinable join conditions but postgresql does not.
        // Following code try to get rid of FULL JOINs(upto three tables) from statement by replacing FULL JOINs with union of LEFT/RIGHT JOINs.
        //================================================
        joins match {
          case join +: Nil if join.isFull =>

            val expr1 = mkSelect
              .withJoins(mkLeftJoin
                .withRightItem(join.getRightItem)
                .on(join.getOnExpression))

            val expr2 = mkSelect
              .withJoins(mkRightJoin
                .withRightItem(join.getRightItem)
                .on(join.getOnExpression))

            val statement = CCJSqlParserUtil.parse(s"$expr1 UNION $expr2")
            select.setSelectBody(statement.asInstanceOf[Select].getSelectBody)

          case join1 +: join2 +: Nil if join1.isFull && join2.isLeft =>

            val expr1 = mkSelect
              .withJoins(
                mkLeftJoin
                  .withRightItem(join1.getRightItem)
                  .on(join1.getOnExpression),
                mkLeftJoin
                  .withRightItem(join2.getRightItem)
                  .on(join2.getOnExpression))

            val expr2 = mkSelect
              .withJoins(
                mkRightJoin
                  .withRightItem(join1.getRightItem)
                  .on(join1.getOnExpression),
                mkLeftJoin
                  .withRightItem(join2.getRightItem)
                  .on(join2.getOnExpression))

            val statement = CCJSqlParserUtil.parse(s"$expr1 UNION $expr2")
            select.setSelectBody(statement.asInstanceOf[Select].getSelectBody)

          case join1 +: join2 +: Nil if join1.isFull && join2.isRight =>

            val expr1 = mkSelect
              .withJoins(
                mkLeftJoin
                  .withRightItem(join1.getRightItem)
                  .on(join1.getOnExpression),
                mkLeftJoin
                  .withRightItem(join2.getRightItem)
                  .on(join2.getOnExpression))

            val expr2 = mkSelect
              .withJoins(
                mkRightJoin
                  .withRightItem(join1.getRightItem)
                  .on(join1.getOnExpression),
                mkRightJoin
                  .withRightItem(join2.getRightItem)
                  .on(join2.getOnExpression))

            val statement = CCJSqlParserUtil.parse(s"$expr1 UNION $expr2")
            select.setSelectBody(statement.asInstanceOf[Select].getSelectBody)

          case join1 +: join2 +: Nil if join1.isLeft && join2.isFull =>

            val expr1 = mkSelect
              .withJoins(
                mkLeftJoin
                  .withRightItem(join1.getRightItem)
                  .on(join1.getOnExpression),
                mkRightJoin
                  .withRightItem(join2.getRightItem)
                  .on(join2.getOnExpression))

            val expr2 = mkSelect
              .withJoins(
                mkLeftJoin
                  .withRightItem(join1.getRightItem)
                  .on(join1.getOnExpression),
                mkLeftJoin
                  .withRightItem(join2.getRightItem)
                  .on(join2.getOnExpression))

            val statement = CCJSqlParserUtil.parse(s"$expr1 UNION $expr2")
            select.setSelectBody(statement.asInstanceOf[Select].getSelectBody)

          case join1 +: join2 +: Nil if join1.isRight && join2.isFull =>

            val expr1 = mkSelect
              .withJoins(
                mkRightJoin
                  .withRightItem(join1.getRightItem)
                  .on(join1.getOnExpression),
                mkLeftJoin
                  .withRightItem(join2.getRightItem)
                  .on(join2.getOnExpression))

            val expr2 = mkSelect
              .withJoins(
                mkRightJoin
                  .withRightItem(join1.getRightItem)
                  .on(join1.getOnExpression),
                mkRightJoin
                  .withRightItem(join2.getRightItem)
                  .on(join2.getOnExpression))

            val statement = CCJSqlParserUtil.parse(s"$expr1 UNION $expr2")
            select.setSelectBody(statement.asInstanceOf[Select].getSelectBody)

          case join1 +: join2 +: Nil if join1.isFull && join2.isFull =>

            val expr1 = mkSelect
              .withJoins(
                mkLeftJoin
                  .withRightItem(join1.getRightItem)
                  .on(join1.getOnExpression),
                mkLeftJoin
                  .withRightItem(join2.getRightItem)
                  .on(join2.getOnExpression))

            val expr2 = mkSelect
              .withJoins(
                mkRightJoin
                  .withRightItem(join1.getRightItem)
                  .on(join1.getOnExpression),
                mkLeftJoin
                  .withRightItem(join2.getRightItem)
                  .on(join2.getOnExpression))

            val expr3 = mkSelect
              .withJoins(
                mkRightJoin
                  .withRightItem(join1.getRightItem)
                  .on(join1.getOnExpression),
                mkRightJoin
                  .withRightItem(join2.getRightItem)
                  .on(join2.getOnExpression))

            val statement = CCJSqlParserUtil.parse(s"$expr1 UNION $expr2 UNION $expr3")
            select.setSelectBody(statement.asInstanceOf[Select].getSelectBody)

          case _ => // do nothing
        }
      case _ =>
    }
    select.getSelectBody.accept(this)
  }

  def visit(execute: Execute): Unit = {
  }

  def visit(set: SetStatement): Unit = {
    set.getExpression.accept(this)
  }

  def visit(merge: Merge): Unit = {
    Option(merge.getUsingTable).fold {
      Option(merge.getUsingSelect).foreach(_.accept(this.asInstanceOf[FromItemVisitor]))
    } { _.accept(this) }
  }

  def visit(alterView: AlterView): Unit = {
  }

  def visit(alter: Alter): Unit = {
  }

  private[this] def visitBinaryExpression(expr: BinaryExpression): Unit = {
    expr.getLeftExpression.accept(this)
    expr.getRightExpression.accept(this)
  }
}
