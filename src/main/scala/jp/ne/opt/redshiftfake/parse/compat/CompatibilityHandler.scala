package jp.ne.opt.redshiftfake.parse.compat

import Ops._
import net.sf.jsqlparser.expression.operators.arithmetic._
import net.sf.jsqlparser.expression._
import net.sf.jsqlparser.expression.operators.conditional._
import net.sf.jsqlparser.expression.operators.relational._
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.schema.{Table, Column}
import net.sf.jsqlparser.statement.create.index.CreateIndex
import net.sf.jsqlparser.statement.create.table.CreateTable
import net.sf.jsqlparser.statement.create.schema.CreateSchema
import net.sf.jsqlparser.statement.delete.Delete
import net.sf.jsqlparser.statement.drop.Drop
import net.sf.jsqlparser.statement.execute.Execute
import net.sf.jsqlparser.statement.insert.Insert
import net.sf.jsqlparser.statement.replace.Replace
import net.sf.jsqlparser.statement.truncate.Truncate
import net.sf.jsqlparser.statement.update.Update
import net.sf.jsqlparser.statement._
import net.sf.jsqlparser.statement.grant._
import net.sf.jsqlparser.statement.analyze._
import net.sf.jsqlparser.statement.show._
import net.sf.jsqlparser.statement.values._
import net.sf.jsqlparser.statement.alter._
import net.sf.jsqlparser.statement.comment._
import net.sf.jsqlparser.statement.upsert._
import net.sf.jsqlparser.statement.alter.sequence._
import net.sf.jsqlparser.statement.create.view.{CreateView, AlterView}
import net.sf.jsqlparser.statement.create.synonym._
import net.sf.jsqlparser.statement.create.sequence._
import net.sf.jsqlparser.statement.merge.Merge
import net.sf.jsqlparser.statement.select._

import scala.collection.JavaConverters._

class CompatibilityHandler extends SelectVisitor
  with FromItemVisitor
  with ExpressionVisitor
  with ItemsListVisitor
  with SelectItemVisitor
  with StatementVisitor {

  def visit(withItem: WithItem): Unit = withItem.getSubSelect.getSelectBody.accept(this)

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

  def visit(safeCastExpression: SafeCastExpression): Unit = {
  }

  def visit(showIndexStatement: ShowIndexStatement): Unit = {
  }

  def visit(overlapsCondition: OverlapsCondition): Unit = {
  }

  def visit(valuesList: ValuesList): Unit = {
  }

  def visit(lateralSubSelect: LateralSubSelect): Unit = {
    lateralSubSelect.getSubSelect.getSelectBody.accept(this)
  }

  def visit(subjoin: SubJoin): Unit = {
    subjoin.getLeft.accept(this);
    for (join <- subjoin.getJoinList.asScala) {
      join.getRightItem.accept(this);
    }
  }

  def visit(table: Table): Unit = {
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

  def visit(column: Column): Unit = {
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
    inExpression.getLeftExpression.accept(this);
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

    selectExpressionItem.getExpression match {
      case expression: AnalyticExpression =>
        if (expression.getName.equalsIgnoreCase("listagg")){
          val asFunction = new Function()
          val parameters = new ExpressionList()
          if (expression.getExpression() != null) {
            parameters.addExpressions(expression.getExpression())
            if (expression.getOffset() != null) {
              parameters.addExpressions(expression.getOffset())
              if (expression.getDefaultValue() != null) {
                parameters.addExpressions(expression.getDefaultValue())
              }
            }
          }
          asFunction.setName(expression.getName)
          asFunction.setParameters(parameters)
          selectExpressionItem.setExpression(asFunction)
        }
      case expression: Function =>
        expression.getName.toLowerCase match {
          case "median" => {
            //https://docs.aws.amazon.com/redshift/latest/dg/r_MEDIAN.html
            val asPercentileCont = new AnalyticExpression().withType(AnalyticType.WITHIN_GROUP)
            asPercentileCont.setName("percentile_cont")
            asPercentileCont.setExpression(new DoubleValue("0.5").asInstanceOf[Expression])
            val orderBy = new OrderByElement()
            orderBy.setExpression(expression.getParameters.getExpressions.get(0))
            asPercentileCont.setOrderByElements(List(orderBy).asJava)
            selectExpressionItem.setExpression(asPercentileCont)
          }
          case "nvl2" => {
            //https://docs.aws.amazon.com/redshift/latest/dg/r_NVL2.html
            val functionsArguments = expression.getParameters.getExpressions
            val isNullExpression = new IsNullExpression()
              .withNot(true)
              .withLeftExpression(functionsArguments.get(0))
            val whenClause = new WhenClause()
              .withWhenExpression(isNullExpression)
              .withThenExpression(functionsArguments.get(1))
            val asCaseStatement = new CaseExpression()
              .withWhenClauses(List(whenClause).asJava)
              .withElseExpression(functionsArguments.get(2))
            selectExpressionItem.setExpression(asCaseStatement)
          }
          //The simplest way to achieve this in postgres is as follows:
          // `case when extract(epoch from age(a, b)) > 0 then 1 when extract(epoch from age(a, b)) < 0 then -1 else 0 end`
          case "timestamp_cmp" => {
            val functionsArguments = expression.getParameters.getExpressions
            val asCaseStatement = new CaseExpression

            val ageFunction = new Function()
            ageFunction.setName("age")
            val ageFunctionParameters = new ExpressionList()

            ageFunctionParameters.setExpressions(functionsArguments)
            ageFunction.setParameters(ageFunctionParameters)

            val extractSecondsFromAge = new ExtractExpression()
            extractSecondsFromAge.setName("epoch")
            extractSecondsFromAge.setExpression(ageFunction)

            val greaterThan = new GreaterThan()
            greaterThan.setLeftExpression(extractSecondsFromAge)
            greaterThan.setRightExpression(new LongValue(0))

            val greaterThanWhenClause = new WhenClause
            greaterThanWhenClause.setWhenExpression(greaterThan)
            greaterThanWhenClause.setThenExpression(new LongValue(1))

            val lessThan = new MinorThan()
            lessThan.setLeftExpression(extractSecondsFromAge)
            lessThan.setRightExpression(new LongValue(0))

            val lessThanWhenClause = new WhenClause
            lessThanWhenClause.setWhenExpression(lessThan)
            lessThanWhenClause.setThenExpression(new LongValue(-1))

            asCaseStatement.setWhenClauses(List(greaterThanWhenClause, lessThanWhenClause).asJava)
            asCaseStatement.setElseExpression(new LongValue(0))

            selectExpressionItem.setExpression(asCaseStatement)
          }
          case _ => ;
        }

      case _ =>;
    }

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
    val specsToIgnore = List("distkey", "sortkey")
    for (columnDefinition <- createTable.getColumnDefinitions.asScala) {
      if (columnDefinition.getColumnSpecs() != null) {
        val newSpecs = columnDefinition.getColumnSpecs.asScala.filter((spec) => {
          ! specsToIgnore.contains(spec.toLowerCase)
        })
        columnDefinition.setColumnSpecs(newSpecs.asJava)
      }
    }
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
    set.getExpressions().asScala.foreach(_.accept(this))
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

  def visit(geometryDistance: GeometryDistance): Unit = {
  }

  def visit(isDistinctExpression: IsDistinctExpression): Unit = {
  }

  def visit(allValue: AllValue): Unit = {
  }

  def visit(oracleNamedFunctionParam: OracleNamedFunctionParameter): Unit = {
  }

  def visit(connectByRootOperator: ConnectByRootOperator): Unit = {
  }

  def visit(jsonFunction: JsonFunction): Unit = {
  }

  def visit(jsonAggregateFunction: JsonAggregateFunction): Unit = {
  }

  def visit(timezoneExpression: TimezoneExpression): Unit = {
  }

  def visit(xmlSerializeExpr: XMLSerializeExpr): Unit = {
  }

  def visit(variableAssignment: VariableAssignment): Unit = {
  }

  def visit(arrayConstructor: ArrayConstructor): Unit = {
  }

  def visit(arrayExpr: ArrayExpression): Unit = {
  }

  def visit(similiarToExpr: SimilarToExpression): Unit = {
  }

  def visit(collateExpr: CollateExpression): Unit = {
  }

  def visit(nextValExpr: NextValExpression): Unit = {
  }

  def visit(notExpr: NotExpression): Unit = {
  }

  def visit(rowGetExpr: RowGetExpression): Unit = {
  }

  def visit(valueListExpr: ValueListExpression): Unit = {
  }

  def visit(jsonOperator: JsonOperator): Unit = {
  }

  def visit(tryCaseExpr: TryCastExpression): Unit = {
  }

  def visit(isBooleanExpression: IsBooleanExpression): Unit = {
  }

  def visit(fullTextSearch: FullTextSearch): Unit = {
  }

  def visit(xorExpression: XorExpression): Unit = {
  }

  def visit(integerDivision: IntegerDivision): Unit = {
  }

  def visit(bitwiseLeftShift: BitwiseLeftShift): Unit = {
  }

  def visit(bitwiseRightShift: BitwiseRightShift): Unit = {
  }

  def visit(parenthesisFromItem: ParenthesisFromItem): Unit = {
  }

  def visit(namedExpressionList: NamedExpressionList): Unit = {
  }

  def visit(unsupportedStatement: UnsupportedStatement): Unit = {
  }

  def visit(alterSystemStatement: AlterSystemStatement): Unit = {
  }

  def visit(purgeStatement: PurgeStatement): Unit = {
  }

  def visit(renameTableStatement: RenameTableStatement): Unit = {
  }

  def visit(ifElseStatement: IfElseStatement): Unit = {
  }

  def visit(alterSession: AlterSession): Unit = {
  }

  def visit(createSynonym: CreateSynonym): Unit = {
  }

  def visit(createFunctionalStatement: CreateFunctionalStatement): Unit = {
  }

  def visit(alterSequence: AlterSequence): Unit = {
  }

  def visit(createSequence: CreateSequence): Unit = {
  }

  def visit(grant: Grant): Unit = {
  }

  def visit(declareStatement: DeclareStatement): Unit = {
  }

  def visit(showStatement: ShowStatement): Unit = {
  }

  def visit(explainStatement: ExplainStatement): Unit = {
  }

  def visit(describeStatement: DescribeStatement): Unit = {
  }

  def visit(valuesStatement: ValuesStatement): Unit = {
  }

  def visit(block: Block): Unit = {
  }

  def visit(useStatement: UseStatement): Unit = {
  }

  def visit(upsert: Upsert): Unit = {
  }

  def visit(showTablesStatement: ShowTablesStatement): Unit = {
  }

  def visit(showColumnsStatement: ShowColumnsStatement): Unit = {
  }

  def visit(resetStatement: ResetStatement): Unit = {
  }

  def visit(createSchema: CreateSchema): Unit = {
  }

  def visit(commit: Commit): Unit = {
  }

  def visit(comment: Comment): Unit = {
  }

  def visit(rollbackStatement: RollbackStatement): Unit = {
  }

  def visit(savepointStatement: SavepointStatement): Unit = {
  }

  def visit(analyze: Analyze): Unit = {
  }

  private[this] def visitBinaryExpression(expr: BinaryExpression): Unit = {
    expr.getLeftExpression.accept(this)
    expr.getRightExpression.accept(this)
  }
}
