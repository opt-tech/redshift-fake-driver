package jp.ne.opt.redshiftfake

import java.sql.Connection

import jp.ne.opt.redshiftfake.s3.S3Service
import jp.ne.opt.redshiftfake.write.Writer
import util.Loan.using

trait Interceptor {
  protected def fetchColumnDefinitions(connection: Connection, command: CopyCommand): Vector[ColumnDefinition] = {
    using(connection.getMetaData.getColumns(null, command.schemaName.orNull, command.tableName, "%")) { rs =>
      Iterator.continually(rs).takeWhile(_.next()).map { rs =>
        val columnName = rs.getString("COLUMN_NAME")
        val columnType = JdbcType.valueOf(rs.getInt("DATA_TYPE"))
        ColumnDefinition(columnName, columnType)
      }.toVector
    }
  }
}

trait CopyInterceptor extends Interceptor {

  def executeCopy(connection: Connection, command: CopyCommand, s3Service: S3Service): Unit = {

    val columnDefinitions = fetchColumnDefinitions(connection, command)
    val filteredColumnDefinitions = getFilteredColumnDefinitions(columnDefinitions, command)
    val placeHolders = filteredColumnDefinitions.map(_ => "?").mkString(",")
    val columnNames = filteredColumnDefinitions.map(column => column.columnName).mkString(",")
    val reader = new read.Reader(command, filteredColumnDefinitions, s3Service)

    reader.read().foreach { case Row(columns) =>
      if (columns.length != filteredColumnDefinitions.length) {
        throw new FakeAmazonSQLException(s"Row $columns has different value count then $filteredColumnDefinitions")
      }
      using(connection.prepareStatement(s"insert into ${command.qualifiedTableName} ($columnNames) values ($placeHolders)")) { stmt =>
        columns.zip(filteredColumnDefinitions).zipWithIndex.foreach { case ((Column(value), ColumnDefinition(_, columnType)), parameterIndex) =>
          value match {
            case Some(s) => ParameterBinder(columnType, command.dateFormatType, command.timeFormatType).bind(s, stmt, parameterIndex + 1)
            case None => {
              if (columnType.stringType) {
                // treat empty string as null when format is JSON of EMPTYASNULL is enabled.
                val jsonFormat = command.copyFormat match {
                  case CopyFormat.Json(_) => true
                  case _ => false
                }

                if (jsonFormat || command.emptyAsNull) {
                  stmt.setObject(parameterIndex + 1, null)
                } else {
                  stmt.setString(parameterIndex + 1, "")
                }
              } else {
                stmt.setObject(parameterIndex + 1, null)
              }
            }
          }
        }

        stmt.executeUpdate()
      }
    }
  }

  /**
    * If CopyCommand has columnList then:
    *   first, checks that all columns from columnList exist in database
    *   second, removes columns from columnDefinitions that are not in CopyCommand.columnList
    * otherwise:
    *   return columnDefinitions without changes
    * @param columnDefinitions column definitions from database
    * @param command copy command
    */
  private def getFilteredColumnDefinitions(columnDefinitions: Vector[ColumnDefinition], command: CopyCommand): Vector[ColumnDefinition] = {
    command.columnList match {
      case Some(commandColumnList) =>
        val dbColumnNamesSet = columnDefinitions.map(col => col.columnName).toSet
        if (!commandColumnList.toSet.subsetOf(dbColumnNamesSet)) {
          throw new FakeAmazonSQLException(s"Passed $commandColumnList for table ${command.qualifiedTableName}, but actual columns are $dbColumnNamesSet")
        }

        val dbColumnMap: Map[String, ColumnDefinition] = columnDefinitions.map {
          column => column.columnName -> column
        }.toMap

        commandColumnList.map {
          columnName => dbColumnMap.get(columnName)
        }.map {
          columnDefinition => columnDefinition.get
        }.toVector

      case None => columnDefinitions
    }
  }
}

trait UnloadInterceptor extends Interceptor {

  def executeUnload(connection: Connection, command: UnloadCommand, s3Service: S3Service): Unit = {

    using(connection.createStatement()) { stmt =>
      using(stmt.executeQuery(command.selectStatement)) { resultSet =>
        val columnCount = resultSet.getMetaData.getColumnCount
        val extractors = (1 to columnCount).map { index =>
          val jdbcType = JdbcType.valueOf(resultSet.getMetaData.getColumnType(index))
          Extractor(jdbcType)
        }
        val columnNames = (1 to columnCount).map { index =>
          resultSet.getMetaData.getColumnName(index)
        }

        val rows = Iterator.continually(resultSet).takeWhile(_.next()).map { rs =>
          val row = extractors.zipWithIndex.map { case (extractor, i) =>
            Column(extractor.extractOpt(rs, i + 1))
          }
          Row(row)
        }

        new Writer(command, s3Service).write(columnNames, rows.toList)
      }
    }
  }
}
