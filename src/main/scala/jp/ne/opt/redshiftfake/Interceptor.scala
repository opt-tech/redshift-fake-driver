package jp.ne.opt.redshiftfake

import java.sql.Connection

import jp.ne.opt.redshiftfake.s3.S3Service
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
    val placeHolders = columnDefinitions.map(_ => "?").mkString(",")
    val reader = new read.Reader(command, columnDefinitions, s3Service)

    reader.read().foreach { case Row(columns) =>
      using(connection.prepareStatement(s"insert into ${command.qualifiedTableName} values ($placeHolders)")) { stmt =>
        columns.zip(columnDefinitions).zipWithIndex.foreach { case ((Column(value), ColumnDefinition(_, columnType)), parameterIndex) =>
          value match {
            case Some(s) => ParameterBinder(columnType).bind(s, stmt, parameterIndex + 1)
            case _ => stmt.setObject(parameterIndex + 1, null)
          }
        }

        stmt.executeUpdate()
      }
    }
  }
}
