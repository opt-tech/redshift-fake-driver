package jp.ne.opt.redshiftfake.views;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by frankfarrell on 14/06/2018.
 *
 * Some system tables that exist in redshift do not necessarily exist in postgres
 *
 * This creates pg_tableef as a view on each connection if it does not exist
 */
public class SystemViews {

    private static final String pgTableDefExistsQuery = 
            "SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'pg_table_def')";
    private static final String createPgTableDefView =
            "CREATE VIEW pg_table_def (schemaname, tablename, \"column\", \"type\", \"encoding\", distkey, sortkey, \"notnull\") " +
                    "AS SELECT table_schema, table_name, column_name, data_type, 'none', false, 0, CASE is_nullable WHEN 'yes' THEN false ELSE true END " +
                    "FROM information_schema.columns;";

    public static void create(final Connection connection) throws SQLException {

        final ResultSet resultSet = connection.createStatement().executeQuery(pgTableDefExistsQuery);

        if(resultSet.next() && !resultSet.getBoolean(1)){
            connection.createStatement().execute(createPgTableDefView);
        }
    }

}
