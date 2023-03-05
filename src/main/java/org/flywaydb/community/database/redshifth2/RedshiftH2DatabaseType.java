package org.flywaydb.community.database.redshifth2;

import org.flywaydb.core.api.ResourceProvider;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.database.base.BaseDatabaseType;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.parser.Parser;
import org.flywaydb.core.internal.parser.ParsingContext;

import java.sql.Connection;
import java.sql.Types;
import java.util.Map;

public class RedshiftH2DatabaseType extends BaseDatabaseType {
    private static final String REDSHIFT_H2_JDBC_DRIVER = "jp.ne.opt.redshiftfake.h2.Driver";

    @Override
    public String getName() {
        return "Redshift-On-H2";
    }

    @Override
    public int getPriority() {
        // Redshift needs to be checked in advance of H2
        return 1;
    }

    @Override
    public int getNullType() {
        return Types.VARCHAR;
    }

    @Override
    public boolean handlesJDBCUrl(String url) {
        return url.startsWith("jdbc:redshifth2:");
    }

    @Override
    public String getDriverClass(String url, ClassLoader classLoader) {
        return REDSHIFT_H2_JDBC_DRIVER;
    }

    @Override
    public String getBackupDriverClass(String url, ClassLoader classLoader) {
        return REDSHIFT_H2_JDBC_DRIVER;
    }

    @Override
    public boolean handlesDatabaseProductNameAndVersion(String databaseProductName, String databaseProductVersion, Connection connection) {
        return true;
    }

    @Override
    public void setOverridingConnectionProps(Map<String, String> props) {
        // Necessary because the Amazon v2 driver does not appear to respect the way Properties.get() handles defaults.
        // If not forced to false, the driver allows resultsets to be read on different threads and will throw if
        // connections are closed before all results are read.
        props.put("enableFetchRingBuffer", "false");
    }

    @Override
    public Database createDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory, StatementInterceptor statementInterceptor) {
        return new RedshiftH2Database(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    @Override
    public Parser createParser(Configuration configuration, ResourceProvider resourceProvider, ParsingContext parsingContext) {
        return new RedshiftH2Parser(configuration, parsingContext);
    }
}
