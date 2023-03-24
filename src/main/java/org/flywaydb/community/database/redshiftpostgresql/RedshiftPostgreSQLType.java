package org.flywaydb.community.database.redshiftpostgresql;

import org.flywaydb.core.internal.database.base.Type;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;

public class RedshiftPostgreSQLType extends Type<RedshiftPostgreSQLDatabase, RedshiftPostgreSQLSchema> {
    public RedshiftPostgreSQLType(JdbcTemplate jdbcTemplate, RedshiftPostgreSQLDatabase database, RedshiftPostgreSQLSchema schema, String name) {
        super(jdbcTemplate, database, schema, name);
    }

    @Override
    protected void doDrop() throws SQLException {
        jdbcTemplate.execute("DROP TYPE " + database.quote(schema.getName(), name));
    }
}
