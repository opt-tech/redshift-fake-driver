package org.flywaydb.community.database.redshifth2;

import org.flywaydb.core.internal.database.base.Type;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;

public class RedshiftH2Type extends Type<RedshiftH2Database, RedshiftH2Schema> {
    public RedshiftH2Type(JdbcTemplate jdbcTemplate, RedshiftH2Database database, RedshiftH2Schema schema, String name) {
        super(jdbcTemplate, database, schema, name);
    }

    @Override
    protected void doDrop() throws SQLException {
        jdbcTemplate.execute("DROP TYPE " + database.quote(schema.getName(), name));
    }
}
