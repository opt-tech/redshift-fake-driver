package org.flywaydb.community.database.redshifth2;

import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;

public class RedshiftH2Table extends Table<RedshiftH2Database, RedshiftH2Schema> {
    RedshiftH2Table(JdbcTemplate jdbcTemplate, RedshiftH2Database database, RedshiftH2Schema schema, String name) {
        super(jdbcTemplate, database, schema, name);
    }

    @Override
    protected void doDrop() throws SQLException {
        jdbcTemplate.execute("DROP TABLE " + database.quote(schema.getName(), name) + " CASCADE");
    }

    @Override
    protected boolean doExists() throws SQLException {
        return jdbcTemplate.queryForBoolean("SELECT EXISTS (\n" +
                                                    "  SELECT 1\n" +
                                                    "  FROM   pg_catalog.pg_class c\n" +
                                                    "  JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n" +
                                                    "  WHERE  n.nspname = ?\n" +
                                                    "  AND    c.relname = ?\n" +
                                                    "  AND    c.relkind = 'r'\n" + // only tables
                                                    ")", schema.getName(),
                                            name.toLowerCase() // Redshift table names are case-insensitive and always in lowercase in pg_class.
                                           );
    }

    @Override
    protected void doLock() throws SQLException {
        jdbcTemplate.execute("DELETE FROM " + this + " WHERE FALSE");
    }
}
