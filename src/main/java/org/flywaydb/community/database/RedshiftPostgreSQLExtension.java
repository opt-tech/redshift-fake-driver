package org.flywaydb.community.database;

import org.flywaydb.core.extensibility.PluginMetadata;

public class RedshiftPostgreSQLExtension implements PluginMetadata {
  public String getDescription() {
    return "Redshift on PostgreSQL 0.0.1";
  }
}
