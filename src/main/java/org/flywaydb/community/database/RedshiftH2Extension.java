package org.flywaydb.community.database;

import org.flywaydb.core.extensibility.PluginMetadata;

public class RedshiftH2Extension implements PluginMetadata {
  public String getDescription() {
    return "Redshift on H2 0.0.1";
  } 
}
