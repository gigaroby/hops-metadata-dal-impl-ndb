package io.hops.metadata.ndb.multizone;

/**
 * This class provides indication on which database to connect to.
 * It also contains the configuration prefix for database settings.
 */
public enum ConnectionTo {
  LOCAL("io.hops.metadata.local"),
  PRIMARY("io.hops.metadata.primary");

  private final String configPrefix;

  ConnectionTo(final String configPrefix) {
    this.configPrefix = configPrefix;
  }

  public String getConfigPrefix() {
    return configPrefix;
  }
}
