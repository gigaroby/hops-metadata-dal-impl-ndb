package io.hops.metadata.ndb.multizone;

import io.hops.MultiZoneStorageConnector;
import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnectorPool;
import io.hops.transaction.TransactionCluster;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Properties;

/**
 * This class is the metadata connector for the primary cluster.
 * As such it returns only connectors to the primary (local) database, regardless of {@link TransactionCluster}.
 */
public class PrimaryConnector implements MultiZoneStorageConnector {
  private static final Log LOG = LogFactory.getLog(PrimaryConnector.class);

  // holds connection to the metadata database cluster
  protected ClusterjConnectorPool local;

  private final Thread monitor;

  /**
   * @param conf configuration parameters for the connectors
   * @throws StorageException
   */
  public PrimaryConnector(final Properties conf) throws StorageException {
    LOG.debug("initializing clusterj connectors");
    LOG.debug("loading configuration for local cluster");
    this.local = new ClusterjConnectorPool(ConnectionTo.LOCAL, conf);
    this.monitor = new Thread(new ConnectionMonitor(this.local));
    this.monitor.start();
  }

  /**
   * @param cluster whether to connect to the local or primary cluster
   * @return a connector to the local database cluster
   * @throws StorageException if the required cluster is disconnected
   */
  @Override
  public StorageConnector connectorFor(TransactionCluster cluster) throws StorageException {
    return local.getConnector();
  }
}
