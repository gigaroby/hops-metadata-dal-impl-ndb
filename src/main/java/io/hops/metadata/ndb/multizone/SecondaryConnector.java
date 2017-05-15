package io.hops.metadata.ndb.multizone;

import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnectorPool;
import io.hops.transaction.TransactionCluster;

import java.util.Properties;

/**
 * A {@link SecondaryConnector} connects to both the local (secondary) and remote (primary) clusters.
 */
public class SecondaryConnector extends PrimaryConnector {
  // connection to the remote (primary) cluster
  private final ClusterjConnectorPool remote;

  // monitors the remote connection for failures
  private final Thread remoteMonitor;

  public SecondaryConnector(Properties conf) throws StorageException {
    super(conf);
    this.remote = new ClusterjConnectorPool(ConnectionTo.PRIMARY, conf);
    remoteMonitor = new Thread(new ConnectionMonitor(this.remote));
    remoteMonitor.setDaemon(true);
    remoteMonitor.start();
  }

  public boolean isConnectedToPrimary() {
    return this.remote.isConnected();
  }

  @Override
  public StorageConnector connectorFor(TransactionCluster cluster) throws StorageException {
    switch (cluster) {
      case LOCAL:
        return this.local.getConnector();
      case PRIMARY:
        return this.remote.getConnector();
    }

    throw new RuntimeException("impossible");
  }
}
