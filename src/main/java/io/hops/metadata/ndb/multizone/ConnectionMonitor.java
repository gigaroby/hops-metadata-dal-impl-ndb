package io.hops.metadata.ndb.multizone;

import io.hops.Reconnector;
import io.hops.exception.StorageException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A connection monitor checks whether a connection is up and attempts reconnection if it is not.
 * More specifically, it checks whether the connection is active every interval milliseconds and if not it starts
 * reconnection procedures.
 * The reconnection procedure tries a reconnection and waits for a set amount of time using an exponential backoff
 * strategy.
 * After a set number of exponential backoff sleeps, the wait becomes linear again (using the last value of the sleep).
 */
public class ConnectionMonitor implements Runnable {
  private static final Log LOG = LogFactory.getLog(ConnectionMonitor.class);

  private final Reconnector target;

  // how long to sleep for (milliseconds)
  private final long interval = 1000;
  // exponential backoff for maxExpCounter times and then linear again (with the last sleep from the backoff)
  private final int maxExpCounter = 8;
  // number of times we slept in exp backoff
  private int reconnectionAttempts = 0;

  public ConnectionMonitor(final Reconnector target) {
    this.target = target;
  }

  private boolean attemptReconnect() {
    try {
      target.reconnect();
    } catch (StorageException exc) {
      return false;
    }
    return true;
  }


  @Override
  public void run() {
    while(!Thread.interrupted()) {
      try {
        if(target.isConnected()) {
          // checking whether the target is connected should be cheap
          linearBackoff();
        } else {
          LOG.debug(String.format("monitored connection is disconnected: will backoff (attempt=%d)", this.reconnectionAttempts));
          exponentialBackoff();
          // attempting reconnection may be expensive
          boolean success = attemptReconnect();
          if(success) {
            LOG.debug(String.format("monitored connection is back up (attempt=%d)", this.reconnectionAttempts));
            // reset exponential backoff counter
            reconnectionAttempts = 0;
          }
        }
      } catch (InterruptedException exc) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void linearBackoff() throws InterruptedException {
    this.sleep(this.interval);
  }

  private void exponentialBackoff() throws InterruptedException {
    long nextSleep;
    if(this.reconnectionAttempts < this.maxExpCounter) {
      // compute the next sleep as interval * (2 ^ reconnectionAttempts)
      nextSleep = this.interval * (1 << this.reconnectionAttempts);
    } else {
      // linear, just compute interval * (2 ^ maxExpCounter)
      nextSleep = this.interval * (1 << this.maxExpCounter);
    }
    this.reconnectionAttempts++;

    this.sleep(nextSleep);
  }

  // can be overridden for testing
  protected void sleep(long milliseconds) throws InterruptedException {
    Thread.sleep(milliseconds);
  }
}
