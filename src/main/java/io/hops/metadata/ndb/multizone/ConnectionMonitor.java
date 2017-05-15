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
  // exponential backoff for maxAttemptsExponential times and then linear again (with the last sleep from the backoff)
  private final int maxAttemptsExponential = 8;
  // number of tried reconnections
  private int attempts = 0;

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
          LOG.debug(String.format("monitored connection is disconnected: will backoff (attempt=%d)", this.attempts));
          exponentialBackoff();
          // attempting reconnection may be expensive
          boolean success = attemptReconnect();
          if(success) {
            LOG.debug(String.format("monitored connection is back up (attempt=%d)", this.attempts));
            // reset exponential backoff counter
            attempts = 0;
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

  /**
   * Sleep for (interval * 2^attempts) until attempts == maxAttemptsExponential.
   * After just sleep for (interval * 2^maxAttemptsExponential)
   * @throws InterruptedException
   */
  private void exponentialBackoff() throws InterruptedException {
    long nextSleep;
    if(this.attempts < this.maxAttemptsExponential) {
      // compute the next sleep as interval * (2 ^ attempts)
      nextSleep = this.interval * (1 << this.attempts);
    } else {
      // linear, just compute interval * (2 ^ maxAttemptsExponential)
      nextSleep = this.interval * (1 << this.maxAttemptsExponential);
    }
    this.attempts++;

    this.sleep(nextSleep);
  }

  // can be overridden for testing
  protected void sleep(long milliseconds) throws InterruptedException {
    Thread.sleep(milliseconds);
  }
}
