package org.apache.hadoop.yarn.mpi.server;

/**
 * MPD Listener operations used by the Application Master
 */
public interface MPDListener {
  /**
   * Add the container to MPD Listener, so the container will keep track of their statuses
   * @param containerId Container Id
   */
  void addContainer(ContainerId containerId);

  /**
   * Check whether all the daemon process is started
   * @return whether started
   */
  boolean isAllMPDStarted();

  /**
   * Check whether all the daemon process is stopped
   * @return whether started
   */
  boolean isAllMPDFinished();

  /**
   * Get the port where the listener listens on
   * @return port number
   */
  int getServerPort();

  /**
   * check whether all the smpd process is healthy
   * @return
   */
  boolean isAllHealthy();
}
