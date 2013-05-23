/**
 * 
 */
package com.taobao.yarn.mpi.server;

/**
 * MPD Listener operations used by the Application Master
 */
public interface MPDListener {
  /**
   * Add the container to MPD Listener, so the container will keep track of their statuses
   * @param containerId Container Id
   */
  void addContainer(int containerId);

  /**
   * Check whether all the smpd process is started
   * @return whether started
   */
  boolean isAllMPDStarted();

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
