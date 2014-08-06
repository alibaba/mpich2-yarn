package org.apache.hadoop.yarn.mpi.server;

/**
 * MPI Daemon Process Status
 */
public enum MPDStatus {
  UNDEFINED,
  INITIALIZED,
  MPD_STARTED,
  MPD_CRASH,
  DISCONNECTED,
  ERROR_FINISHED,
  FINISHED
}
