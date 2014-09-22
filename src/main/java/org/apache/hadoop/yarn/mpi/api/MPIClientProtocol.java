package org.apache.hadoop.yarn.mpi.api;

import org.apache.hadoop.ipc.VersionedProtocol;


/**
 * An interface that client connects directly with the Application Master
 */
public interface MPIClientProtocol extends VersionedProtocol{
  /**
   * Version Id
   */
  public static final long versionID = 1L;

  String[] popAllMPIMessages();
}
