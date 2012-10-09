/**
 * 
 */
package com.taobao.yarn.mpi.allocator;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;

/**
 * General container allocator for different rules
 */
public interface ContainersAllocator {
  /**
   * Allocate Containers
   * @param numContainer
   * @return list of allocated containers
   * @throws YarnRemoteException
   */
  List<Container> allocateContainers(int numContainer) throws YarnRemoteException;

  Map<String, Integer> getHostToProcNum();
}
