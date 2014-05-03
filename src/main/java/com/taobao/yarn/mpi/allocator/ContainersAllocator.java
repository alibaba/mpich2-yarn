/**
 * 
 */
package com.taobao.yarn.mpi.allocator;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * General container allocator for different rules
 */
public interface ContainersAllocator {
  /**
   * Allocate Containers
   * @param numContainer the number of containers to allocate
   * @return list of allocated containers
   * @throws YarnException
   */
  List<Container> allocateContainers(int numContainer) throws YarnException;

  Map<String, Integer> getHostToProcNum();

  int getCurrentRequestId();
}
