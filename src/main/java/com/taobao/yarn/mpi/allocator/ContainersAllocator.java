/**
 * 
 */
package com.taobao.yarn.mpi.allocator;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * General container allocator for different rules
 */
public abstract class ContainersAllocator {

   private static final Log LOG = LogFactory.getLog(ContainersAllocator.class);

  /**
   * Allocate Containers
   * @param numContainer the number of containers to allocate
   * @return list of allocated containers
   * @throws YarnException
   */
  public static ContainersAllocator newInstanceByName(
      String className, AMRMClient rmClient,
      int requestPriority, int containerMemory,
      ApplicationAttemptId appAttemptID) {
    try {
      Class<?> clazz = Class.forName(className);
      Constructor<?> ctor = clazz.getConstructor(
          AMRMClient.class, Integer.class, Integer.class,
          ApplicationAttemptId.class);
      return (ContainersAllocator) ctor.newInstance(rmClient,
          requestPriority, containerMemory, appAttemptID);
    } catch (Exception e) {
      LOG.error("Error constructing containers allocator in class " + className);
      return null;
    }
  }

  public abstract List<Container> allocateContainers(int numContainer)
      throws YarnException;

  public abstract Map<String, Integer> getHostToProcNum();

  public abstract int getCurrentRequestId();
}
