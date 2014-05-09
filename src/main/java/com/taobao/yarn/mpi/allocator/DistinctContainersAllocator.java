package com.taobao.yarn.mpi.allocator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import com.taobao.yarn.mpi.util.Utilities;

/**
 * Allocate containers on distinct nodes
 */
public class DistinctContainersAllocator extends ContainersAllocator {
  private static final Log LOG = LogFactory.getLog(DistinctContainersAllocator.class);
  // Allocated container count so that we know how many containers has the RM
  // allocated to us
  private final AtomicInteger numAllocatedContainers = new AtomicInteger();
  // Count of containers already requested from the RM
  // Needed as once requested, we should not request for containers again and again.
  // Only request for more if the original requirement changes.
  private final AtomicInteger numRequestedContainers = new AtomicInteger();
  // Request priority
  private final int requestPriority;
  // Container memory
  private final int containerMemory;
  // Incremental counter for rpc calls to the RM
  private final AtomicInteger rmRequestID = new AtomicInteger();
  // Application Attempt Id ( combination of attemptId and fail count )
  private final ApplicationAttemptId appAttemptID;
  // Hosts for running MPI process
  private final Set<String> hosts = new HashSet<String>();
  // Number for containers
  private final Map<String, Integer> hostToProcNum = new HashMap<String, Integer>();

  /**
   * Constructor
   * @throws YarnException
   */
  public DistinctContainersAllocator(AMRMClient rmClient, int requestPriority,
      int containerMemory, ApplicationAttemptId appAttemptId)
      throws YarnException {
    super(rmClient);

    this.requestPriority = requestPriority;
    this.containerMemory = containerMemory;
    this.appAttemptID = appAttemptId;
  }

  @Override
  public synchronized List<Container> allocateContainers(int numContainer)
      throws YarnException {

    List<Container> result = new ArrayList<Container>();
    List<Container> released = new ArrayList<Container>();

    while (numContainer > numAllocatedContainers.get()) {
      LOG.info(String.format("Current requesting state: needed=%d, requested=%d, allocated=%d, requestId=%d",
          numContainer, numRequestedContainers.get(), numAllocatedContainers.get(), rmRequestID.get()));
      float progress = (float)numAllocatedContainers.get()/numContainer;
      // FIXME Hard coding sleep time
      Utilities.sleep(1000);
      int askCount = numContainer - numAllocatedContainers.get();
      numRequestedContainers.addAndGet(askCount);

      // Setup request for RM and send it
      // TODO: Instead of ApplicationClientProtocol, AMRMClient can no longer
      // gain the view of cluster nodes, so how to do this wisely?
      LOG.info(String.format("Asking RM for %d containers", askCount));
      for (int i = 1; i <= askCount; ++i) {
        ContainerRequest containerAsk = Utilities.setupContainerAskForRM(
            requestPriority, containerMemory);
        rmClient.addContainerRequest(containerAsk);
      }

      try {
        // Retrieve list of allocated containers from the response
        AllocateResponse amResp = rmClient.allocate(progress);
        List<Container> allocatedContainers = amResp.getAllocatedContainers();
        LOG.info("Got response from RM for container ask, allocatedCount="
            + allocatedContainers.size());

        for (Container allocatedContainer : allocatedContainers) {
          String host = allocatedContainer.getNodeId().getHost();
          if (!hosts.contains(host)) {
            hosts.add(host);
            LOG.info(String.format("Got a distinct container on %s", host));
            result.add(allocatedContainer);
            numAllocatedContainers.incrementAndGet();
          } else {
            released.add(allocatedContainer);
          }
        }

        // Release containers
        for (Container container : released) {
          rmClient.releaseAssignedContainer(container.getId());
        }
      } catch (IOException e) {
        LOG.error("Error asking RM for containers", e);
      }
    }  // end while

    // The "Distinct" of the class name means each host has only one process
    for (Container container : result) {
      hostToProcNum.put(container.getNodeId().getHost(), new Integer(1));
    }
    return result;
  }

  @Override
  public Map<String, Integer> getHostToProcNum() {
    return hostToProcNum;
  }

  @Override
  public int getCurrentRequestId() {
    return rmRequestID.get();
  }
}
