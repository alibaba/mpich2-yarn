package com.taobao.yarn.mpi.allocator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;


import com.taobao.yarn.mpi.MPIConfiguration;
import com.taobao.yarn.mpi.util.Utilities;



/**
 * The container allocates
 */
public class MultiMPIProcContainersAllocator extends ContainersAllocator {
  private static final Log LOG = LogFactory.getLog(MultiMPIProcContainersAllocator.class);
  private final int requestPriority;
  private final int containerMemory;
  private final AtomicInteger rmRequestID = new AtomicInteger();
  private final AtomicInteger numRequestedContainers = new AtomicInteger();
  private final AtomicInteger numAllocatedContainers = new AtomicInteger();
  // How many mpi processes can the job hold?
  private final AtomicInteger numProcessCanRun = new AtomicInteger();
  private final ApplicationAttemptId appAttemptID;
  private final Map<String, Integer> hostToProcNum = new HashMap<String, Integer>();
  private final Map<String, Container> hostToContainer = new HashMap<String, Container>();

  private final MPIConfiguration conf;

  public MultiMPIProcContainersAllocator(AMRMClient rmClient,
      Integer reuqestPriority, Integer containerMemory,
      ApplicationAttemptId appAttemptId) {
    super(rmClient);

    this.rmClient = rmClient;
    this.requestPriority = reuqestPriority;
    this.containerMemory = containerMemory;
    this.appAttemptID = appAttemptId;
    conf=new MPIConfiguration();
  }

  @Override
  public synchronized List<Container> allocateContainers(int numContainer)
      throws YarnException {
    List<Container> result = new ArrayList<Container>();
    // Until we get our fully allocated quota, we keep on polling RM for containers
    // Keep looping until all the containers are launched and shell script executed on them
    // ( regardless of success/failure).
    while (numContainer > numProcessCanRun.get()) {
      LOG.info(String.format("Current requesting state: needed=%d, procVolum=%d, requested=%d, allocated=%d, requestId=%d",
          numContainer, numProcessCanRun.get(), numRequestedContainers.get(), numAllocatedContainers.get(), rmRequestID.get()));
      float progress = (float) numProcessCanRun.get() / numContainer;
      int allocateInterval=conf.getInt(MPIConfiguration.MPI_ALLOCATE_INTERVAL, 1000);
      Utilities.sleep(allocateInterval);
      int askCount = numContainer - numProcessCanRun.get();
      numRequestedContainers.addAndGet(askCount);

      // Setup request for RM and send it
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
        LOG.info(String.format("Got response from RM for container ask, " +
            "allocatedCount=%d", allocatedContainers.size()));
        numAllocatedContainers.addAndGet(allocatedContainers.size());
        numProcessCanRun.addAndGet(allocatedContainers.size());

        // Allocation complete, we will reduce numContainer
        if (numAllocatedContainers.get() >= numContainer) {
          for (Container allocatedContainer : allocatedContainers) {
            LOG.info("AllocatedContainer: Id=" + allocatedContainer.getId()
                + ", NodeId=" + allocatedContainer.getNodeId()
                + ", Host=" + allocatedContainer.getNodeId().getHost());
            String host = allocatedContainer.getNodeId().getHost();
            if (!hostToContainer.containsKey(host)) {
              hostToContainer.put(host, allocatedContainer);
              hostToProcNum.put(host, new Integer(1));
              result.add(allocatedContainer);
            } else {
              Container container = hostToContainer.get(host);
              int procNum = hostToProcNum.get(host).intValue();
              procNum++;
              hostToProcNum.put(host, new Integer(procNum));
              // TODO check if this works
              container.getResource().setMemory(procNum * containerMemory);
              //allocatedContainer.setState(ContainerState.COMPLETE);
            }
          }
        }  // end if
      } catch (IOException e) {
        LOG.error("Failed asking RM for containers.", e);
      }

      }  // end while

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
