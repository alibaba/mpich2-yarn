package org.apache.hadoop.yarn.mpi.allocator;

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
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.mpi.MPIConfiguration;
import org.apache.hadoop.yarn.mpi.util.Utilities;

/**
 * The container allocates
 */
public class MultiMPIProcContainersAllocator extends ContainersAllocator {
  private static final Log LOG = LogFactory
      .getLog(MultiMPIProcContainersAllocator.class);
  private final int requestPriority;
  private final int containerMemory;
  private final AtomicInteger rmRequestID = new AtomicInteger();

  // The number of container has been requested
  private final AtomicInteger numRequestedContainers = new AtomicInteger();

  private final AtomicInteger numAllocatedContainers = new AtomicInteger();
  // How many mpi processes can the job hold?
  private final AtomicInteger numProcessCanRun = new AtomicInteger();
  private final ApplicationAttemptId appAttemptID;
  private final Map<String, Integer> hostToProcNum = new HashMap<String, Integer>();
  private final Map<String, Container> hostToContainer = new HashMap<String, Container>();

  private final MPIConfiguration conf;

  public MultiMPIProcContainersAllocator(AMRMClientAsync.CallbackHandler rmAsyncHandler,
      Integer reuqestPriority, Integer containerMemory,
      ApplicationAttemptId appAttemptId) {
    super(rmAsyncHandler);
    this.requestPriority = reuqestPriority;
    this.containerMemory = containerMemory;
    this.appAttemptID = appAttemptId;
    conf = new MPIConfiguration();
  }

  @Override
  public synchronized List<Container> allocateContainers(int numContainer)
      throws YarnException {
    List<Container> acquiredContainers = new ArrayList<Container>();

    // It's incorrect to add remaining request to the AMRMClient each loop,
    // because it seems that AMRMClient has guarantee that it will retrieve
    // specified resource at last. 2014/08/17
    while (numContainer > numProcessCanRun.get()) {
      LOG.info(String
          .format(
              "Current requesting state: needed=%d, procVolum=%d, requested=%d, allocated=%d, requestId=%d",
              numContainer, numProcessCanRun.get(),
              numRequestedContainers.get(), numAllocatedContainers.get(),
              rmRequestID.get()));
      float progress = (float) numProcessCanRun.get() / numContainer;
      int allocateInterval = conf.getInt(
          MPIConfiguration.MPI_ALLOCATE_INTERVAL, 1000);
      Utilities.sleep(allocateInterval);
      int askCount = numContainer - numProcessCanRun.get();
      numRequestedContainers.addAndGet(askCount);

      try {
        // Retrieve list of allocated containers from the response
        AllocateResponse amResp = rmClient.allocate(progress);
        List<Container> allocatedContainers = amResp.getAllocatedContainers();
        acquiredContainers.addAll(allocatedContainers);
        LOG.info(String.format("Got response from RM for container ask, "
            + "allocatedCount=%d", allocatedContainers.size()));
        numAllocatedContainers.addAndGet(allocatedContainers.size());
        numProcessCanRun.addAndGet(allocatedContainers.size());
      } catch (IOException e) {
        LOG.error("Failed asking RM for containers.", e);
      }
    } // end while

    List<Container> distinctContainers = new ArrayList<Container>();

    // Allocation complete, we will reduce numContainer
    // What MPI application cares is hostToProcNum, how many process does the
    // host should create. Here we will filter the allocated containers to get
    // hostToProcNum
    LOG.info("Successfully acquired " + acquiredContainers.size()
        + " containers.");
    for (Container acquiredContainer : acquiredContainers) {
      LOG.info("AcquiredContainer: Id=" + acquiredContainer.getId()
          + ", NodeId=" + acquiredContainer.getNodeId() + ", Host="
          + acquiredContainer.getNodeId().getHost());
      String host = acquiredContainer.getNodeId().getHost();
      if (!hostToContainer.containsKey(host)) {
        hostToContainer.put(host, acquiredContainer);
        hostToProcNum.put(host, new Integer(1));
        distinctContainers.add(acquiredContainer);
      } else {
        int procNum = hostToProcNum.get(host).intValue();
        procNum++;
        hostToProcNum.put(host, new Integer(procNum));
        // TODO check if this works
        Container container = hostToContainer.get(host);
        container.getResource().setMemory(procNum * containerMemory);
        // allocatedContainer.setState(ContainerState.COMPLETE);
      }
    }

    return distinctContainers;
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
