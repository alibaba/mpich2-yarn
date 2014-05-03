package com.taobao.yarn.mpi.allocator;

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
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import com.taobao.yarn.mpi.util.Utilities;

/**
 * Allocate containers on distinct nodes
 */
public class DistinctContainersAllocator implements ContainersAllocator {
  private static final Log LOG = LogFactory.getLog(DistinctContainersAllocator.class);
  // Handle to communicate with the Resource Manager
  private final ApplicationMasterProtocol resourceManager;
  // Handle to talk to the Resource Manager/Applications Manager
  private final ClientRMProtocol applicationsManager;
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
  // All the nodes in the cluster
  private final List<String> nodes;
  // Number for containers
  private final Map<String, Integer> hostToProcNum = new HashMap<String, Integer>();

  /**
   * Constructor
   * @throws YarnException
   */
  public DistinctContainersAllocator(
      ApplicationMasterProtocol resourceManager,
      ClientRMProtocol applicationsManager,
      int requestPriority,
      int containerMemory,
      ApplicationAttemptId appAttemptId) throws YarnException {
    this.resourceManager = resourceManager;
    this.applicationsManager = applicationsManager;
    this.requestPriority = requestPriority;
    this.containerMemory = containerMemory;
    this.appAttemptID = appAttemptId;
    this.nodes = getClusterNodes();
  }

  @Override
  public synchronized List<Container> allocateContainers(int numContainer) throws YarnException {
    List<Container> result = new ArrayList<Container>();
    List<ContainerId> released = new ArrayList<ContainerId>();

    while (numContainer > numAllocatedContainers.get()) {
      LOG.info(String.format("Current requesting state: needed=%d, requested=%d, allocated=%d, requestId=%d",
          numContainer, numRequestedContainers.get(), numAllocatedContainers.get(), rmRequestID.get()));
      float progress = (float)numAllocatedContainers.get()/numContainer;
      // FIXME Hard coding sleep time
      Utilities.sleep(1000);
      int askCount = numContainer - numAllocatedContainers.get();
      numRequestedContainers.addAndGet(askCount);

      List<ResourceRequest> resourceReq = new ArrayList<ResourceRequest>();
      if (askCount > 0) {
        ResourceRequest containerAsk = Utilities.setupContainerAskForRM(
            askCount, requestPriority, containerMemory);
        resourceReq.add(containerAsk);
      }

      // Send the request to RM
      LOG.info(String.format("Asking RM for %d containers", askCount));
      AMResponse amResp = Utilities.sendContainerAskToRM(
          rmRequestID,
          appAttemptID,
          resourceManager,
          resourceReq,
          new ArrayList<ContainerId>(),
          progress);
      // Retrieve list of allocated containers from the response
      List<Container> allocatedContainers = amResp.getAllocatedContainers();
      LOG.info("Got response from RM for container ask, allocatedCount="
          + allocatedContainers.size());

      boolean firstAllocationSuccess = false;
      for (Container allocatedContainer : allocatedContainers) {
        firstAllocationSuccess = true;
        String host = allocatedContainer.getNodeId().getHost();
        if (!hosts.contains(host)) {
          hosts.add(host);
          LOG.info(String.format("Got a distinct container on %s", host));
          result.add(allocatedContainer);
          numAllocatedContainers.incrementAndGet();
        } else {
          released.add(allocatedContainer.getId());
        }
      }

      // Second phase allocation, release the containers
      if (firstAllocationSuccess) {
        askCount = numContainer - numAllocatedContainers.get();
        List<ResourceRequest> requests = new ArrayList<ResourceRequest>(askCount);
        for (String node : nodes) {
          if (askCount > 0) {
            if (!hosts.contains(node)) {
              ResourceRequest request = setupAContainerAskForRM(node);
              requests.add(request);
              askCount --;
            }
          } else {
            break;
          }
        }
        amResp = Utilities.sendContainerAskToRM(
            rmRequestID,
            appAttemptID,
            resourceManager,
            requests,
            new ArrayList<ContainerId>(),
            progress);
      }  // end if
    }  // end while

    // The "Distinct" of the class name means each host has only one process
    for (Container container : result) {
      hostToProcNum.put(container.getNodeId().getHost(), new Integer(1));
    }
    return result;
  }

  /**
   * Setup a container request on specified node
   * @param node the specified node
   * @return ResourceRequest sent to RM
   */
  private ResourceRequest setupAContainerAskForRM(String node) {
    ResourceRequest request = Records.newRecord(ResourceRequest.class);
    request.setHostName(node);
    request.setNumContainers(1);  // important

    Priority priority = Records.newRecord(Priority.class);
    priority.setPriority(requestPriority);
    request.setPriority(priority);

    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(containerMemory);
    request.setCapability(capability);

    return request;
  }

  /**
   * Get all the nodes in the cluster, this method generate RPC
   * @return host names
   * @throws YarnException
   */
  private List<String> getClusterNodes() throws YarnException {
    List<String> result = new ArrayList<String>();
    GetClusterNodesRequest clusterNodesReq = Records.newRecord(GetClusterNodesRequest.class);
    GetClusterNodesResponse clusterNodesResp = applicationsManager.getClusterNodes(clusterNodesReq);
    List<NodeReport> nodeReports = clusterNodesResp.getNodeReports();
    for (NodeReport nodeReport : nodeReports) {
      result.add(nodeReport.getNodeId().getHost());
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
