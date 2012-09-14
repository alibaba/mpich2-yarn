package com.taobao.yarn.mpi.allocator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
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
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.Records;

import com.taobao.yarn.mpi.server.ApplicationMaster;

/**
 * Allocate containers on distinct nodes
 */
public class DistinctContainersAllocator implements ContainersAllocator {
  private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);
  // Handle to communicate with the Resource Manager
  private final AMRMProtocol resourceManager;
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
  private ApplicationAttemptId appAttemptID;
  // Hosts for running MPI process
  private final Set<String> hosts = new HashSet<String>();
  // All the nodes in the cluster
  private final List<String> nodes;

  /**
   * Constructor
   * @throws YarnRemoteException
   */
  public DistinctContainersAllocator(AMRMProtocol resourceManager,
      ClientRMProtocol applicationsManager,
      int requestPriority, int containerMemory) throws YarnRemoteException {
    this.resourceManager = resourceManager;
    this.requestPriority = requestPriority;
    this.containerMemory = containerMemory;
    this.applicationsManager = applicationsManager;
    this.nodes = getClusterNodes();
  }

  @Override
  public synchronized List<Container> allocateContainers(int numContainer) throws YarnRemoteException {
    List<Container> result = new ArrayList<Container>();
    List<ContainerId> released = new ArrayList<ContainerId>();

    while (numContainer > numAllocatedContainers.get()) {
      LOG.info(String.format("Current requesting state: needed=%d, requested=%d, allocated=%d, requestId=%d",
          numContainer, numRequestedContainers, numAllocatedContainers, rmRequestID));
      float progress = (float)numAllocatedContainers.get()/numContainer;
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.warn("Sleep interrupted " + e.getMessage());
      }
      int askCount = numContainer - numAllocatedContainers.get();
      numRequestedContainers.addAndGet(askCount);

      List<ResourceRequest> resourceReq = new ArrayList<ResourceRequest>();
      if (askCount > 0) {
        ResourceRequest containerAsk = setupContainerAskForRM(askCount);
        resourceReq.add(containerAsk);
      }

      // Send the request to RM
      LOG.info("Asking RM for containers, askCount=" + askCount);
      AMResponse amResp = sendContainerAskToRM(resourceReq, new ArrayList<ContainerId>(), progress);
      // Retrieve list of allocated containers from the response
      List<Container> allocatedContainers = amResp.getAllocatedContainers();
      LOG.info("Got response from RM for container ask, allocatedCount="
          + allocatedContainers.size());

      boolean allocateSuccess = false;
      for (Container allocatedContainer : allocatedContainers) {
        allocateSuccess = true;
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

      // Second phase allocation
      if (allocateSuccess) {
        // FIXME Consider the cluster load and container count more than nodes
        // Consider if containers more than nodes, fail. Because in smpd, if host is specified,
        // no processor numbers are allowed.
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
        amResp = sendContainerAskToRM(requests, new ArrayList<ContainerId>(), progress);

      }
    }
    return result;
  }

  /**
   * Setup the request that will be sent to the RM for the container ask.
   * @param numContainers Containers to ask for from RM
   * @return the setup ResourceRequest to be sent to RM
   */
  private ResourceRequest setupContainerAskForRM(int numContainers) {
    ResourceRequest request = Records.newRecord(ResourceRequest.class);
    // setup requirements for hosts, whether a particular rack/host is needed
    // Refer to apis under org.apache.hadoop.net for more details on how to get figure out
    // rack/host mapping. using * as any host will do for the distributed shell app
    request.setHostName("*");
    // set no. of containers needed
    request.setNumContainers(numContainers);

    // set the priority for the request
    Priority pri = Records.newRecord(Priority.class);
    // TODO - what is the range for priority? how to decide?
    pri.setPriority(requestPriority);
    request.setPriority(pri);

    // Set up resource type requirements
    // For now, only memory is supported so we set memory requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(containerMemory);
    request.setCapability(capability);

    return request;
  }

  /**
   * Setup a container request on specified node
   * @param node
   * @return
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
   * Ask RM to allocate given no. of containers to this Application Master
   * @param requestedContainers Containers to ask for from RM
   * @return Response from RM to AM with allocated containers
   * @throws YarnRemoteException
   */
  private AMResponse sendContainerAskToRM(
      List<ResourceRequest> requestedContainers,
      List<ContainerId> releasedContainers,
      float progress) throws YarnRemoteException {
    AllocateRequest req = Records.newRecord(AllocateRequest.class);
    req.setResponseId(rmRequestID.incrementAndGet());
    req.setApplicationAttemptId(appAttemptID);
    req.addAllAsks(requestedContainers);
    req.addAllReleases(releasedContainers);
    req.setProgress(progress);

    LOG.info("Sending request to RM for containers"
        + ", requestedSet=" + requestedContainers.size()
        + ", releasedSet=" + releasedContainers.size()
        + ", progress=" + req.getProgress());

    for (ResourceRequest  rsrcReq : requestedContainers) {
      LOG.info("Requested container ask: " + rsrcReq.toString());
    }
    for (ContainerId id : releasedContainers) {
      LOG.info("Released container, id=" + id.getId());
    }

    AllocateResponse resp = resourceManager.allocate(req);
    return resp.getAMResponse();
  }

  /**
   * Get all the nodes in the cluster, this method generate RPC
   * @return host names
   * @throws YarnRemoteException
   */
  private List<String> getClusterNodes() throws YarnRemoteException {
    List<String> result = new ArrayList<String>();
    GetClusterNodesRequest clusterNodesReq = Records.newRecord(GetClusterNodesRequest.class);
    GetClusterNodesResponse clusterNodesResp = applicationsManager.getClusterNodes(clusterNodesReq);
    List<NodeReport> nodeReports = clusterNodesResp.getNodeReports();
    for (NodeReport nodeReport : nodeReports) {
      result.add(nodeReport.getNodeId().getHost());
    }
    return result;
  }
}
