package com.taobao.yarn.mpi.server;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.Records;

public final class Utilities {
  private static Log LOG = LogFactory.getLog(Utilities.class);

  /**
   * This is a static class
   */
  private Utilities() { }

  /**
   * Sleep for millis milliseconds, which throws no exception
   * @param millis milliseconds
   */
  public static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      LOG.warn("Thread Interrupted ...");
      LOG.warn(e.getMessage());
    }
  }

  /**
   * Setup the request that will be sent to the RM for the container ask.
   * @param numContainers Containers to ask for from RM
   * @return the setup ResourceRequest to be sent to RM
   */
  public static ResourceRequest setupContainerAskForRM(
      int numContainers,
      int requestPriority,
      int containerMemory) {
    ResourceRequest request = Records.newRecord(ResourceRequest.class);
    // setup requirements for hosts, whether a particular rack/host is needed
    // Refer to apis under org.apache.hadoop.net for more details on how to get figure out
    // rack/host mapping. using * as any host will do for the distributed shell app
    request.setHostName("*");
    // set number of containers needed
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
   * Ask RM to allocate given no. of containers to this Application Master
   * @param requestedContainers Containers to ask for from RM
   * @return Response from RM to AM with allocated containers
   * @throws YarnRemoteException
   */
  public static AMResponse sendContainerAskToRM(
      AtomicInteger rmRequestID,
      ApplicationAttemptId appAttemptID,
      AMRMProtocol resourceManager,
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
}
