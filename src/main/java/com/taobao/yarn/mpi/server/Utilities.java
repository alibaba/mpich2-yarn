package com.taobao.yarn.mpi.server;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

public final class Utilities {
  private static Log LOG = LogFactory.getLog(Utilities.class);

  private static Random rnd = new Random();

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
      LOG.warn("Thread Interrupted ...", e);
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
    // set the priority for the request
    Priority pri = Records.newRecord(Priority.class);
    // TODO - what is the range for priority? how to decide?
    pri.setPriority(requestPriority);

    // Set up resource type requirements
    // For now, only memory is supported so we set memory requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(containerMemory);

    // setup requirements for hosts, whether a particular rack/host is needed
    // Refer to apis under org.apache.hadoop.net for more details on how to get figure out
    // rack/host mapping. using * as any host will do for the distributed shell app
    ResourceRequest request = ResourceRequest.newInstance(
        pri, "*", capability, numContainers);
    return request;
  }

  /**
   * Ask RM to allocate given no. of containers to this Application Master
   * @param requestedContainers Containers to ask for from RM
   * @return Response from RM to AM with allocated containers
   * @throws YarnException, IOException
   */
  public static AllocateResponse sendContainerAskToRM(
      AtomicInteger rmRequestID,
      ApplicationAttemptId appAttemptID,
      ApplicationMasterProtocol resourceManager,
      List<ResourceRequest> requestedContainers,
      List<ContainerId> releasedContainers,
      float progress) throws YarnException, IOException {
    AllocateRequest req = AllocateRequest.newInstance(
        rmRequestID.incrementAndGet(), progress,
        requestedContainers, releasedContainers, null);

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
    return resp;
  }

  /**
   * Get the random phrase of 0-9, A-Z, a-z
   * @param length the length of the phrase
   * @return the phrase
   */
  public static String getRandomPhrase(int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      char c = (char) rnd.nextInt(62);
      if (c < (char) 10) {
        c += '0';  // '0' to ''9'
      } else if (c < (char) 36) {
        c += 55;  // 'A'(65) to 'Z'(90)
      } else {
        c += 61;  // 'a'(97) to 'z'(122)
      }
      sb.append(c);
    }
    return sb.toString();
  }
}
