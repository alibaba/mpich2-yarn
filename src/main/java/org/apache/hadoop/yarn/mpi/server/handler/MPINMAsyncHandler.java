/**
 *
 */
package org.apache.hadoop.yarn.mpi.server.handler;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync.CallbackHandler;

public class MPINMAsyncHandler implements CallbackHandler {
  private static final Log LOG = LogFactory
      .getLog(MPINMAsyncHandler.class);
  private final AtomicInteger completedContainerCount = new AtomicInteger(0);
  /* (non-Javadoc)
   * @see org.apache.hadoop.yarn.client.api.async.NMClientAsync.CallbackHandler#onContainerStarted(org.apache.hadoop.yarn.api.records.ContainerId, java.util.Map)
   */
  @Override
  public void onContainerStarted(ContainerId containerId,
      Map<String, ByteBuffer> allServiceResponse) {
    LOG.info("onContainerStarted invoked.");
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.yarn.client.api.async.NMClientAsync.CallbackHandler#onContainerStatusReceived(org.apache.hadoop.yarn.api.records.ContainerId, org.apache.hadoop.yarn.api.records.ContainerStatus)
   */
  @Override
  public void onContainerStatusReceived(ContainerId containerId,
      ContainerStatus containerStatus) {
    LOG.info("onContainerStatusReceived invoked.");

  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.yarn.client.api.async.NMClientAsync.CallbackHandler#onContainerStopped(org.apache.hadoop.yarn.api.records.ContainerId)
   */
  @Override
  public void onContainerStopped(ContainerId containerId) {
    LOG.info("onContainerStopped invoked, id=" + containerId);

  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.yarn.client.api.async.NMClientAsync.CallbackHandler#onStartContainerError(org.apache.hadoop.yarn.api.records.ContainerId, java.lang.Throwable)
   */
  @Override
  public void onStartContainerError(ContainerId containerId, Throwable t) {
    LOG.info("onStartContainerError invoked: " + containerId);
    LOG.info(t.getMessage());
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.yarn.client.api.async.NMClientAsync.CallbackHandler#onGetContainerStatusError(org.apache.hadoop.yarn.api.records.ContainerId, java.lang.Throwable)
   */
  @Override
  public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.yarn.client.api.async.NMClientAsync.CallbackHandler#onStopContainerError(org.apache.hadoop.yarn.api.records.ContainerId, java.lang.Throwable)
   */
  @Override
  public void onStopContainerError(ContainerId containerId, Throwable t) {
    // TODO Auto-generated method stub

  }

}
