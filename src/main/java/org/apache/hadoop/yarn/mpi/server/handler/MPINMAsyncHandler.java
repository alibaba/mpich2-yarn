/**
 *
 */
package org.apache.hadoop.yarn.mpi.server.handler;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync.CallbackHandler;

/**
 * @author Óá²©ÎÄ
 *
 */
public class MPINMAsyncHandler implements CallbackHandler {

  /* (non-Javadoc)
   * @see org.apache.hadoop.yarn.client.api.async.NMClientAsync.CallbackHandler#onContainerStarted(org.apache.hadoop.yarn.api.records.ContainerId, java.util.Map)
   */
  @Override
  public void onContainerStarted(ContainerId containerId,
      Map<String, ByteBuffer> allServiceResponse) {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.yarn.client.api.async.NMClientAsync.CallbackHandler#onContainerStatusReceived(org.apache.hadoop.yarn.api.records.ContainerId, org.apache.hadoop.yarn.api.records.ContainerStatus)
   */
  @Override
  public void onContainerStatusReceived(ContainerId containerId,
      ContainerStatus containerStatus) {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.yarn.client.api.async.NMClientAsync.CallbackHandler#onContainerStopped(org.apache.hadoop.yarn.api.records.ContainerId)
   */
  @Override
  public void onContainerStopped(ContainerId containerId) {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.yarn.client.api.async.NMClientAsync.CallbackHandler#onStartContainerError(org.apache.hadoop.yarn.api.records.ContainerId, java.lang.Throwable)
   */
  @Override
  public void onStartContainerError(ContainerId containerId, Throwable t) {
    // TODO Auto-generated method stub

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
