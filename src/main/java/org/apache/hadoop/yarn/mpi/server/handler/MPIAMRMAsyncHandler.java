/**
 *
 */
package org.apache.hadoop.yarn.mpi.server.handler;

import java.util.List;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler;

/**
 * @author Óá²©ÎÄ
 *
 */
public class MPIAMRMAsyncHandler implements CallbackHandler {
  /* (non-Javadoc)
   * @see org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler#onContainersCompleted(java.util.List)
   */
  @Override
  public void onContainersCompleted(List<ContainerStatus> statuses) {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler#onContainersAllocated(java.util.List)
   */
  @Override
  public void onContainersAllocated(List<Container> containers) {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler#onShutdownRequest()
   */
  @Override
  public void onShutdownRequest() {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler#onNodesUpdated(java.util.List)
   */
  @Override
  public void onNodesUpdated(List<NodeReport> updatedNodes) {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler#getProgress()
   */
  @Override
  public float getProgress() {
    // TODO Auto-generated method stub
    return 0;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler#onError(java.lang.Throwable)
   */
  @Override
  public void onError(Throwable e) {
    // TODO Auto-generated method stub

  }

}
