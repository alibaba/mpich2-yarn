package org.apache.hadoop.yarn.mpi.server;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.mpi.MPIConfiguration;
import org.apache.hadoop.yarn.mpi.util.MPDException;
import org.apache.hadoop.yarn.util.SystemClock;

/**
 * Implementation of MPDProtocol and MPDListener
 */
public class MPDListenerImpl extends CompositeService implements MPDProtocol,
MPDListener {

  private static final Log LOG = LogFactory.getLog(MPDListenerImpl.class);

  private Server server;

  private final Map<ContainerId, MPDStatus> containerToStatus;

  protected TaskHeartbeatHandler taskHeartbeatHandler;

  public MPDListenerImpl() {
    super("MPDListener");
    containerToStatus = new ConcurrentHashMap<ContainerId, MPDStatus>();
  }

  @Override
  public void init(Configuration conf) {
    // Set AbstractService.conf
    registerHeartbeatHandler(conf);
    super.init(conf);
  }

  protected void registerHeartbeatHandler(Configuration conf) {
    taskHeartbeatHandler = new TaskHeartbeatHandler(this, new SystemClock(),
        conf.getInt(MPIConfiguration.MPI_AM_TASK_LISTENER_THREAD_COUNT,
            MPIConfiguration.DEFAULT_MPI_AM_TASK_LISTENER_THREAD_COUNT));
    addService(taskHeartbeatHandler);
  }

  @Override
  public void start() {
    startRpcServer();
    super.start();
  }

  private void startRpcServer() {
    Configuration conf = getConfig();
    RPC.Builder builder = new RPC.Builder(conf);
    builder.setProtocol(MPDProtocol.class);
    builder.setInstance(this);
    builder.setBindAddress("0.0.0.0");
    builder.setPort(0);
    try {
      server = builder.build();
    } catch (Exception e) {
      LOG.error("Error building RPC server.");
      e.printStackTrace();
      return;
    }
    server.start();
  }

  @Override
  public void reportStatus(ContainerId containerId, MPDStatus containerStatus) {
    LOG.info("Try to report status.");
    try {
      LOG.info(containerId.toString() + " report status " + containerStatus);
      containerToStatus.put(containerId, containerStatus);
      // TODO We need a state machine here to handle port in use and crash
      if (containerStatus.equals(MPDStatus.MPD_CRASH)) {
        LOG.error("Container " + containerId.toString() + " is crashed");
      }
    } catch (Exception e) {
      LOG.error("Error reporting status.");
      e.printStackTrace();
    }
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return MPDProtocol.versionID;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(this, protocol,
        clientVersion, clientMethodsHash);
  }

  @Override
  public void addContainer(ContainerId containerId) {
    containerToStatus.put(containerId, MPDStatus.UNDEFINED);
    taskHeartbeatHandler.register(containerId);
  }

  @Override
  public boolean isAllMPDStarted() throws MPDException {
    Iterator<Entry<ContainerId, MPDStatus>> i = containerToStatus.entrySet()
        .iterator();
    if (containerToStatus.isEmpty()) {
      return false;
    }
    while (i.hasNext()) {
      Entry<ContainerId, MPDStatus> e = i.next();
      if (e.getValue().equals(MPDStatus.ERROR_FINISHED)) {
        throw new MPDException(String.format("Container %s error", e.getKey()
            .toString()));
      } else if (e.getValue().equals(MPDStatus.DISCONNECTED)) {
        throw new MPDException(String.format("Container %s is disconnected", e
            .getKey().toString()));
      } else if (e.getValue().equals(MPDStatus.MPD_CRASH)) {
        throw new MPDException(String.format("Container %s is crashed", e
            .getKey().toString()));
      } else if (e.getValue().equals(MPDStatus.INITIALIZED)
          || e.getValue().equals(MPDStatus.UNDEFINED)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean isAllMPDFinished() {
    if (containerToStatus.isEmpty()) {
      return false;
    }
    LOG.info("Containers in total: " + containerToStatus.size());
    Iterator<Entry<ContainerId, MPDStatus>> i = containerToStatus.entrySet()
        .iterator();
    while (i.hasNext()) {
      Entry<ContainerId, MPDStatus> e = i.next();
      if (!e.getValue().equals(MPDStatus.FINISHED)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int getServerPort() {
    return server.getPort();
  }

  private boolean isAmFinished = false;

  /**
   * Tell all the contaners that am has finished.
   */
  public void setAmFinished(){
    isAmFinished = true;
  }

  @Override
  public boolean ping(ContainerId containerId) {
    taskHeartbeatHandler.pinged(containerId);
    return isAmFinished;
  }

  @Override
  public boolean isAllHealthy() throws MPDException {
    Boolean healthy = true;
    Iterator<Entry<ContainerId, MPDStatus>> i = containerToStatus.entrySet()
        .iterator();
    while (i.hasNext()) {
      Entry<ContainerId, MPDStatus> e = i.next();
      if (e.getValue().equals(MPDStatus.ERROR_FINISHED)) {
        throw new MPDException(String.format("Container %s error", e.getKey()
            .toString()));
      } else if (e.getValue().equals(MPDStatus.DISCONNECTED)) {
        throw new MPDException(String.format("Container %s is disconnected", e
            .getKey().toString()));
      } else if (e.getValue().equals(MPDStatus.MPD_CRASH)) {
        throw new MPDException(String.format("Container %s is crashed", e
            .getKey().toString()));
      }
    }
    return healthy;
  }

}
