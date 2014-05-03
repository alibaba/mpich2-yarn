package com.taobao.yarn.mpi.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.HadoopYarnProtoRPC;
import org.apache.hadoop.yarn.util.SystemClock;

import com.taobao.yarn.mpi.MPIConfiguration;
import com.taobao.yarn.mpi.util.MPDException;

/**
 * Implementation of MPDProtocol and MPDListener
 */
public class MPDListenerImpl extends CompositeService implements MPDProtocol, MPDListener {

  private static final Log LOG = LogFactory.getLog(MPDListenerImpl.class);

  private HadoopYarnProtoRPC RPC = new HadoopYarnProtoRPC();
  private Server server;

  private final Map<Integer, MPDStatus> containerToStatus;

  protected TaskHeartbeatHandler taskHeartbeatHandler;

  public MPDListenerImpl() {
    super("MPDListener");
    containerToStatus = new ConcurrentHashMap<Integer, MPDStatus>();
  }

  @Override
  public void init(Configuration conf) {
    // Set AbstractService.conf
    registerHeartbeatHandler(conf);
    super.init(conf);
  }

  protected void registerHeartbeatHandler(Configuration conf) {
    taskHeartbeatHandler = new TaskHeartbeatHandler(this, new SystemClock(), conf.getInt(MPIConfiguration.MPI_AM_TASK_LISTENER_THREAD_COUNT,
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
    server = RPC.getServer(MPDProtocol.class, this,
        new InetSocketAddress("0.0.0.0", 0), conf, null, 1);
    server.start();
  }

  @Override
  public void reportStatus(int containerId, MPDStatus containerStatus) {
    LOG.info(containerId + " report status " + containerStatus);
    containerToStatus.put(containerId, containerStatus);
    // TODO We need a state machine here to handle port in use and crash
    if (containerStatus.equals(MPDStatus.MPD_CRASH)) {
      LOG.error("Container " + containerId + " is crashed");
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
    return ProtocolSignature.getProtocolSignature(this,
        protocol, clientVersion, clientMethodsHash);
  }

  @Override
  public void addContainer(int containerId) {
    containerToStatus.put(containerId, MPDStatus.UNDEFINED);
    taskHeartbeatHandler.register(containerId);
  }

  @Override
  public boolean isAllMPDStarted() throws MPDException {
    Iterator<Entry<Integer, MPDStatus>> i = containerToStatus.entrySet().iterator();
    if (containerToStatus.isEmpty()) {
      return false;
    }
    while (i.hasNext()) {
      Entry<Integer, MPDStatus> e = i.next();
      if (e.getValue().equals(MPDStatus.ERROR_FINISHED)){
        throw new MPDException(String.format("Container %d error", e.getKey()));
      } else if (e.getValue().equals(MPDStatus.DISCONNECTED)) {
        throw new MPDException(String.format("Container %d is disconnected", e.getKey()));
      } else if (e.getValue().equals(MPDStatus.MPD_CRASH)) {
        throw new MPDException(String.format("Container %d is crashed", e.getKey()));
      } else if (e.getValue().equals(MPDStatus.INITIALIZED) || e.getValue().equals(MPDStatus.UNDEFINED)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int getServerPort() {
    return server.getPort();
  }

  @Override
  public void ping(int containerId) {
    taskHeartbeatHandler.pinged(containerId);
  }

  @Override
  public boolean isAllHealthy() throws MPDException {
    Boolean healthy = true;
    Iterator<Entry<Integer, MPDStatus>> i = containerToStatus.entrySet().iterator();
    while (i.hasNext()) {
      Entry<Integer, MPDStatus> e = i.next();
      if (e.getValue().equals(MPDStatus.ERROR_FINISHED)){
        throw new MPDException(String.format("Container %d error", e.getKey()));
      }else if (e.getValue().equals(MPDStatus.DISCONNECTED)) {
        throw new MPDException(String.format("Container %d is disconnected", e.getKey()));
      }else if (e.getValue().equals(MPDStatus.MPD_CRASH)) {
        throw new MPDException(String.format("Container %d is crashed", e.getKey()));
      }
    }
    return healthy;
  }

}
