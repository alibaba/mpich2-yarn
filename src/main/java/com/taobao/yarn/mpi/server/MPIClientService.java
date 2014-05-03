/**
 *
 */
package com.taobao.yarn.mpi.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.HadoopYarnProtoRPC;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;

import com.taobao.yarn.mpi.api.MPIClientProtocol;
import com.taobao.yarn.mpi.webapps.AMWebApp;

/**
 * Abstract Service
 */
public class MPIClientService extends AbstractService implements MPIClientProtocol {
  private static final Log LOG = LogFactory.getLog(MPIClientService.class);
  // MPI Web app for each ApplicationMaster
  private WebApp wa;
  private final AppContext appContext;
  private final HadoopYarnProtoRPC RPC = new HadoopYarnProtoRPC();
  private Server server;
  //address binded to MPIClientProtocal's RPC Service
  private InetSocketAddress bindAddress;

  public MPIClientService(AppContext appContext) {
    super(MPIClientService.class.getSimpleName());
    this.appContext = appContext;
  }

  @Override
  public void start() {
    Configuration conf = getConfig();
    try {
      LOG.info("Initializing MPIClientProtocol's RPC services");
      server = RPC.getServer(MPIClientProtocol.class, this, 
          new InetSocketAddress("0.0.0.0", 0), conf, null, 1);
      server.start();
      bindAddress = NetUtils.getConnectAddress(server);
      LOG.info("Starting MPIClientProtocol's RPC service at" + bindAddress);
    } catch (YarnException e) {
      LOG.error("Error starting MPIClientProtocal's RPC Service", e);
    }

    try {
      // TODO why this should be set to "mapreduce", any way to construct resources from
      // other names?
      wa = WebApps.$for("mapreduce", AppContext.class, appContext, null).with(conf)
          .start(new AMWebApp());
      // TODO Build RPC Server for clients

    } catch (Exception e) {
      LOG.error("Web App failed to start, ignore...", e);
    }
  }

  /**
   * Get HTTP Port which web server listens on
   * @return http port
   */
  public int getHttpPort() {
    return wa.port();
  }

  @Override
  public String[] popAllMPIMessages() {
    BlockingQueue<String> msgs = appContext.getMpiMsgQueue();
    ArrayList<String> result = new ArrayList<String>();
    while (true) {
      String line = msgs.poll();
      if (null == line) {
        break;
      }
      result.add(line);
    }
    return result.toArray(new String[0]);
  }

  public InetSocketAddress getBindAddress() {
    return bindAddress;
  }

  public void setBindAddress(InetSocketAddress bindAddress) {
    this.bindAddress = bindAddress;
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return MPIClientProtocol.versionID;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(this,
        protocol, clientVersion, clientMethodsHash);
  }

}
