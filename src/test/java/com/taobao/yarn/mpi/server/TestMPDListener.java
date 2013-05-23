/**
 * 
 */
package com.taobao.yarn.mpi.server;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMPDListener {
  static MPDListenerImpl mpdListener;
  static Configuration conf;
  static Log LOG = LogFactory.getLog(TestMPDListener.class);

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    mpdListener = new MPDListenerImpl();
    conf = new Configuration();
    mpdListener.init(conf);
    mpdListener.start();

    LOG.info("Listener port: " + mpdListener.getServerPort());
  }

  /**
   * Test method for {@link com.taobao.yarn.mpi.server.MPDListenerImpl#getServerPort()}.
   */
  @Test
  public void testGetServerPort() {
    Assert.assertTrue(mpdListener.getServerPort() > 0);

  }

  /**
   * Test method for {@link com.taobao.yarn.mpi.server.MPDListenerImpl#reportStatus(int, com.taobao.yarn.mpi.server.MPDStatus)}.
   * @throws IOException
   */
  @Test
  public void testReportStatus() throws IOException {
    int port = mpdListener.getServerPort();

    InetSocketAddress addr = new InetSocketAddress("localhost", port);
    MPDProtocol protocol = RPC.getProxy(MPDProtocol.class, MPDProtocol.versionID, addr, conf);

    protocol.reportStatus(1, MPDStatus.INITIALIZED);
    protocol.reportStatus(1, MPDStatus.MPD_STARTED);
    protocol.reportStatus(1, MPDStatus.MPD_CRASH);
    protocol.reportStatus(1, MPDStatus.FINISHED);
  }

  /**
   * Test method for {@link com.taobao.yarn.mpi.server.MPDListenerImpl#addContainer(int)}.
   */
  @Test
  public void testAddContainer() {
    mpdListener.addContainer(0);
    mpdListener.addContainer(1);
    mpdListener.addContainer(2);
    mpdListener.addContainer(3);
  }

}
