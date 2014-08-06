/**
 * 
 */
package org.apache.hadoop.yarn.mpi.server;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.mpi.server.MPDListenerImpl;
import org.apache.hadoop.yarn.mpi.server.MPDProtocol;
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
   * Test method for {@link org.apache.hadoop.yarn.mpi.server.MPDListenerImpl#getServerPort()}.
   */
  @Test
  public void testGetServerPort() {
    Assert.assertTrue(mpdListener.getServerPort() > 0);

  }

  /**
   * Test method for {@link org.apache.hadoop.yarn.mpi.server.MPDListenerImpl#reportStatus(int, org.apache.hadoop.yarn.mpi.server.MPDStatus)}.
   * @throws IOException
   */
  @Test
  public void testReportStatus() throws IOException {
    int port = mpdListener.getServerPort();

    InetSocketAddress addr = new InetSocketAddress("localhost", port);
    MPDProtocol protocol = RPC.getProxy(MPDProtocol.class, MPDProtocol.versionID, addr, conf);
    // TODO
  }

  /**
   * Test method for {@link org.apache.hadoop.yarn.mpi.server.MPDListenerImpl#addContainer(int)}.
   */
  @Test
  public void testAddContainer() {
    // TODO
  }

}
