/**
 *
 */
package org.apache.hadoop.yarn.mpi.server;

import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMPIClientService {

  private static ApplicationMaster appMaster;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Configuration conf = new Configuration();
    // appMaster = new ApplicationMaster();
    // appMaster.initAndStartRPCServices();
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testHttpRequest() throws IOException {
    // String trackingUrl = "http://localhost:" + appMaster.getHttpPort();
    // System.out.println("Tracking URL is " + trackingUrl);
    //    URL url = new URL(trackingUrl);
    //    HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
    //    httpConn.setRequestMethod("GET");
    //    Assert.assertEquals("HTTP/1.1 200 OK", httpConn.getHeaderField(0));
  }

}
