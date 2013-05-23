package com.taobao.yarn.mpi.util;

import static org.junit.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.util.StringHelper;
import org.junit.Assert;
import org.junit.Test;

public class TestUtilities {
  static Log LOG = LogFactory.getLog(TestUtilities.class);

  @Test
  public void testGetRandomPhrase() {
    String s = "";
    int mod = 1;
    for (int i = 0; i < 1000; i++) {
      s = Utilities.getRandomPhrase(16);
      if (i % mod == 0) {
        System.out.println("Number" + i + " Phrase is: " + s);
        mod *= 5;
      }
      Assert.assertTrue(s.length() == 16);
      for (int j = 0; j < s.length(); j++) {
        char c = s.charAt(j);
        if (c < '0' || (c > '9' && c < 'A') || (c > 'Z' && c < 'a') || (c > 'z'))
          fail("Wrong phrase " + s);
      }
    }
  }

  @Test
  public void testLocalhostName() {
    String localhost = NetUtils.getHostname();
    Assert.assertNotNull(localhost);
    Assert.assertNotSame("localhost", localhost);
    Assert.assertNotSame("local", localhost);
    Assert.assertNotSame("127.0.0.1", localhost);
    Assert.assertNotSame("0.0.0.0", localhost);
    LOG.info("Localhost: " + localhost);
  }

  @Test
  public void testPAJoin() {
    String s = StringHelper.pajoin("A", "B", "C");
    Assert.assertEquals(s, "A/:B/:C");
  }

}
