package com.taobao.yarn.mpi.util;

import static org.junit.Assert.fail;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.yarn.mpi.server.Utilities;

public class TestUtilities {

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

}
