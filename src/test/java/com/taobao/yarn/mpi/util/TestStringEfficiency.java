package com.taobao.yarn.mpi.util;

import org.junit.Assert;
import org.junit.Test;

public class TestStringEfficiency {

  @Test
  public void testMessageEfficiency() {
    Runtime runtime = Runtime.getRuntime();

    System.out.println("This test case will prove String.format() is memory friendly.");

    System.gc();
    System.gc();

    long currentMemory = runtime.freeMemory();
    long currentTime = System.currentTimeMillis();
    for (int i = 0; i < 100000; i++) {
      @SuppressWarnings("unused")
      String s = String.format(
          "Current requesting state: needed=%d, requested=%d, allocated=%d, requestId=%d",
          i, i, i, i);
    }
    long consumedMemory1 = (currentMemory - runtime.freeMemory());
    System.out.println("String.format() time: "
        + (System.currentTimeMillis() - currentTime)
        + ", memory:"
        + consumedMemory1);

    System.gc();
    System.gc();

    currentTime = System.currentTimeMillis();
    currentMemory = runtime.freeMemory();
    for (int i = 0; i < 100000; i++) {
      @SuppressWarnings("unused")
      String s = "Current requesting state: needed="
          + i +", requested=" + i +", allocated="+ i +", requestId=" + i;
    }
    long consumedMemory2 = (currentMemory - runtime.freeMemory());
    System.out.println("Operator '+' time: "
        + (System.currentTimeMillis() - currentTime)
        + ", memory:"
        + consumedMemory2);

    Assert.assertTrue(consumedMemory1 < consumedMemory2);

  }

}
