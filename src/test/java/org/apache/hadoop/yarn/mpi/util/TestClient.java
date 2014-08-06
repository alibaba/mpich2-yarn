/**
 * 
 */
package org.apache.hadoop.yarn.mpi.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.mpi.client.Client;
import org.junit.Assert;
import org.junit.Test;


public class TestClient {

  /**
   * Test method for {@link org.apache.hadoop.yarn.mpi.client.Client#run()}.
   * @throws NoSuchMethodException
   * @throws SecurityException
   * @throws InvocationTargetException
   * @throws IllegalAccessException
   * @throws IllegalArgumentException
   */
  @Test
  public void testParseAppId() throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
    Method method = Client.class.getDeclaredMethod("parseAppId", String.class);
    method.setAccessible(true);
    ApplicationId appId = (ApplicationId) method.invoke(null, "application_1350393940012_0020");
    Assert.assertEquals(appId.getId(), 20);
    Assert.assertEquals(appId.getClusterTimestamp(), 1350393940012L);

    appId = (ApplicationId) method.invoke(null, "application");
    Assert.assertNull(appId);
  }

}
