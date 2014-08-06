/**
 * 
 */
package org.apache.hadoop.yarn.mpi.webapps;

import org.apache.hadoop.yarn.webapp.WebApp;

/**
 * @author zhuoluo
 *
 */
public class AMWebApp extends WebApp {

  @Override
  public void setup() {
    route("/", AppController.class);
    route("/mpi", AppController.class);
  }
}
