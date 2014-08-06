package org.apache.hadoop.yarn.mpi.webapps;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.webapp.Controller;

import com.google.inject.Inject;

public class AppController extends Controller {

  private final Configuration conf;
  private final App app;

  @Inject
  public AppController(App app, Configuration conf, RequestContext ctx) {
    super(ctx);  // this.context
    this.conf = conf;
    this.app = app;
  }

  @Override
  public void index() {
    set(MPIWebConst.APP_ID, app.context.getApplicationID().toString());

    // Is there any good way to pass object from controller to view?
    List<Container> containers = app.context.getAllContainers();
    set(MPIWebConst.CONTAINER_NUMBER, String.valueOf(containers.size()));
    int i = 1;
    for (Container container : containers) {
      set(MPIWebConst.CONTAINER_HTTP_ADDRESS_PREFIX + i, container.getNodeHttpAddress());
      set(MPIWebConst.CONTAINER_ID_PREFIX + i, container.getId().toString());
      //set(MPIWebConst.CONTAINER_STATUS_PREFIX + i, container.getState().toString());
      i++;
    }
    render(IndexPage.class);
  }

}
