package com.taobao.yarn.mpi.webapps;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

public class IndexBlock extends HtmlBlock {
  private static final Log LOG = LogFactory.getLog(IndexBlock.class);

  @Override
  protected void render(Block html) {
    String userName = null;
    try {
      userName = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      LOG.error("error while geting userName", e);
    }
    html.h2()._("Application Id :" + $(MPIWebConst.APP_ID))._();
    int numContainers = Integer.parseInt($(MPIWebConst.CONTAINER_NUMBER));
    for (int i = 1; i <= numContainers; i++) {
      html.div().a(
          String.format("http://%s/node/containerlogs/%s/" + userName,
              $(MPIWebConst.CONTAINER_HTTP_ADDRESS_PREFIX + i),
              $(MPIWebConst.CONTAINER_ID_PREFIX + i)),
              String.format("Container Logs: %s", $(MPIWebConst.CONTAINER_ID_PREFIX + i)))
              ._(" Status:" + $(MPIWebConst.CONTAINER_STATUS_PREFIX + i))._();
    }
    html.div()._("by Zhuoluo (zhuoluo.yzl@taobao.com)")._();
  }
}