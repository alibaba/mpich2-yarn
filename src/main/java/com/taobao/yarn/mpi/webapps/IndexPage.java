package com.taobao.yarn.mpi.webapps;

import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML;
import org.apache.hadoop.yarn.webapp.view.TwoColumnLayout;

/**
 * The MPI Application Monitoring Page
 */
public class IndexPage extends TwoColumnLayout {
  @Override
  protected void preHead(HTML<_> html) {
    super.preHead(html);
    setTitle("MPI Application");
  }

  @Override
  protected Class<? extends SubView> content() {
    return IndexBlock.class;
  }
}
