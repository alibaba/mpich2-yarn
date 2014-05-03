package com.taobao.yarn.mpi.client;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.taobao.yarn.mpi.util.Utilities;

/**
 * Client Shutdown Hook for Killing running application
 */
public class KillRunningAppHook extends Thread {
  private static final Log LOG = LogFactory.getLog(KillRunningAppHook.class);

  private final AtomicBoolean isRunning;

  private final ApplicationId appId;

  private final ClientRMProtocol applicationsManager;

  public KillRunningAppHook(AtomicBoolean isRunning, ClientRMProtocol applicationsManager, ApplicationId appId) {
    super("KillRunningAppHook");
    this.isRunning = isRunning;
    this.appId = appId;
    this.applicationsManager = applicationsManager;
  }

  @Override
  public void run() {
    if (isRunning.get()) {
      try {
        Utilities.killApplication(applicationsManager, appId);
      } catch (YarnException e) {
        LOG.error("Error killing application: ", e);
      }
    }
  }

}
