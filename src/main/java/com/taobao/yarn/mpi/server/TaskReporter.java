package com.taobao.yarn.mpi.server;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.taobao.yarn.mpi.MPIConfiguration;


public class TaskReporter extends Thread {

  private static final Log LOG = LogFactory.getLog(TaskHeartbeatHandler.class);

  private MPDProtocol protocol;

  private Configuration conf;

  private Integer taskPingInterval;

  private ContainerId containerId;

  private Integer taskPingRetry;

  public TaskReporter(MPDProtocol protocol, Configuration conf,
      ContainerId containerId){
    this.protocol = protocol;
    this.conf = conf;
    this.containerId = containerId;
    this.taskPingInterval = this.conf.getInt(MPIConfiguration.TASK_PING_INTERVAL, 30 * 1000);
    taskPingRetry = this.conf.getInt(MPIConfiguration.TASK_PING_RETRY, 3);
  }

  @SuppressWarnings("static-access")
  public void run() {
    while (!Thread.currentThread().interrupted()) {
      int retry = 1;
      while (retry++ <= taskPingRetry){
        try {
          protocol.ping(containerId);
          retry = Integer.MAX_VALUE;//ping successfully,skip the loop
        } catch (Exception e) {
          LOG.error("Communication exception:", e);
        }
      }
      try {
        Thread.sleep(taskPingInterval);
      } catch (InterruptedException e) {
        LOG.info("TaskReporter thread interrupted");
        break;
      }
    }
  }
}
