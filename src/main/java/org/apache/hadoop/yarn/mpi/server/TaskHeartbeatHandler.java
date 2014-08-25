package org.apache.hadoop.yarn.mpi.server;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.mpi.MPIConfiguration;
import org.apache.hadoop.yarn.util.Clock;



/**
 * This class keeps track of mpi tasks that have already been launched. It
 * determines if a mpi task is alive and running or marks a mpi task as dead if it does
 * not hear from it for a long time.
 *
 */

public class TaskHeartbeatHandler extends AbstractService {

  private static class ReportTime {
    private long lastPing;

    public ReportTime(long time) {
      setLastPing(time);
    }

    public synchronized void setLastPing(long time) {
      lastPing = time;
    }

    public synchronized long getLastPing() {
      return lastPing;
    }

  }

  private static final Log LOG = LogFactory.getLog(TaskHeartbeatHandler.class);

  //thread which runs periodically to see the last time since a heartbeat is
  //received from a task.
  private Thread lostTaskCheckerThread;
  private int taskTimeOut = 5 * 60 * 1000;// 2 mins
  private int taskTimeOutCheckInterval = 30 * 1000; // 30 seconds.
  private final MPDListenerImpl mpdListener;

  private final Clock clock;

  private final ConcurrentMap<ContainerId, ReportTime> runningMPDs;

  public TaskHeartbeatHandler(MPDListenerImpl listener, Clock clock,
      int numThreads) {
    super("TaskHeartbeatHandler");
    this.mpdListener = listener;
    this.clock = clock;
    runningMPDs =
        new ConcurrentHashMap<ContainerId, ReportTime>(16, 0.75f, numThreads);
    LOG.info("TaskHeartbeatHandler starts successfully");
  }

  @Override
  public void init(Configuration conf) {
    super.init(conf);
    taskTimeOut = conf.getInt(MPIConfiguration.MPI_TASK_TIMEOUT, 5 * 60 * 1000);
    taskTimeOutCheckInterval =
        conf.getInt(MPIConfiguration.MPI_TASK_TIMEOUT_CHECK_INTERVAL_MS, 30 * 1000);
  }

  @Override
  public void start() {
    lostTaskCheckerThread = new Thread(new PingChecker());
    lostTaskCheckerThread.setName("TaskHeartbeatHandler PingChecker");
    lostTaskCheckerThread.start();
    LOG.info("TaskHeartbeatHandler PingChecker starts successfully");
    super.start();
  }

  @Override
  public void stop() {
    lostTaskCheckerThread.interrupt();
    super.stop();
  }

  public void pinged(ContainerId containerId) {
    ReportTime time = runningMPDs.get(containerId);
    if(time != null) {
      time.setLastPing(clock.getTime());
    }
  }

  public void register(ContainerId containerId) {
    runningMPDs.put(containerId, new ReportTime(clock.getTime()));
  }

  public void unregister(ContainerId containerId) {
    runningMPDs.remove(containerId);
  }

  private class PingChecker implements Runnable {

    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        Iterator<Map.Entry<ContainerId, ReportTime>> iterator =
            runningMPDs.entrySet().iterator();
        long currentTime = clock.getTime();
        while (iterator.hasNext()) {
          Map.Entry<ContainerId, ReportTime> entry = iterator.next();
          boolean pingTimedOut =
              (currentTime > (entry.getValue().getLastPing() + taskTimeOut));
          if(pingTimedOut) {
            Map.Entry<ContainerId, ReportTime> containerIdToReport =
                iterator.next();
            mpdListener.reportStatus(
                containerIdToReport.getKey(), MPDStatus.DISCONNECTED);
            LOG.error(String.format("containerId:%s timed out after %d second",
                containerIdToReport.getKey().toString(), taskTimeOut/1000));
          }
        }
        try {
          Thread.sleep(taskTimeOutCheckInterval);
        } catch (InterruptedException e) {
          LOG.info("TaskHeartbeatHandler thread interrupted");
          break;
        }
      }
    }
  }

}
