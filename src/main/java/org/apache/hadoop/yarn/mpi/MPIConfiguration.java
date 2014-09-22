package org.apache.hadoop.yarn.mpi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class MPIConfiguration extends YarnConfiguration {

  private static final String MPI_DEFAULT_XML_FILE = "mpi-default.xml";

  private static final String MPI_SITE_XML_FILE = "mpi-site.xml";

  // Here, I do not like the PREFIX+FEATURE style, because it's not good for searching.
  public static final String MPI_EXEC_LOCATION = "yarn.mpi.exec.location";

  public static final String MPI_CONTAINER_MEMORY = "yarn.mpi.container.memory";

  public static final String MPI_AM_PRIORITY = "yarn.mpi.appmaster.priority";

  public static final String MPI_CONTAINER_PRIORITY = "yarn.mpi.container.priority";

  public static final String MPI_QUEUE = "yarn.mpi.queue";

  public static final String MPI_TIMEOUT = "yarn.mpi.timeout";

  public static final String MPI_DOWNLOAD_RETRY = "yarn.mpi.download.retry";

  public static final String MPI_CONTAINER_JAVA_OPTS_EXCEPT_MEMORY = "yarn.mpi.container.java.opts";
  public static final String MPI_APPLICATION_JAVA_OPTS_EXCEPT_MEMORY = "yarn.mpi.application.java.opts";

  public static final String MPI_CONTAINER_DOWNLOAD_SAVE = "yarn.mpi.container.download.save";

  public static final String MPI_ALLOCATE_INTERVAL = "mpi.allocate.interval";

  public static final String MPI_CONTAINER_ALLOCATOR = "yarn.mpi.container.allocator";

  public static final String DEFAULT_MPI_CONTAINER_ALLOCATOR = "org.apache.hadoop.yarn.mpi.allocator.MultiMPIProcContainersAllocator";

  public static final String MPI_LOG_PULL_INTERVAL = "mpi.log.pull.interval";

  public static final String MPI_TASK_TIMEOUT = "yarn.mpi.task.timeout";

  public static final String MPI_TASK_TIMEOUT_CHECK_INTERVAL_MS = "yarn.mpi.task.timeout.check.interval";

  public static final String MPI_AM_TASK_LISTENER_THREAD_COUNT = "yarn.mpi.am.task.listener.thread.count";

  public static final Integer DEFAULT_MPI_AM_TASK_LISTENER_THREAD_COUNT = 5;

  public static final String TASK_PING_INTERVAL = "yarn.mpi.task.ping.interval";

  public static final String TASK_PING_RETRY = "yarn.mpi.task.ping.retry";

  public static final String MPI_NM_STARTUP_USERDIR = "mpi.nodemanger.startup.userdir";

  public static final String MPI_SSH_PUBLICKEY_ADDR = "mpi.ssh.publickey.addr";

  public static final String DEFAULT_MPI_SSH_PUBLICKEY_ADDR = "/home/hadoop/.ssh/id_rsa.pub";

  public static final String MPI_APPLICATION_MASTER_DEFAULT_ADDRESS = "0.0.0.0:0";

  public static final int MPI_APPLICATION_WAIT_MESSAGE_EMPTY = 3000;

  public static final String MPI_AM_LOG_LEVEL = "yarn.mpi.am.log.level";

  public static final String DEFAULT_MPI_AM_LOG_LEVEL = "INFO";

  public static final String MPI_CONTAINER_LOG_LEVEL = "yarn.mpi.container.log.level";

  public static final String DEFAULT_MPI_CONTAINER_LOG_LEVEL = "INFO";

  public static final String MPI_AM_LOG_SIZE = "mpi.am.log.size";

  public static final long DEFAULT_MPI_AM_LOG_SIZE = Long.MAX_VALUE;

  public static final String MPI_CONTAINER_LOG_SIZE = "mpi.container.log.size";

  public static final long DEFAULT_MPI_CONTAINER_LOG_SIZE = Long.MAX_VALUE;

  public static final String MPI_SCRATCH_DIR = "yarn.mpi.scratch.dir";

  public static final String MPI_SSH_AUTHORIZED_KEYS_PATH = "yarn.mpi.ssh.authorizedkeys.path";

  public static final String DEFAULT_MPI_SCRATCH_DIR = "hdfs://hdpnn/group/dc/mpi-tmp";

  public static final String[] DEFAULT_MPI_APPLICATION_CLASSPATH = {
    "$HADOOP_CONF_DIR", "$HADOOP_COMMON_HOME/share/hadoop/common/*",
    "$HADOOP_COMMON_HOME/share/hadoop/common/lib/*",
    "$HADOOP_HDFS_HOME/share/hadoop/hdfs/*",
    "$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*",
    "$YARN_HOME/share/hadoop/yarn/*", "$YARN_HOME/share/hadoop/yarn/lib/*" };

  static{
    YarnConfiguration.addDefaultResource(MPI_DEFAULT_XML_FILE);
    YarnConfiguration.addDefaultResource(MPI_SITE_XML_FILE);
  }

  public MPIConfiguration() {
    super();
  }

  public MPIConfiguration(Configuration conf) {
    super(conf);
  }
}
