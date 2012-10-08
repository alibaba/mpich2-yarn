package com.taobao.yarn.mpi;

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

  public static final String[] DEFAULT_MPI_APPLICATION_CLASSPATH = {
    "$HADOOP_CONF_DIR", "$HADOOP_COMMON_HOME/share/hadoop/common/*",
    "$HADOOP_COMMON_HOME/share/hadoop/common/lib/*",
    "$HADOOP_HDFS_HOME/share/hadoop/hdfs/*",
    "$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*",
    "$YARN_HOME/share/hadoop/yarn/*", "$YARN_HOME/share/hadoop/yarn/lib/*" };

  static {
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
