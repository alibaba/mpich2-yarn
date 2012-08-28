package com.taobao.yarn.mpi;

/**
 * Constants used in both Client and Application Master
 */
public class MPIConstants {

  /**
   * Environment key name pointing to the mpi program's hdfs location
   */
  public static final String MPIEXECLOCATION = "MPIEXECLOCATION";

  /**
   * Environment key name denoting the file timestamp for the mpi program.
   * Used to validate the local resource.
   */
  public static final String MPIEXECTIMESTAMP = "MPIEXECTIMESTAMP";

  /**
   * Environment key name denoting the file content length for the mpi program.
   * Used to validate the local resource.
   */
  public static final String MPIEXECLEN = "MPIEXECLEN";

  /**
   * Environment key name pointing to the hdfs location of appmaster.jar
   */
  public static final String APPJARLOCATION = "APPJARLOCATION";

  /**
   * Environment key name denoting the file timestamp for the appmaster.jar
   * Used to validate the local resource.
   */
  public static final String APPJARTIMESTAMP = "APPJARTIMESTAMP";

  /**
   * Environment key name denoting the file content length for the appmaster.jar
   * Used to validate the local resource.
   */
  public static final String APPJARLEN = "APPJARLEN";
}
