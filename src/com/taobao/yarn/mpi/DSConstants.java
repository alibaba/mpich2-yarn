package com.taobao.yarn.mpi;

/**
 * Constants used in both Client and Application Master
 */
public class DSConstants {

  /**
   * Environment key name pointing to the mpi program's location
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
}
