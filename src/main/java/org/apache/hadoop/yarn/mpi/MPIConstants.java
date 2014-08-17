package org.apache.hadoop.yarn.mpi;

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
   * Environment key name denoting MPI options
   */
  public static final String MPIOPTIONS = "MPIOPTIONS";

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

  /**
   * Environment key name pointing to the container's data location
   */
  public static final String CONTAININPUT = "CONTAININPUT";

  public static final String CONTAINOUTPUT = "CONTAINOUTPUT";

  /**
   * Environment key name pointing to the appAttemptID
   */
  public static final String APPATTEMPTID = "APPATTEMPTID";

  /**
   * the prefix of the input file
   */
  public static final String MPIINPUTS = "MPIINPUTS";

  /**
   * the prefix of the mpi result
   */
  public static final String MPIOUTPUTS = "MPIOUTPUTS";

  /**
   * process per container
   */
  public static final String PROCESSESPERCONTAINER = "PROCESSESPERCONTAINER";

  public static final int MAX_LINE_LOGS = 200;

  public static final String ALLOCATOR = "ALLOCATOR";
}
