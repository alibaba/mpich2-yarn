package com.taobao.yarn.mpi.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import com.taobao.yarn.mpi.MPIConfiguration;
import com.taobao.yarn.mpi.MPIConstants;
import com.taobao.yarn.mpi.api.MPIClientProtocol;
import com.taobao.yarn.mpi.server.ApplicationMaster;
import com.taobao.yarn.mpi.util.InputFile;
import com.taobao.yarn.mpi.util.MPDException;
import com.taobao.yarn.mpi.util.Utilities;

/**
 * Client for MPI application submission to YARN.
 */
public class Client {

  private static final Log LOG = LogFactory.getLog(Client.class);
  // Configuration
  private final MPIConfiguration conf;
  // Handle to talk to the Resource Manager/Applications Manager
  private ApplicationClientProtocol applicationsManager;
  //Handle to communicate to the ApplicationMaster
  private MPIClientProtocol mpiClient;
  // Application master specific info to register a new Application with RM/ASM
  private String appName = "MPICH2-";
  // App master priority
  private int amPriority = 0;
  // Queue for App master
  private String amQueue = "";
  // Amt. of memory resource to request for to run the App Master
  private int amMemory = 64;
  // Application master jar file
  private String appMasterJar = "";
  // Shell Command Container priority
  private int containerPriority = 0;
  // Amt of memory to request for container in which shell script will be executed
  private int containerMemory = 10;
  // No. of containers in which the shell script needs to be executed
  private int numContainers = 1;
  // Start time for client
  private final long clientStartTime = System.currentTimeMillis();
  // Timeout threshold for client. Kill app after time interval expires.
  private long clientTimeout = 24 * 60 * 60 * 1000;  // 86400000 ms, 24 hours
  // Location of the MPI application
  private String mpiApplication = "";
  // Is the MPI application running, we do NOT use 'volatile' for passing reference
  private transient AtomicBoolean isRunning = new AtomicBoolean(false);
  // MPI Options
  private String mpiOptions = "";
  // Debug flag
  boolean debugFlag = false;
  //jvm parameters
  private String jvmOptions = "";
  // processes per container
  private int ppc = 1;
  // key: FILENAME variable, value:the dfs location of the input file
  private final ConcurrentHashMap<String, InputFile> fileToLocation = new ConcurrentHashMap<String, InputFile>();
  // key: FILENAME variable ,value:the dfs location of the result file
  private final ConcurrentHashMap<String, String> resultToLocation = new ConcurrentHashMap<String, String>();

  private enum TaskType {
    RUN, KILL
  }
  private TaskType taskType = TaskType.RUN;
  private String appIdToKill;
  private ApplicationId appId;
  private final FileSystem dfs;

  /**
   * @param args Command line arguments
   */
  public static void main(String[] args) {
    boolean result = false;
    try {
      Client client = new Client();
      LOG.info("Initializing Client");
      if (!client.init(args)) {
        System.exit(0);
      }
      if (client.taskType == TaskType.RUN) {
        result = client.run();
      } else if (client.taskType == TaskType.KILL) {
        result = client.kill();
      }
    } catch (Throwable t) {
      LOG.fatal("Error running CLient", t);
      System.exit(1);
    }
    if (result) {
      LOG.info("Application completed successfully");
      System.exit(0);
    }
    LOG.error("Application failed to complete successfully");
    System.exit(2);
  }

  /**
   * Constructor, create YarnRPC
   * @throws IOException
   */
  public Client(MPIConfiguration conf) throws IOException {
    // Set up the configuration and RPC
    this.conf = conf;
    dfs = FileSystem.get(conf);
  }

  /**
   * Constructor, create YarnRPC
   */
  public Client() throws Exception  {
    this(new MPIConfiguration());
  }

  /**
   * Helper function to print out usage
   * @param opts Parsed command line options
   */
  private void printUsage(Options opts) {
    new HelpFormatter().printHelp("Client", opts);
  }

  // load/reload Configuration
  public void reloadConfiguration() {
    amMemory = conf.getInt(MPIConfiguration.MPI_EXEC_LOCATION, 64);
    containerMemory = conf.getInt(MPIConfiguration.MPI_CONTAINER_MEMORY, 64);
    amPriority = conf.getInt(MPIConfiguration.MPI_AM_PRIORITY, 0);
    containerPriority = conf.getInt(MPIConfiguration.MPI_CONTAINER_PRIORITY, 0);
    amQueue = conf.get(MPIConfiguration.MPI_QUEUE);
    clientTimeout = conf.getLong(MPIConfiguration.MPI_TIMEOUT, 24 * 60 * 60 * 1000);
    jvmOptions = conf.get(MPIConfiguration.MPI_APPLICATION_JAVA_OPTS_EXCEPT_MEMORY, "");
  }

  /**
   * Parse command line options
   * @param args Parsed command line options
   * @return Whether the init was successful to run the client
   * @throws ParseException
   * @throws IOException
   */
  @SuppressWarnings("static-access")
  public boolean init(String[] args) throws ParseException, IOException {

    reloadConfiguration();

    Options opts = new Options();
    opts.addOption("M", "master-memory", true, "Amount of memory in MB to be requested to run the application master");
    opts.addOption("m", "container-memory", true, "Amount of memory in MB to be requested to run the shell command");
    opts.addOption("a", "mpi-application", true, "Location of the mpi application to be executed");
    opts.addOption("o", "mpi-options", true, "Options for mpi program");
    opts.addOption("P", "priority", true, "Application Priority. Default 0");
    opts.addOption("p", "container-priority", true, "Priority for the shell command containers");
    opts.addOption("q", "queue", true, "RM Queue in which this application is to be submitted");
    opts.addOption("t", "timeout", true, "Wall-time, Application timeout in milliseconds, default is 86400000ms, i.e. 24hours");
    opts.addOption("n", "num-containers", true, "No. of containers on which the mpi program needs to be executed");
    opts.addOption("k", "kill", true, "Running MPI Application id");
    opts.addOption("d", "debug", false, "Dump out debug information");
    opts.addOption("h", "help", false, "Print usage");
    opts.addOption("ppc", "processes-per-container", true, "processes per container,take effect when all containers download the same file");
    //support -D for the input file
    Option property = OptionBuilder.withArgName("property=value")
        .hasArgs(2)
        .withValueSeparator()
        .withDescription("dfs location,representing the source data of MPI")
        .create("D");
    opts.addOption(property);

    // support -S for whether all containers share the same input file
    Option downSame = OptionBuilder.withArgName("property=value")
        .hasArgs(2)
        .withValueSeparator()
        .withDescription("Do all containers download the same file?")
        .create("S");
    opts.addOption(downSame);

    //support -O for the mpi result
    Option output = OptionBuilder.withArgName("property=value")
        .hasArgs(2)
        .withValueSeparator()
        .withDescription("dfs location,representing the mpi result")
        .create("O");
    opts.addOption(output);

    CommandLine cliParser = new GnuParser().parse(opts, args);
    // empty commandline
    if (cliParser.getOptions().length == 0  || cliParser.hasOption("help")) {
      printUsage(opts);
      return false;
    }

    if (cliParser.hasOption("kill")) {
      taskType = TaskType.KILL;
      appIdToKill = cliParser.getOptionValue("kill");
      return true;
    }

    if (cliParser.hasOption("master-memory")) {
      amMemory = Integer.parseInt(cliParser.getOptionValue("master-memory", "64"));
    }
    if (amMemory <= 0) {
      throw new IllegalArgumentException("Invalid memory specified for application master, exiting."
          + " Specified memory=" + amMemory);
    }

    if (cliParser.hasOption("container-memory")) {
      containerMemory = Integer.parseInt(cliParser.getOptionValue("container-memory", "64"));
    }
    if (containerMemory <= 0) {
      throw new IllegalArgumentException("Invalid memory specified for containers, exiting."
          + "Specified memory=" + containerMemory);
    }

    if (cliParser.hasOption("priority")) {
      amPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
    }

    if (cliParser.hasOption("queue")) {
      amQueue = cliParser.getOptionValue("queue", "default");
    }

    // MPI executable program is a must
    if (!cliParser.hasOption("mpi-application")) {
      throw new IllegalArgumentException("No mpi executable program specified, exiting.");
    }
    mpiApplication = cliParser.getOptionValue("mpi-application");
    appName += (new File(mpiApplication)).getName();

    if (cliParser.hasOption("mpi-options")) {
      mpiOptions = cliParser.getOptionValue("mpi-options");
    }

    if (cliParser.hasOption("container-priority")) {
      containerPriority = Integer.parseInt(cliParser.getOptionValue("container-priority", "0"));
    }

    numContainers = Integer.parseInt(cliParser.getOptionValue("num-containers", "1"));
    LOG.info("Container number is " + numContainers);
    if (numContainers < 1) {
      throw new IllegalArgumentException("Invalid no. of containers specified, exiting."
          + ", numContainer=" + numContainers);
    }

    if (cliParser.hasOption("timeout")) {
      clientTimeout = Integer.parseInt(cliParser.getOptionValue("timeout", "86400000"));
    }

    if (cliParser.hasOption("debug")) {
      debugFlag = true;
    }

    if (cliParser.hasOption("processes-per-container")){
      ppc = Integer.parseInt(cliParser.getOptionValue("processes-per-container", "1"));
    }

    parseInput(cliParser);

    parseOutput(cliParser);

    appMasterJar = JobConf.findContainingJar(ApplicationMaster.class);
    LOG.info("Application Master's jar is " + appMasterJar);

    return true;
  }

  /**
   * @param cliParser
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  private void parseOutput(CommandLine cliParser) throws IOException {
    Properties outs = cliParser.getOptionProperties("O");
    Enumeration<String> fileNames = (Enumeration<String>)outs.propertyNames();
    while (fileNames.hasMoreElements()) {
      String name = fileNames.nextElement();
      String destination = outs.getProperty(name);
      Path path = new Path(destination);
      if (!dfs.isDirectory(path) && dfs.exists(path)) {
        throw new MPDException(String.format("file %s exists", destination));
      }
      resultToLocation.put(name, destination);
    }
  }

  /**
   * @param cliParser
   */
  @SuppressWarnings("unchecked")
  private void parseInput(CommandLine cliParser) {
    Map<String, Boolean> mapSame = new HashMap<String, Boolean>();
    Properties files = cliParser.getOptionProperties("D");
    Properties sameFiles = cliParser.getOptionProperties("S");
    Enumeration<String> fileNames = (Enumeration<String>)files.propertyNames();
    Enumeration<String> sameFileNames = (Enumeration<String>)sameFiles.propertyNames();
    while (sameFileNames.hasMoreElements()) {
      String name = sameFileNames.nextElement();
      mapSame.put(name, Boolean.valueOf(sameFiles.getProperty(name)));
    }

    while (fileNames.hasMoreElements()) {
      String fileName = fileNames.nextElement();
      InputFile input = new InputFile();
      input.setLocation(files.getProperty(fileName));
      if (mapSame.containsKey(fileName)) {
        input.setSame(mapSame.get(fileName));
      }else {
        input.setSame(false);
      }
      fileToLocation.put(fileName, input);
    }
  }

  /**
   * Main run function for the client
   * @return true if application completed successfully
   * @throws IOException
   */
  public boolean run() throws IOException {
    LOG.info("Starting Client");
    // Connect to ResourceManager
    applicationsManager = Utilities.connectToASM(conf);

    assert(applicationsManager != null);

    // Get a new application id
    GetNewApplicationResponse newApp = getApplication();
    appId = newApp.getApplicationId();
    LOG.info("Got Applicatioin: " + appId.toString());

    // TODO get min/max resource capabilities from RM and change memory ask if needed
    // If we do not have min/max, we may not be able to correctly request
    // the required resources from the RM for the app master
    // Memory ask has to be a multiple of min and less than max.
    // Dump out information about cluster capability as seen by the resource manager
    //int minMem = newApp.getMinimumResourceCapability().getMemory();
    int maxMem = newApp.getMaximumResourceCapability().getMemory();
    //LOG.info("Min mem capabililty of resources in this cluster " + minMem);
    LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

    // A resource ask has to be at least the minimum of the capability of the cluster,
    // the value has to be a multiple of the min value and cannot exceed the max.
    // If it is not an exact multiple of min, the RM will allocate to the nearest multiple of min
    /*
    if (amMemory < minMem) {
      LOG.info("AM memory specified below min threshold of cluster. Using min value."
          + ", specified=" + amMemory
          + ", min=" + minMem);
      amMemory = minMem;
    } else*/ if (amMemory > maxMem) {
      LOG.info("AM memory specified above max threshold of cluster. Using max value."
          + ", specified=" + amMemory
          + ", max=" + maxMem);
      amMemory = maxMem;
    }
    if (containerMemory * numContainers > maxMem) {
      LOG.error("Container memories specified above the max threhold "
          +"(yarn.scheduler.maximum-allocation-mb) of the cluster");
      return false;
    }

    // Create launch context for app master
    LOG.info("Setting up application submission context for ASM");
    ApplicationSubmissionContext appContext = Records.newRecord(ApplicationSubmissionContext.class);

    // set the application id
    LOG.info("Set Application Id: " + appId);
    appContext.setApplicationId(appId);

    // set the application name
    LOG.info("Set Application Name: " + appName);
    appContext.setApplicationName(appName);

    LOG.info("Copy App Master jar from local filesystem and add to local environment");
    // Copy the application master jar to the filesystem
    // Create a local resource to point to the destination jar path
    Path appJarSrc = new Path(appMasterJar);
    Path appJarDst = Utilities.getAppFile(conf, appName, appId, "AppMaster.jar");
    LOG.info("Source path: " + appJarSrc.toString());
    LOG.info("Destination path: " + appJarDst.toString());
    dfs.copyFromLocalFile(false, true, appJarSrc, appJarDst);
    FileStatus appJarDestStatus = dfs.getFileStatus(appJarDst);

    // set local resources for the application master
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    LocalResource amJarRsrc = Records.newRecord(LocalResource.class);
    amJarRsrc.setType(LocalResourceType.FILE);
    amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
    // Set the resource to be copied over
    amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(appJarDst));
    // Set timestamp and length of file so that the framework can do basic sanity
    // checks for the local resource after it has been copied over to ensure
    // it is the same resource the client intended to use with the application
    amJarRsrc.setTimestamp(appJarDestStatus.getModificationTime());
    amJarRsrc.setSize(appJarDestStatus.getLen());
    localResources.put("AppMaster.jar",  amJarRsrc);

    LOG.info("Copy MPI application from local filesystem to remote.");
    assert(!mpiApplication.isEmpty());
    Path mpiAppSrc = new Path(mpiApplication);
    Path mpiAppDst = Utilities.getAppFile(conf, appName, appId, "MPIExec");
    LOG.info("Source path: " + mpiAppSrc.toString());
    LOG.info("Destination path: " + mpiAppDst.toString());
    dfs.copyFromLocalFile(false, true, mpiAppSrc, mpiAppDst);
    FileStatus mpiAppFileStatus = dfs.getFileStatus(mpiAppDst);

    // Set the env variables to be setup in the env where the application master will be run
    LOG.info("Set the environment for the application master and mpi application");
    Map<String, String> env = new HashMap<String, String>();
    // put location of mpi app and appmaster.jar into env
    // using the env info, the application master will create the correct local resource for the
    // eventual containers that will be launched to execute the shell scripts
    env.put(MPIConstants.MPIEXECLOCATION, mpiAppDst.toUri().toString());
    env.put(MPIConstants.MPIEXECTIMESTAMP, Long.toString(mpiAppFileStatus.getModificationTime()));
    env.put(MPIConstants.MPIEXECLEN, Long.toString(mpiAppFileStatus.getLen()));
    env.put(MPIConstants.APPJARLOCATION, appJarDst.toUri().toString());
    env.put(MPIConstants.APPJARTIMESTAMP, Long.toString(appJarDestStatus.getModificationTime()));
    env.put(MPIConstants.APPJARLEN, Long.toString(appJarDestStatus.getLen()));
    if (!mpiOptions.isEmpty()) {
      env.put(MPIConstants.MPIOPTIONS, mpiOptions);
    }
    env.put(MPIConstants.PROCESSESPERCONTAINER,String.valueOf(ppc));
    Set<String> keyFiles = fileToLocation.keySet();
    StringBuilder names = new StringBuilder(50);
    if (keyFiles != null && keyFiles.size() > 0) {
      Iterator<String> itKeys = keyFiles.iterator();
      while (itKeys.hasNext()) {
        String key = itKeys.next();
        //TODO hard coding with '@'
        names.append(key).append("@");
        env.put(key, fileToLocation.get(key).toString());
      }
      env.put(MPIConstants.MPIINPUTS, names.substring(0, names.length()-1).toString());
    }

    Set<String> keyResults = resultToLocation.keySet();
    StringBuilder resultNames = new StringBuilder(50);
    if (keyResults != null && keyResults.size() > 0) {
      Iterator<String> itKeys = keyResults.iterator();
      while (itKeys.hasNext()) {
        String key = itKeys.next();
        resultNames.append(key).append("@");
        env.put(key, resultToLocation.get(key).toString());
      }
      env.put(MPIConstants.MPIOUTPUTS, resultNames.substring(0,resultNames.length()-1).toString());
    }

    // Add AppMaster.jar location to classpath. At some point we should not be
    // required to add the hadoop specific classpaths to the env. It should be
    // provided out of the box. For now setting all required classpaths including
    // the classpath to "." for the application jar
    StringBuilder classPathEnv = new StringBuilder("${CLASSPATH}:./*");
    for (String c : conf.getStrings(
        MPIConfiguration.YARN_APPLICATION_CLASSPATH,
        MPIConfiguration.DEFAULT_MPI_APPLICATION_CLASSPATH)) {
      classPathEnv.append(':');
      classPathEnv.append(c.trim());
    }
    // add the runtime classpath needed for tests to work
    String testRuntimeClassPath = Client.getTestRuntimeClasspath();
    classPathEnv.append(':');
    classPathEnv.append(testRuntimeClassPath);
    env.put("CLASSPATH", classPathEnv.toString());

    // Set the necessary command to execute the application master
    Vector<CharSequence> vargs = new Vector<CharSequence>(30);
    // Set java executable command
    LOG.info("Setting up app master command");
    vargs.add("${JAVA_HOME}" + "/bin/java");
    // Set Xmx based on am memory size
    vargs.add("-Xmx" + amMemory + "m");
    // log are specified by the nodeManager's container-log4j.properties and client can specify the MPI_AM_LOG_LEVEL and MPI_AM_LOG_SIZE
    /*
    String logLevel = conf.get(MPIConfiguration.MPI_AM_LOG_LEVEL, MPIConfiguration.DEFAULT_MPI_AM_LOG_LEVEL);
    long logSize = conf.getLong(MPIConfiguration.MPI_AM_LOG_SIZE, MPIConfiguration.DEFAULT_MPI_AM_LOG_SIZE);
    Utilities.addLog4jSystemProperties(logLevel, logSize, vargs);
    */
    //set java opts except memory
    if (!StringUtils.isBlank(jvmOptions)) {
      vargs.add(jvmOptions);
    }

    // Set class name
    vargs.add("com.taobao.yarn.mpi.server.ApplicationMaster");
    // Set params for Application Master
    vargs.add("--container_memory " + String.valueOf(containerMemory));
    vargs.add("--num_containers " + String.valueOf(numContainers));
    vargs.add("--priority " + String.valueOf(containerPriority));
    if (debugFlag) {
      vargs.add("--debug");
    }
    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");
    // Get final commmand
    StringBuilder command = new StringBuilder();
    for (CharSequence str : vargs) {
      command.append(str).append(" ");
    }

    LOG.info("Completed setting up app master command " + command.toString());
    List<String> commands = new ArrayList<String>();
    commands.add(command.toString());

    // Set up resource type requirements
    // For now, only memory is supported so we set memory requirements
    /*
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(amMemory);
    amContainer.setResource(capability);
    */

    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
        localResources, env, commands, null, null, null);

    appContext.setAMContainerSpec(amContainer);

    // Set the priority for the application master
    Priority pri = Records.newRecord(Priority.class);
    // TODO - what is the range for priority? how to decide?
    pri.setPriority(amPriority);
    appContext.setPriority(pri);

    // Set the queue to which this application is to be submitted in the RM
    appContext.setQueue(amQueue);

    // Create the request to send to the applications manager
    SubmitApplicationRequest appRequest = Records.newRecord(SubmitApplicationRequest.class);
    appRequest.setApplicationSubmissionContext(appContext);

    // Submit the application to the applications manager
    // Ignore the response as either a valid response object is returned on success
    // or an exception thrown to denote some form of a failure
    try {
      LOG.info("Submitting application to ASM");
      SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest);
      isRunning.set(submitResp != null);
      LOG.info("Submisstion result: " + isRunning);
    } catch (YarnException e) {
      LOG.error("Submission failure.", e);
      return false;
    }

    // Monitor the application
    return monitorApplication();
  }

  /**
   * Monitor the submitted application for completion.
   * Kill application if time expires.
   * @param appId Application Id of application to be monitored
   * @return true if application completed successfully
   * @throws IOException
   */
  private boolean monitorApplication() throws IOException {

    Runtime.getRuntime().addShutdownHook(
        new KillRunningAppHook(isRunning, applicationsManager, appId));
    while (true) {
      // Get application report for the appId we are interested in
      ApplicationReport report = Utilities.getApplicationReport(appId, applicationsManager);
      assert(report != null);
      if (mpiClient == null && isRunning.get() == true) {
        LOG.info("Got application report from ASM for"
            + ", appId=" + appId.getId()
            + ", clientToken=" + report.getClientToAMToken()
            + ", appDiagnostics=" + report.getDiagnostics()
            + ", appMasterHost=" + report.getHost()
            + ", rpcPort:" + report.getRpcPort()
            + ", appQueue=" + report.getQueue()
            + ", appMasterRpcPort=" + report.getRpcPort()
            + ", appStartTime=" + report.getStartTime()
            + ", yarnAppState=" + report.getYarnApplicationState().toString()
            + ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
            + ", appTrackingUrl=" + report.getTrackingUrl()
            + ", appUser=" + report.getUser());
        mpiClient = Utilities.connectToAM(conf, report.getHost(), report.getRpcPort());
      }

      YarnApplicationState state = report.getYarnApplicationState();
      FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
      if (YarnApplicationState.FINISHED == state) {
        mpiClient = null;
        isRunning.set(false);
        if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
          LOG.info("Application has completed successfully. Breaking monitoring loop");
          return true;
        } else {
          LOG.info("Application did finished unsuccessfully."
              + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
              + ". Breaking monitoring loop");
          return false;
        }
      } else if (YarnApplicationState.KILLED == state
          || YarnApplicationState.FAILED == state) {
        mpiClient = null;
        isRunning.set(false);
        LOG.info("Application did not finish."
            + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
            + ". Breaking monitoring loop");
        return false;
      }

      if (System.currentTimeMillis() > (clientStartTime + clientTimeout)) {
        mpiClient = null;
        LOG.info("Reached client specified timeout for application. Killing application");
        Utilities.killApplication(applicationsManager, appId);
        isRunning.set(false);
        return false;
      }

      if (mpiClient != null) {
        String[] messages = mpiClient.popAllMPIMessages();
        if (messages != null && messages.length > 0) {
          for (String message : messages) {
            LOG.info(message);
          }
        }
      }
      // pulling log interval,the default is 1000ms
      int logInterval = conf.getInt(MPIConfiguration.MPI_LOG_PULL_INTERVAL, 1000);
      Utilities.sleep(logInterval);
    }  // end while
  }

  /**
   * Get a new application from the ASM
   * @return New Application
   * @throws YarnException
   */
  private GetNewApplicationResponse getApplication() throws YarnException {
    GetNewApplicationRequest request = Records.newRecord(GetNewApplicationRequest.class);
    GetNewApplicationResponse response = applicationsManager.getNewApplication(request);
    LOG.info("Got new application id=" + response.getApplicationId());
    return response;
  }

  private static String getTestRuntimeClasspath() {
    InputStream classpathFileStream = null;
    BufferedReader reader = null;
    String envClassPath = "";

    LOG.info("Trying to generate classpath for app master from current thread's classpath");
    try {
      // Create classpath from generated classpath
      // Check maven ppom.xml for generated classpath info
      // Works if compile time env is same as runtime. Mainly tests.
      ClassLoader thisClassLoader =
          Thread.currentThread().getContextClassLoader();
      String generatedClasspathFile = "yarn-apps-ds-generated-classpath";
      classpathFileStream =
          thisClassLoader.getResourceAsStream(generatedClasspathFile);
      if (classpathFileStream == null) {
        LOG.info("Could not classpath resource from class loader");
        return envClassPath;
      }
      LOG.info("Readable bytes from stream=" + classpathFileStream.available());
      reader = new BufferedReader(new InputStreamReader(classpathFileStream));
      String cp = reader.readLine();
      if (cp != null) {
        envClassPath += cp.trim() + ":";
      }
      // Put the file itself on classpath for tasks.
      envClassPath += thisClassLoader.getResource(generatedClasspathFile).getFile();
    } catch (IOException e) {
      LOG.info("Could not find the necessary resource to generate class path for tests. Error="
          + e.getMessage());
    }

    try {
      if (classpathFileStream != null) {
        classpathFileStream.close();
      }
      if (reader != null) {
        reader.close();
      }
    } catch (IOException e) {
      LOG.info("Failed to close class path file stream or reader. Error=" + e.getMessage());
    }
    return envClassPath;
  }

  /**
   * Kill the command-line specified appId
   * @return
   */
  public boolean kill() {
    try {
      applicationsManager = Utilities.connectToASM(conf);
      ApplicationId appId = parseAppId(appIdToKill);
      if (null == appId)
        return false;
      Utilities.killApplication(applicationsManager, appId);
    } catch (YarnException e) {
      LOG.error("Killing " + appIdToKill + " failed", e);
      return false;
    } catch (IOException e) {
      LOG.error("Connecting RM to kill " + appIdToKill + " failed", e);
      return false;
    }
    return true;
  }

  private static ApplicationId parseAppId(String appIdStr) {
    Pattern p = Pattern.compile("application_(\\d+)_(\\d+)");
    Matcher m = p.matcher(appIdStr);
    if (!m.matches())
      return null;
    ApplicationId appId = Records.newRecord(ApplicationId.class);
    appId.setClusterTimestamp(Long.parseLong(m.group(1)));
    appId.setId(Integer.parseInt(m.group(2)));
    return appId;
  }
}
