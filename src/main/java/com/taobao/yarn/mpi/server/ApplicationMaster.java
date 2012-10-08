package com.taobao.yarn.mpi.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import com.taobao.yarn.mpi.MPIConfiguration;
import com.taobao.yarn.mpi.MPIConstants;
import com.taobao.yarn.mpi.allocator.ContainersAllocator;
import com.taobao.yarn.mpi.allocator.MultiMPIProcContainersAllocator;

public class ApplicationMaster {

  private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);
  // Configuration
  private final Configuration conf;
  // YARN RPC to communicate with the Resource Manager or Node Manager
  private final YarnRPC rpc;
  // Handle to communicate with the Resource Manager
  private AMRMProtocol resourceManager;
  // Handle to talk to the Resource Manager/Applications Manager
  @SuppressWarnings("unused")
  private final ClientRMProtocol applicationsManager;
  // Application Attempt Id ( combination of attemptId and fail count )
  private ApplicationAttemptId appAttemptID;
  // TODO For status update for clients - yet to be implemented.
  // Hostname of the container
  private final String appMasterHostname = "";
  // Port on which the app master listens for status update requests from clients
  private final int appMasterRpcPort = 0;
  // Tracking url to which app master publishes info for clients to monitor
  private final String appMasterTrackingUrl = "";
  // App Master configuration
  // No. of containers to run shell command on
  private int numTotalContainers = 1;
  // Memory to request for the container on which the shell command will run
  private int containerMemory = 10;
  // Priority of the request
  private int requestPriority;
  // Simple flag to denote whether all works is done
  private boolean appDone = false;
  // Counter for completed containers ( complete denotes successful or failed )
  private final AtomicInteger numCompletedContainers = new AtomicInteger();
  // Count of failed containers
  private final AtomicInteger numFailedContainers = new AtomicInteger();
  // Launch threads
  private final List<Thread> launchThreads = new ArrayList<Thread>();
  // Hosts
  private final Set<String> hosts = new HashSet<String>();
  // location of MPI program on HDFS
  private String hdfsMPIExecLocation;
  // timestamp of MPI program on HDFS
  private long hdfsMPIExecTimestamp;
  // file length of MPI program on HDFS
  private long hdfsMPIExecLen;
  // location of AppMaster.jar on HDFS
  private String hdfsAppJarLocation;
  // timestamp of AppMaster.jar on HDFS
  private long hdfsAppJarTimeStamp;
  // file length of AppMaster.jar on HDFS
  private long hdfsAppJarLen;
  // MPI Exec local dir, eath node must have the same dir
  private String mpiExecDir;
  // MPI program options
  private String mpiOptions = "";
  private Map<Container, Integer> procNumForContainers;
  private List<Container> allContainers;

  /**
   * @param args Command line args
   */
  public static void main(String[] args) {
    boolean result = false;
    try {
      ApplicationMaster appMaster = new ApplicationMaster();
      LOG.info("Initializing ApplicationMaster");
      if (!appMaster.init(args)) {
        System.exit(0);
      }
      result = appMaster.run();
    } catch (Throwable t) {
      LOG.fatal("Error running ApplicationMaster", t);
      System.exit(1);
    }
    if (result) {
      LOG.info("Application Master completed successfully. exiting");
      System.exit(0);
    } else {
      LOG.info("Application Master failed. exiting");
      System.exit(2);
    }
  }

  /**
   * Dump out contents of the environment to stdout for debugging
   */
  private void dumpOutDebugInfo() {
    LOG.info("Dump debug output");
    Map<String, String> envs = System.getenv();
    for (Map.Entry<String, String> env : envs.entrySet()) {
      LOG.info("System env: key=" + env.getKey() + ", val=" + env.getValue());
      System.out.println("System env: key=" + env.getKey() + ", val=" + env.getValue());
    }
  }

  public ApplicationMaster() throws Exception {
    // Set up the configuration and RPC
    conf = new Configuration();
    rpc = YarnRPC.create(conf);
    applicationsManager = connectToASM();
  }

  /**
   * Parse command line options
   * @param args Command line args
   * @return Whether init successful and run should be invoked
   * @throws ParseException
   * @throws IOException
   */
  public boolean init(String[] args) throws ParseException, IOException {

    Options opts = new Options();
    opts.addOption("app_attempt_id", true, "App Attempt ID. Not to be used unless for testing purposes");
    opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run the shell command");
    opts.addOption("num_containers", true, "No. of containers on which the shell command needs to be executed");
    opts.addOption("priority", true, "Application Priority. Default 0");
    opts.addOption("o", "mpi-options", true, "MPI Program Options");
    opts.addOption("debug", false, "Dump out debug information");
    opts.addOption("help", false, "Print usage");
    CommandLine cliParser = new GnuParser().parse(opts, args);

    if (args.length == 0) {
      printUsage(opts);
      throw new IllegalArgumentException("No args specified for application master to initialize");
    }

    if (cliParser.hasOption("help")) {
      printUsage(opts);
      return false;
    }

    if (cliParser.hasOption("debug")) {
      dumpOutDebugInfo();
    }

    Map<String, String> envs = System.getenv();

    appAttemptID = Records.newRecord(ApplicationAttemptId.class);
    if (!envs.containsKey(ApplicationConstants.AM_CONTAINER_ID_ENV)) {
      // Only for test purpose
      if (cliParser.hasOption("app_attempt_id")) {
        String appIdStr = cliParser.getOptionValue("app_attempt_id", "");
        appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
      } else {
        throw new IllegalArgumentException("Application Attempt Id not set in the environment");
      }
    } else {
      ContainerId containerId = ConverterUtils.toContainerId(envs.get(ApplicationConstants.AM_CONTAINER_ID_ENV));
      appAttemptID = containerId.getApplicationAttemptId();
    }

    LOG.info("Application master for app"
        + ", appId=" + appAttemptID.getApplicationId().getId()
        + ", clustertimestamp=" + appAttemptID.getApplicationId().getClusterTimestamp()
        + ", attemptId=" + appAttemptID.getAttemptId());

    assert(envs.containsKey(MPIConstants.MPIEXECLOCATION));
    hdfsMPIExecLocation = envs.get(MPIConstants.MPIEXECLOCATION);
    LOG.info("HDFS mpi application location: " + hdfsMPIExecLocation);
    if (envs.containsKey(MPIConstants.MPIEXECTIMESTAMP)) {
      hdfsMPIExecTimestamp = Long.valueOf(envs.get(MPIConstants.MPIEXECTIMESTAMP));
    }
    if (envs.containsKey(MPIConstants.MPIEXECLEN)) {
      hdfsMPIExecLen = Long.valueOf(envs.get(MPIConstants.MPIEXECLEN));
    }
    if (!hdfsMPIExecLocation.isEmpty()
        && (hdfsMPIExecTimestamp <= 0
        || hdfsMPIExecLen <= 0)) {
      LOG.error("Illegal values in env for shell script path"
          + ", path=" + hdfsMPIExecLocation
          + ", len=" + hdfsMPIExecLen
          + ", timestamp=" + hdfsMPIExecTimestamp);
      throw new IllegalArgumentException("Illegal values in env for mpi app path");
    }

    assert(envs.containsKey(MPIConstants.APPJARLOCATION));
    hdfsAppJarLocation = envs.get(MPIConstants.APPJARLOCATION);
    LOG.info("HDFS AppMaster.jar location: " + hdfsAppJarLocation);
    if (envs.containsKey(MPIConstants.APPJARTIMESTAMP)) {
      hdfsAppJarTimeStamp = Long.valueOf(envs.get(MPIConstants.APPJARTIMESTAMP));
    }
    if (envs.containsKey(MPIConstants.APPJARLEN)) {
      hdfsAppJarLen = Long.valueOf(envs.get(MPIConstants.APPJARLEN));
    }
    if (!hdfsAppJarLocation.isEmpty()
        && (hdfsAppJarTimeStamp <= 0
        || hdfsAppJarLen <= 0)) {
      LOG.error("Illegal values in env for shell script path"
          + ", path=" + hdfsAppJarLocation
          + ", len=" + hdfsAppJarLen
          + ", timestamp=" + hdfsAppJarTimeStamp);
      throw new IllegalArgumentException("Illegal values in env for AppMaster.jar path");
    }

    if (envs.containsKey(MPIConstants.MPIOPTIONS)) {
      mpiOptions = envs.get(MPIConstants.MPIOPTIONS);
      LOG.info("Got extra MPI options: \"" + mpiOptions + "\"");
    }

    // FIXME MPI executable local directory, hard coding '/home/hadoop'
    mpiExecDir = "/home/hadoop/mpiexecs/" + appAttemptID.toString();

    containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));
    LOG.info("Container memory is " + containerMemory + " MB");

    numTotalContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));
    LOG.info("Number of total containers is " + numTotalContainers);

    requestPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
    return true;
  }

  /**
   * Helper function to print usage
   * @param opts Parsed command line options
   */
  private void printUsage(Options opts) {
    new HelpFormatter().printHelp("ApplicationMaster", opts);
  }

  /**
   * Main run function for the application master
   * @throws IOException
   */
  public boolean run() throws IOException {
    LOG.info("Starting ApplicationMaster");

    // Connect to ResourceManager
    resourceManager = connectToRM();

    // Setup local RPC Server to accept status requests directly from clients
    // TODO need to setup a protocol for client to be able to communicate to the RPC server
    // TODO use the rpc port info to register with the RM for the client to send requests to this app master

    // Register self with ResourceManager
    RegisterApplicationMasterResponse response = registerToRM();
    // Dump out information about cluster capability as seen by the resource manager
    int minMem = response.getMinimumResourceCapability().getMemory();
    int maxMem = response.getMaximumResourceCapability().getMemory();
    LOG.info("Min mem capabililty of resources in this cluster " + minMem);
    LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

    // A resource ask has to be atleast the minimum of the capability of the cluster, the value has to be
    // a multiple of the min value and cannot exceed the max.
    // If it is not an exact multiple of min, the RM will allocate to the nearest multiple of min
    if (containerMemory < minMem) {
      LOG.info("Container memory specified below min threshold of cluster. Using min value."
          + ", specified=" + containerMemory
          + ", min=" + minMem);
      containerMemory = minMem;  // container memory should be multiple of minMem
    } else if (containerMemory > maxMem) {
      LOG.info("Container memory specified above max threshold of cluster. Using max value."
          + ", specified=" + containerMemory
          + ", max=" + maxMem);
      containerMemory = maxMem;
    }

    // Setup heartbeat emitter
    // TODO poll RM every now and then with an empty request to let RM know that
    // we are alive. The heartbeat interval after which an AM is timed out by the
    // RM is defined by a config setting: RM_AM_EXPIRY_INTERVAL_MS with default
    // defined by DEFAULT_RM_AM_EXPIRY_INTERVAL_MS. The allocate calls to the RM
    // count as heartbeats so, for now, this additional heartbeat emitter is not required.

    // Setup ask for containers from RM
    // Send request for containers to RM
    // Until we get our fully allocated quota, we keep on polling RM for containers
    // Keep looping until all the containers are launched and shell script executed on them
    // ( regardless of success/failure).

    ContainersAllocator allocator = new MultiMPIProcContainersAllocator(resourceManager,
        requestPriority, containerMemory, appAttemptID);
    allContainers = allocator.allocateContainers(numTotalContainers);
    procNumForContainers = allocator.getProcNumForContainers();
    for (Container allocatedContainer : allContainers) {
      String host = allocatedContainer.getNodeId().getHost();
      hosts.add(host);
      LOG.info("Launching command on a new container."
          + ", containerId=" + allocatedContainer.getId()
          + ", containerNode=" + allocatedContainer.getNodeId().getHost()
          + ":" + allocatedContainer.getNodeId().getPort()
          + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
          + ", containerState" + allocatedContainer.getState()
          + ", containerResourceMemory" + allocatedContainer.getResource().getMemory());
      //+ ", containerToken" + allocatedContainer.getContainerToken().getIdentifier().toString());
      LaunchContainerRunnable runnableLaunchContainer =
          new LaunchContainerRunnable(allocatedContainer);
      Thread launchThread = new Thread(runnableLaunchContainer);
      // launch and start the container on a separate thread to keep the main thread unblocked
      // as all containers may not be allocated at one go.
      launchThreads.add(launchThread);
      launchThread.start();
    }

    // FIXME Bad Coding, wait all SMPD for staring
    try {
      LOG.info("Wait 5s to start MPI program ...");
      Thread.sleep(5000);
    } catch (InterruptedException e1) {
      LOG.warn(e1.getMessage());
      e1.printStackTrace();
    }

    launchMpiExec();

    int loopCounter = -1;

    while (numCompletedContainers.get() < numTotalContainers
        && !appDone) {
      loopCounter++;
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.warn(e.getMessage());
        e.printStackTrace();
      }

      LOG.info("Sending empty request to RM, to let RM know we are alive");
      AMResponse amResp = sendEmptyContainerAskToRM();

      // Check what the current available resources in the cluster are
      // TODO should we do anything if the available resources are not enough?
      Resource availableResources = amResp.getAvailableResources();
      LOG.info("Current available resources in the cluster " + availableResources);

      // Check the completed containers
      List<ContainerStatus> completedContainers = amResp.getCompletedContainersStatuses();
      LOG.info("Got response from RM for container ask, completedCnt=" + completedContainers.size());
      for (ContainerStatus containerStatus : completedContainers) {
        LOG.info("Got container status for containerID= " + containerStatus.getContainerId()
            + ", state=" + containerStatus.getState()
            + ", exitStatus=" + containerStatus.getExitStatus()
            + ", diagnostics=" + containerStatus.getDiagnostics());

        // increment counters for completed/failed containers
        int exitStatus = containerStatus.getExitStatus();
        switch (exitStatus) {
        case 0:  // exit successfully
          numCompletedContainers.incrementAndGet();
          LOG.info("Container completed successfully."
              + " containerId=" + containerStatus.getContainerId());
          break;
        case -1000:  // still running
          LOG.info("Container is still running."
              + " containerId=" + containerStatus.getContainerId());
          break;
        case -100:  // being killed or getting lost
          numCompletedContainers.incrementAndGet();
          numFailedContainers.incrementAndGet();
          break;
        case -101:
          LOG.info("Container is not launched."
              + " containerId=" + containerStatus.getContainerId());
          break;
        default:
          LOG.info("Something else happened."
              + " containerId=" + containerStatus.getContainerId());
          break;
        }
      }
      if (numCompletedContainers.get() == numTotalContainers) {
        appDone = true;
      }

      LOG.info("Current application state: loop=" + loopCounter
          + ", appDone=" + appDone
          + ", total=" + numTotalContainers
          + ", completed=" + numCompletedContainers
          + ", failed=" + numFailedContainers);
      // TODO Add a timeout handling layer, for misbehaving mpi application
    }  // end while

    // Join all launched threads
    // needed for when we time out
    // and we need to release containers
    for (Thread launchThread : launchThreads) {
      try {
        launchThread.join();
      } catch (InterruptedException e) {
        LOG.info("Exception thrown in thread join: " + e.getMessage());
        e.printStackTrace();
      }
    }

    // When the application completes, it should send a finish application signal
    // to the RM
    LOG.info("Application completed. Signalling finish to RM");

    FinishApplicationMasterRequest finishReq = Records.newRecord(FinishApplicationMasterRequest.class);
    finishReq.setAppAttemptId(appAttemptID);
    boolean isSuccess = true;
    if (numFailedContainers.get() == 0) {
      finishReq.setFinishApplicationStatus(FinalApplicationStatus.SUCCEEDED);
    }
    else {
      finishReq.setFinishApplicationStatus(FinalApplicationStatus.FAILED);
      String diagnostics = "Diagnostics."
          + ", total=" + numTotalContainers
          + ", completed=" + numCompletedContainers.get()
          + ", failed=" + numFailedContainers.get();
      finishReq.setDiagnostics(diagnostics);
      isSuccess = false;
    }
    resourceManager.finishApplicationMaster(finishReq);
    return isSuccess;
  }

  /**
   * Application Master launches "mpiexec" process locally
   * @throws IOException
   */
  private void launchMpiExec() throws IOException {
    LOG.info("Launching mpiexec from the Application Master...");

    StringBuilder commandBuilder = new StringBuilder("mpiexec -hosts ");
    commandBuilder.append(allContainers.size());
    for (Container host : allContainers) {
      commandBuilder.append(" ");
      commandBuilder.append(host.getNodeId().getHost());
      commandBuilder.append(" ");
      commandBuilder.append(procNumForContainers.get(host));
    }
    commandBuilder.append(" ");
    commandBuilder.append(mpiExecDir);
    commandBuilder.append("/MPIExec");
    if (!mpiOptions.isEmpty()) {
      commandBuilder.append(" ");
      commandBuilder.append(mpiOptions);
    }
    LOG.info("Executing command:" + commandBuilder.toString());
    Runtime rt = Runtime.getRuntime();

    String[] envStrs = generateEnvStrs();
    final Process pc = rt.exec(commandBuilder.toString(), envStrs);

    Thread stdinThread = new Thread(new Runnable() {
      @Override
      public void run() {
        Scanner pcStdin = new Scanner(pc.getInputStream());
        while (pcStdin.hasNextLine()) {
          LOG.info(pcStdin.nextLine());
        }
      }
    });
    stdinThread.start();

    Thread stderrThread = new Thread(new Runnable() {
      @Override
      public void run() {
        Scanner pcStderr = new Scanner(pc.getErrorStream());
        while (pcStderr.hasNextLine()) {
          LOG.info(pcStderr.nextLine());
        }
      }
    });
    stderrThread.start();

    Thread shutdownThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          int ret = pc.waitFor();
          LOG.info("MPI Process returned with value: " + ret);
          LOG.info("Shutting down daemons...");
          Runtime rt = Runtime.getRuntime();
          for (String host : hosts) {
            try {
              // FIXME hard coding password
              String command = "smpd -shutdown " + host + " -phrase 123456 -debug";
              LOG.info("Executing the command: " + command);
              Process process = rt.exec(command);
              Scanner scanner = new Scanner(process.getInputStream());
              while (scanner.hasNextLine()) {
                LOG.info(scanner.nextLine());
              }

              scanner = new Scanner(process.getErrorStream());
              while (scanner.hasNextLine()) {
                LOG.info(scanner.nextLine());
              }
            } catch (IOException e) {
              LOG.warn(e.getMessage());
              e.printStackTrace();
            }
          }
          LOG.info("All daemons shut down! :-D");
        } catch (InterruptedException e) {
          e.printStackTrace();
          LOG.warn(e.getMessage());
        }
      }
    });
    shutdownThread.start();
  }

  private String[] generateEnvStrs() {
    Map<String, String> envs = System.getenv();
    String[] envStrs = new String[envs.size()];
    int i = 0;
    for (Entry<String, String> env : envs.entrySet()) {
      if (env.getKey().equals("HOME")) {
        envStrs[i] = env.getKey() + "=/home/hadoop";  // FIXME hard code
      } else {
        envStrs[i] = env.getKey() + "=" + env.getValue();
      }
      i++;
    }
    return envStrs;
  }

  /**
   * Thread to connect to the {@link ContainerManager} and
   * launch the container that will execute the shell command.
   */
  private class LaunchContainerRunnable implements Runnable {

    Log LOG = LogFactory.getLog(LaunchContainerRunnable.class);
    // Allocated container
    Container container;
    // Handle to communicate with ContainerManager
    ContainerManager cm;

    /**
     * @param lcontainer Allocated container
     */
    public LaunchContainerRunnable(Container lcontainer) {
      this.container = lcontainer;
    }

    /**
     * Helper function to connect to CM
     */
    private void connectToCM() {
      LOG.debug("Connecting to ContainerManager for containerid=" + container.getId());
      String cmIpPortStr = container.getNodeId().getHost() + ":"
          + container.getNodeId().getPort();
      InetSocketAddress cmAddress = NetUtils.createSocketAddr(cmIpPortStr);
      LOG.info("Connecting to ContainerManager at " + cmIpPortStr);
      this.cm = ((ContainerManager) rpc.getProxy(ContainerManager.class, cmAddress, conf));
    }

    /**
     * Connects to CM, sets up container launch context
     * for shell command and eventually dispatches the container
     * start request to the CM.
     */
    @Override
    public void run() {
      // Connect to ContainerManager
      connectToCM();

      LOG.info("Setting up container launch container for containerid=" + container.getId());

      ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
      ctx.setContainerId(container.getId());
      ctx.setResource(container.getResource());

      String jobUserName = System.getenv(ApplicationConstants.Environment.USER
          .name());
      ctx.setUser(jobUserName);
      LOG.info("Setting user in ContainerLaunchContext to: " + jobUserName);

      // Set the local resources for each container
      Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
      assert(!hdfsMPIExecLocation.isEmpty());
      LocalResource mpiRsrc = Records.newRecord(LocalResource.class);
      mpiRsrc.setType(LocalResourceType.FILE);
      mpiRsrc.setVisibility(LocalResourceVisibility.PUBLIC);
      try {
        mpiRsrc.setResource(ConverterUtils.getYarnUrlFromURI(new URI(hdfsMPIExecLocation)));
      } catch (URISyntaxException e) {
        LOG.error("Error when trying to use mpi application path specified in env"
            + ", path=" + hdfsMPIExecLocation);
        e.printStackTrace();
        return;
      }
      mpiRsrc.setTimestamp(hdfsMPIExecTimestamp);
      mpiRsrc.setSize(hdfsMPIExecLen);
      localResources.put("MPIExec", mpiRsrc);
      assert(!hdfsAppJarLocation.isEmpty());
      LocalResource appJarRsrc = Records.newRecord(LocalResource.class);
      appJarRsrc.setType(LocalResourceType.FILE);
      appJarRsrc.setVisibility(LocalResourceVisibility.PUBLIC);
      try {
        appJarRsrc.setResource(ConverterUtils.getYarnUrlFromURI(new URI(hdfsAppJarLocation)));
      } catch (URISyntaxException e) {
        LOG.error("Error when trying to use appmaster.jar path specified in env"
            + ", path=" + hdfsAppJarLocation);
        e.printStackTrace();
        return;
      }
      appJarRsrc.setTimestamp(hdfsAppJarTimeStamp);
      appJarRsrc.setSize(hdfsAppJarLen);
      localResources.put("AppMaster.jar", appJarRsrc);
      ctx.setLocalResources(localResources);

      // Set the env variables to be setup in the env where the container will be run
      LOG.info("Set the environment for the application master");
      Map<String, String> env = new HashMap<String, String>();
      // Add AppMaster.jar location to classpath. At some point we should not be
      // required to add the hadoop specific classpaths to the env. It should be
      // provided out of the box. For now setting all required classpaths
      // including the classpath to "." for the application jar
      StringBuilder classPathEnv = new StringBuilder("${CLASSPATH}:./*");
      for (String c : conf.getStrings(
          YarnConfiguration.YARN_APPLICATION_CLASSPATH,
          MPIConfiguration.DEFAULT_MPI_APPLICATION_CLASSPATH)) {
        classPathEnv.append(':');
        classPathEnv.append(c.trim());
      }
      env.put("CLASSPATH", classPathEnv.toString());
      env.put("MPIEXECDIR", mpiExecDir);
      ctx.setEnvironment(env);

      // Set the necessary command to execute on the allocated container
      LOG.info("Setting up container command");
      Vector<CharSequence> vargs = new Vector<CharSequence>(5);
      vargs.add("${JAVA_HOME}" + "/bin/java");
      vargs.add("-Xmx" + containerMemory + "m");
      vargs.add("com.taobao.yarn.mpi.server.Container");
      // Add log redirect params
      // FIXME Redirect the output to HDFS
      vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
      vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
      // Commands to be executed
      List<String> commands = new ArrayList<String>();
      // Get final daemonCmd, add spaces to each varg
      StringBuilder containerCmd = new StringBuilder();
      // Set executable command
      for (CharSequence str : vargs) {
        containerCmd.append(str).append(" ");
      }
      commands.add(containerCmd.toString());
      LOG.info("Executing command: " + commands.toString());
      ctx.setCommands(commands);

      StartContainerRequest startReq = Records.newRecord(StartContainerRequest.class);
      startReq.setContainerLaunchContext(ctx);
      try {
        cm.startContainer(startReq);
      } catch (YarnRemoteException e) {
        LOG.info("Start container failed for :"
            + ", containerId=" + container.getId());
        e.printStackTrace();
        // TODO do we need to release this container?
      }
    }
  }

  /**
   * Connect to the Resource Manager
   * @return Handle to communicate with the RM
   */
  private AMRMProtocol connectToRM() {
    YarnConfiguration yarnConf = new YarnConfiguration(conf);
    InetSocketAddress rmAddress = yarnConf.getSocketAddr(
        YarnConfiguration.RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    return ((AMRMProtocol) rpc.getProxy(AMRMProtocol.class, rmAddress, conf));
  }

  /**
   * Register the Application Master to the Resource Manager
   * @return the registration response from the RM
   * @throws YarnRemoteException
   */
  private RegisterApplicationMasterResponse registerToRM() throws YarnRemoteException {
    RegisterApplicationMasterRequest appMasterRequest = Records.newRecord(RegisterApplicationMasterRequest.class);
    // set the required info into the registration request:
    // application attempt id,
    // host on which the app master is running
    // rpc port on which the app master accepts requests from the client
    // tracking url for the app master
    appMasterRequest.setApplicationAttemptId(appAttemptID);
    appMasterRequest.setHost(appMasterHostname);
    appMasterRequest.setRpcPort(appMasterRpcPort);
    appMasterRequest.setTrackingUrl(appMasterTrackingUrl);
    return resourceManager.registerApplicationMaster(appMasterRequest);
  }

  /**
   * Connect to the Resource Manager/Applications Manager
   * @return Handle to communicate with the ASM
   * @throws IOException
   */
  private ClientRMProtocol connectToASM() throws IOException {
    YarnConfiguration yarnConf = new YarnConfiguration(conf);
    InetSocketAddress rmAddress = yarnConf.getSocketAddr(
        YarnConfiguration.RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_PORT);
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    ClientRMProtocol applicationsManager = ((ClientRMProtocol) rpc.getProxy(
        ClientRMProtocol.class, rmAddress, conf));
    return applicationsManager;
  }

  private AMResponse sendEmptyContainerAskToRM()
      throws YarnRemoteException {
    List<ResourceRequest> requestedContainers = new ArrayList<ResourceRequest>();
    AllocateRequest req = Records.newRecord(AllocateRequest.class);
    req.setApplicationAttemptId(appAttemptID);
    req.addAllAsks(requestedContainers);
    req.addAllReleases(new ArrayList<ContainerId>());
    req.setProgress((float)numCompletedContainers.get()/numTotalContainers);

    LOG.info("Sending request to RM for containers"
        + ", requestedSet=" + requestedContainers.size()
        + ", progress=" + req.getProgress());

    for (ResourceRequest  rsrcReq : requestedContainers) {
      LOG.info("Requested container ask: " + rsrcReq.toString());
    }

    AllocateResponse resp = resourceManager.allocate(req);
    return resp.getAMResponse();
  }
}
