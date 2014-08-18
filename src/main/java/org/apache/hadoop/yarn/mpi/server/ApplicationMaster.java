package org.apache.hadoop.yarn.mpi.server;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.mpi.MPIConfiguration;
import org.apache.hadoop.yarn.mpi.MPIConstants;
import org.apache.hadoop.yarn.mpi.server.handler.MPIAMRMAsyncHandler;
import org.apache.hadoop.yarn.mpi.server.handler.MPINMAsyncHandler;
import org.apache.hadoop.yarn.mpi.util.FileSplit;
import org.apache.hadoop.yarn.mpi.util.InputFile;
import org.apache.hadoop.yarn.mpi.util.MPIResult;
import org.apache.hadoop.yarn.mpi.util.Utilities;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class ApplicationMaster extends CompositeService {

  private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);
  // Configuration
  private final Configuration conf;
  // YARN RPC to communicate with the Resource Manager or Node Manager
  private final YarnRPC rpc;
  // Handle to talk to the ResourceManager
  private AMRMClientAsync<ContainerRequest> rmClientAsync = null;
  // Handle to talk to the NodeManager
  private NMClientAsync nmClientAsync = null;
  // Application Attempt Id ( combination of attemptId and fail count )
  private ApplicationAttemptId appAttemptID;
  // Host name of the container, for status update of clients
  private String appMasterHostname = "";
  // Tracking url to which app master publishes info for clients to monitor
  private String appMasterTrackingUrl = "";
  // App Master configuration
  // No. of containers to run shell command on
  private int numTotalContainers = 1;
  // Memory to request for the container on which the shell command will run
  private int containerMemory = 10;
  // Priority of the request
  private int requestPriority;
  // Simple flag to denote whether all works is done
  private final boolean appDone = false;
  // Counter for completed containers ( complete denotes successful or failed )
  private final AtomicInteger numCompletedContainers = new AtomicInteger(0);
  // Count of failed containers
  private final AtomicInteger numFailedContainers = new AtomicInteger();
  // Container Hosts
  private final Set<String> containerHosts = new HashSet<String>();
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
  private Map<String, Integer> hostToProcNum;
  private List<Container> distinctContainers;
  private String phrase = "";
  private int port = 5000;
  // A queue that keep track of the mpi messages
  private final LinkedBlockingQueue<String> mpiMsgs = new LinkedBlockingQueue<String>(
      MPIConstants.MAX_LINE_LOGS);
  // processes per container
  private int ppc = 1;
  // true if all the containers download the same file
  private boolean isAllSame = true;

  private static final String NM_HOST_ENV = "NM_HOST";
  private static final String CONTAINER_ID = ApplicationConstants.Environment.CONTAINER_ID
      .toString();

  // MPI Input Data location
  private final ConcurrentHashMap<String, InputFile> fileToLocation = new ConcurrentHashMap<String, InputFile>();

  // MPI output Data location in the container
  private final ConcurrentHashMap<String, String> fileToDestination = new ConcurrentHashMap<String, String>();

  private final ConcurrentHashMap<String, List<FileStatus>> fileDownloads = new ConcurrentHashMap<String, List<FileStatus>>();

  // MPI result
  private final ConcurrentHashMap<String, MPIResult> resultToDestination = new ConcurrentHashMap<String, MPIResult>();

  // Running Container Status
  private final Map<String, MPDStatus> containerToStatus = new ConcurrentHashMap<String, MPDStatus>();
  // An RPC Service listening the container status
  private MPDListenerImpl mpdListener;
  // mpi clent service including the web application and an RPC Service
  // transfering the logs of the ApplicationMaster
  private MPIClientService clientService;
  // Pull status interval
  private static final int PULL_INTERVAL = 1000;
  // Running App Context for passing to the web interface
  private final AppContext appContext = new RunningAppContext();
  private final FileSystem dfs;
  private MPIAMRMAsyncHandler rmAsyncHandler;
  private MPINMAsyncHandler nmAsyncHandler;

  /**
   * @param args
   *          Command line args
   */
  public static void main(String[] args) {
    boolean result = false;
    ApplicationMaster appMaster = null;
    try {
      appMaster = new ApplicationMaster();
      Utilities.printRelevantParams("Very Beginning of AM", appMaster.conf);
      appMaster.appendMsg("Initializing ApplicationMaster");
      LOG.info("Initializing ApplicationMaster");
      if (!appMaster.parseArgs(args)) {
        System.exit(0);
      }
      result = appMaster.run();
    } catch (Exception e) {
      LOG.fatal("Error running ApplicationMaster", e);
      e.printStackTrace();
      System.exit(1);
    } finally {
      if (appMaster != null) {
        // wait until the mpiMsgs is empty
        while (appMaster.getMpiMsgs().size() > 0) {
          try {
            Thread.sleep(MPIConfiguration.MPI_APPLICATION_WAIT_MESSAGE_EMPTY);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
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
      LOG.info("System env: " + env.getKey() + "=" + env.getValue());
    }
  }

  /**
   * Constructor, connect to Resource Manager
   *
   * @throws IOException
   */
  public ApplicationMaster() throws IOException {
    super(ApplicationMaster.class.getName());
    // Set up the configuration and RPC
    conf = new MPIConfiguration();
    rpc = YarnRPC.create(conf);
    dfs = FileSystem.get(conf);
  }

  /**
   * Parse command line options
   *
   * @param args
   *          Command line args
   * @return Whether init successful and run should be invoked
   * @throws ParseException
   * @throws IOException
   */
  public boolean parseArgs(String[] args) throws ParseException, IOException {

    Options opts = new Options();
    opts.addOption("app_attempt_id", true,
        "App Attempt ID. Not to be used unless for testing purposes");
    opts.addOption("container_memory", true,
        "Amount of memory in MB to be requested to run the shell command");
    opts.addOption("num_containers", true,
        "No. of containers on which the shell command needs to be executed");
    opts.addOption("priority", true, "Application Priority. Default 0");
    opts.addOption("o", "mpi-options", true, "MPI Program Options");
    opts.addOption("debug", false, "Dump out debug information");
    opts.addOption("help", false, "Print usage");
    CommandLine cliParser = new GnuParser().parse(opts, args);

    if (args.length == 0) {
      printUsage(opts);
      throw new IllegalArgumentException(
          "No args specified for application master to initialize");
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
    if (!envs.containsKey(CONTAINER_ID)) {
      // Only for test purpose
      if (cliParser.hasOption("app_attempt_id")) {
        String appIdStr = cliParser.getOptionValue("app_attempt_id", "");
        appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
      } else {
        LOG.error("Application Attempt Id not set in the environment");
        throw new IllegalArgumentException(
            "Application Attempt Id not set in the environment");
      }
    } else {
      org.apache.hadoop.yarn.api.records.ContainerId containerId = ConverterUtils
          .toContainerId(envs.get(CONTAINER_ID));
      appAttemptID = containerId.getApplicationAttemptId();
    }

    LOG.info("Application master for app" + ", appId="
        + appAttemptID.getApplicationId().getId() + ", clustertimestamp="
        + appAttemptID.getApplicationId().getClusterTimestamp()
        + ", attemptId=" + appAttemptID.getAttemptId());

    assert (envs.containsKey(MPIConstants.MPIEXECLOCATION));
    hdfsMPIExecLocation = envs.get(MPIConstants.MPIEXECLOCATION);
    LOG.info("HDFS mpi application location: " + hdfsMPIExecLocation);
    if (envs.containsKey(MPIConstants.MPIEXECTIMESTAMP)) {
      hdfsMPIExecTimestamp = Long.valueOf(envs
          .get(MPIConstants.MPIEXECTIMESTAMP));
    }
    if (envs.containsKey(MPIConstants.MPIEXECLEN)) {
      hdfsMPIExecLen = Long.valueOf(envs.get(MPIConstants.MPIEXECLEN));
    }
    if (!hdfsMPIExecLocation.isEmpty()
        && (hdfsMPIExecTimestamp <= 0 || hdfsMPIExecLen <= 0)) {
      LOG.error("Illegal values in env for shell script path" + ", path="
          + hdfsMPIExecLocation + ", len=" + hdfsMPIExecLen + ", timestamp="
          + hdfsMPIExecTimestamp);
      throw new IllegalArgumentException(
          "Illegal values in env for mpi app path");
    }

    assert (envs.containsKey(MPIConstants.APPJARLOCATION));
    hdfsAppJarLocation = envs.get(MPIConstants.APPJARLOCATION);
    LOG.info("HDFS AppMaster.jar location: " + hdfsAppJarLocation);
    if (envs.containsKey(MPIConstants.APPJARTIMESTAMP)) {
      hdfsAppJarTimeStamp = Long
          .valueOf(envs.get(MPIConstants.APPJARTIMESTAMP));
    }
    if (envs.containsKey(MPIConstants.APPJARLEN)) {
      hdfsAppJarLen = Long.valueOf(envs.get(MPIConstants.APPJARLEN));
    }
    if (!hdfsAppJarLocation.isEmpty()
        && (hdfsAppJarTimeStamp <= 0 || hdfsAppJarLen <= 0)) {
      LOG.error("Illegal values in env for shell script path" + ", path="
          + hdfsAppJarLocation + ", len=" + hdfsAppJarLen + ", timestamp="
          + hdfsAppJarTimeStamp);
      throw new IllegalArgumentException(
          "Illegal values in env for AppMaster.jar path");
    }

    numTotalContainers = Integer.parseInt(cliParser.getOptionValue(
        "num_containers", "1"));

    if (envs.containsKey(MPIConstants.MPIOPTIONS)) {
      mpiOptions = envs.get(MPIConstants.MPIOPTIONS);
      LOG.info("Got extra MPI options: \"" + mpiOptions + "\"");
    }
    String mpiInputs = envs.get(MPIConstants.MPIINPUTS);
    if (!StringUtils.isBlank(mpiInputs)) {
      // TODO hard coding with '@',the regular is same with client
      String[] inputs = StringUtils.split(mpiInputs, "@");
      if (inputs != null && inputs.length > 0) {
        for (String fileName : inputs) {
          InputFile input = new InputFile();
          String inputEvn = envs.get(fileName);
          LOG.debug(String.format("path of %s : %s", fileName, inputEvn));
          if (!StringUtils.isBlank(inputEvn)) {
            String inputSplit[] = StringUtils.split(inputEvn, ";");
            input.setLocation(inputSplit[0]);
            input.setSame(Boolean.valueOf(inputSplit[1]));
            fileToLocation.put(fileName, input);
            Path inputPath = new Path(input.getLocation());
            inputPath = dfs.makeQualified(inputPath);
            List<FileStatus> downLoadFiles = Utilities.listRecursive(inputPath,
                dfs, null);
            fileDownloads.put(fileName, downLoadFiles);
            if (!input.isSame()) {
              if (downLoadFiles != null && downLoadFiles.size() > 0) {
                // if the download mode is same,and the file count is less than
                // container count, we assgin file count to container count
                if (downLoadFiles.size() < numTotalContainers) {
                  numTotalContainers = downLoadFiles.size();
                }
              }
              isAllSame = false;
            }
          }
        }
      }
      LOG.info(String.format("container count:%d", numTotalContainers));

      if (isAllSame) {
        ppc = Integer.parseInt(envs.get(MPIConstants.PROCESSESPERCONTAINER));
      }
    }

    String mpiResults = envs.get(MPIConstants.MPIOUTPUTS);
    if (!StringUtils.isBlank(mpiResults)) {
      // TODO hard coding with '@',the regular is same with client
      String[] results = StringUtils.split(mpiResults, "@");
      if (results != null && results.length > 0) {
        for (String fileName : results) {
          String location = envs.get(fileName);
          MPIResult mResult = new MPIResult();
          mResult.setDfsLocation(location);
          String local = Utilities.getApplicationDir(conf,
              appAttemptID.toString())
              + fileName;
          mResult.setContainerLocal(local);
          resultToDestination.put(fileName, mResult);
          LOG.debug(String.format("path of %s : %s", fileName,
              mResult.toString()));
        }
      }
    }

    if (envs.containsKey(NM_HOST_ENV)) {
      appMasterHostname = envs.get(NM_HOST_ENV);
      LOG.info("Environment " + NM_HOST_ENV + " is " + appMasterHostname);
    }

    mpiExecDir = Utilities.getMpiExecDir(conf, appAttemptID);

    containerMemory = Integer.parseInt(cliParser.getOptionValue(
        "container_memory", "10"));
    LOG.info("Container memory is " + containerMemory + " MB");

    requestPriority = Integer.parseInt(cliParser
        .getOptionValue("priority", "0"));

    phrase = Utilities.getRandomPhrase(16);
    // TODO Port range, max is 65535, min is 5000, should be configurable
    port = appAttemptID.getApplicationId().getId() % 60536 + 5000;

    return true;
  }

  /**
   * Helper function to print usage
   *
   * @param opts
   *          Parsed command line options
   */
  private void printUsage(Options opts) {
    new HelpFormatter().printHelp("ApplicationMaster", opts);
  }

  /**
   * split the downLoadFiles
   *
   * @param downLoadFiles
   * @param fileToLocation
   * @param containerSize
   * @return
   */
  private ConcurrentHashMap<Integer, List<FileSplit>> getFileSplit(
      final ConcurrentHashMap<String, List<FileStatus>> downLoadFiles,
      final ConcurrentHashMap<String, InputFile> fileToLocation,
      final List<Container> containers) {
    int containerSize = containers.size();
    ConcurrentHashMap<Integer, List<FileSplit>> mSplits = new ConcurrentHashMap<Integer, List<FileSplit>>();
    // init the mSplits
    for (Container container : containers) {
      List<FileSplit> fSplits = new ArrayList<FileSplit>();
      mSplits.putIfAbsent(Integer.valueOf(container.getId().getId()), fSplits);
    }
    Set<String> fileKeys = downLoadFiles.keySet();
    Iterator<String> itKeys = fileKeys.iterator();
    while (itKeys.hasNext()) {
      String fileName = itKeys.next();
      List<FileStatus> files = downLoadFiles.get(fileName);
      InputFile inputFile = fileToLocation.get(fileName);
      List<Path> paths = Utilities.convertToPath(files);
      if (inputFile.isSame()) {
        FileSplit sameSplit = new FileSplit();
        sameSplit.setFileName(fileName);
        sameSplit.setSplits(paths);
        for (Container container : containers) {
          Integer containerId = container.getId().getId();
          String downFileName = Utilities.getApplicationDir(conf,
              appAttemptID.toString())
              + fileName;
          fileToDestination.put(fileName, downFileName);
          sameSplit.setDownFileName(downFileName);
          mSplits.get(containerId).add(sameSplit);
        }
      } else {
        ConcurrentHashMap<Integer, ConcurrentHashMap<String, FileSplit>> notSame = new ConcurrentHashMap<Integer, ConcurrentHashMap<String, FileSplit>>();
        for (int i = 0, len = paths.size(); i < len; i++) {
          Integer index = i % containerSize;
          ConcurrentHashMap<String, FileSplit> mapSplit = null;
          Integer containerID = containers.get(index).getId().getId();
          if (notSame.containsKey(containerID)) {
            mapSplit = notSame.get(containerID);
          } else {
            mapSplit = new ConcurrentHashMap<String, FileSplit>();
            notSame.put(containerID, mapSplit);
          }
          if (mapSplit.containsKey(fileName)) {
            mapSplit.get(fileName).addPath(paths.get(i));
          } else {
            FileSplit fsNotSame = new FileSplit();
            fsNotSame.setFileName(fileName);
            List<Path> ps = new ArrayList<Path>();
            ps.add(paths.get(i));
            fsNotSame.setSplits(ps);
            String destination = Utilities.getApplicationDir(conf,
                appAttemptID.toString())
                + fileName;
            fsNotSame.setDownFileName(destination);
            fileToDestination.put(fileName, destination);
            mapSplit.put(fileName, fsNotSame);
          }
        }
        Set<Integer> containerIDS = notSame.keySet();
        Iterator<Integer> itContainer = containerIDS.iterator();
        while (itContainer.hasNext()) {
          Integer key = itContainer.next();
          mSplits.get(key).add(notSame.get(key).get(fileName));
        }
      }
    }

    return mSplits;
  }

  /**
   * Initialize and start RPC services
   */
  public void initAndStartRPCServices() {
    // set the required info into the registration request:
    // host on which the app master is running
    // rpc port on which the app master accepts requests from the client
    // tracking url for the app master
    LOG.info("Creating AM<->RM Protocol...");
    // AMRMClient.createAMRMClient();
    rmAsyncHandler = new MPIAMRMAsyncHandler();
    rmClientAsync = AMRMClientAsync.createAMRMClientAsync(1000, rmAsyncHandler);
    rmClientAsync.init(conf);
    rmClientAsync.start();

    // also init NMClient here
    LOG.info("Creating AM<->NM Protocol...");
    nmAsyncHandler = new MPINMAsyncHandler();
    nmClientAsync = NMClientAsync.createNMClientAsync(nmAsyncHandler);
    nmClientAsync.init(conf);
    nmClientAsync.start();

    LOG.info("Initializing MPDProtocal's RPC services...");
    mpdListener = new MPDListenerImpl();
    mpdListener.init(conf);
    LOG.info("Starting MPDProtocal's RPC services...");
    mpdListener.start();

    LOG.info("Initiallizing MPIClient service and WebApp...");
    clientService = new MPIClientService(appContext);
    clientService.init(conf);

    LOG.info("Starting MPIClient service...");
    clientService.start();
    appMasterTrackingUrl = appMasterHostname + ":"
        + clientService.getHttpPort();
    LOG.info("Application Master tracking url is " + appMasterTrackingUrl);
  }

  /**
   * Main run function for the application master
   *
   * @throws IOException
   */
  public boolean run() throws IOException {
    LOG.info("Starting ApplicationMaster");

    // debug_launch_mpiexec();

    // Connect to ResourceManager

    // TODO Setup local RPC Server to accept status requests directly from
    // clients
    // TODO use the rpc port info to register with the RM for the client to send
    // requests to this app master
    initAndStartRPCServices();

    // Register self with ResourceManager
    try {
      RegisterApplicationMasterResponse response = registerToRM();
      // Dump out information about cluster capability as seen by the
      // resource manager
      int maxMem = response.getMaximumResourceCapability().getMemory();
      // LOG.info("Min mem capabililty of resources in this cluster " + minMem);
      LOG.info("Max mem capabililty of resources in this cluster " + maxMem);
      if (containerMemory > maxMem) {
        LOG.info("Container memory specified above max threshold of cluster. Using max value."
            + ", specified=" + containerMemory + ", max=" + maxMem);
        containerMemory = maxMem;
      }
    } catch (Exception e) {
      LOG.error("Error registering to ResourceManger.");
      e.printStackTrace();
    }

    // Setup heartbeat emitter
    // TODO poll RM every now and then with an empty request to let RM know that
    // we are alive. The heartbeat interval after which an AM is timed out by
    // the
    // RM is defined by a config setting: RM_AM_EXPIRY_INTERVAL_MS with default
    // defined by DEFAULT_RM_AM_EXPIRY_INTERVAL_MS. The allocate calls to the RM
    // count as heartbeats so, for now, this additional heartbeat emitter is not
    // required.
    List<FutureTask<Boolean>> launchResults = new ArrayList<FutureTask<Boolean>>();

    for (int i = 0; i < numTotalContainers; i++) {
      ContainerRequest req = setupContainerAskForRM();
      rmClientAsync.addContainerRequest(req);
    }

    LOG.info("Try to allocate " + numTotalContainers + " containers.");

    int allocateInterval = conf.getInt(MPIConfiguration.MPI_ALLOCATE_INTERVAL,
        1000);
    rmClientAsync.setHeartbeatInterval(allocateInterval);

    while (rmAsyncHandler.getAllocatedContainerNumber() < numTotalContainers) {
      Utilities.sleep(allocateInterval);
    }

    LOG.info(numTotalContainers + " containers allocated.");

    hostToProcNum = rmAsyncHandler.getHostToProcNum();
    distinctContainers = rmAsyncHandler.getDistinctContainers();

    // key(Integer) represent the containerID;value(List<FileSplit>) represent
    // the files which need to be downloaded
    ConcurrentHashMap<Integer, List<FileSplit>> splits = getFileSplit(
        fileDownloads, fileToLocation, distinctContainers);
    for (Container allocatedContainer : distinctContainers) {
      String host = allocatedContainer.getNodeId().getHost();
      containerHosts.add(host);
      LOG.info("Launching command on a new container" + ", containerId="
          + allocatedContainer.getId() + ", containerNode="
          + allocatedContainer.getNodeId().getHost() + ":"
          + allocatedContainer.getNodeId().getPort() + ", containerNodeURI="
          + allocatedContainer.getNodeHttpAddress()
          // + ", containerState" + allocatedContainer.getState()
          + ", containerResourceMemory"
          + allocatedContainer.getResource().getMemory());

      Boolean result = launchContainerAsync(allocatedContainer,
          splits.get(Integer.valueOf(allocatedContainer.getId().getId())),
          resultToDestination.values());

      mpdListener.addContainer(new ContainerId(allocatedContainer.getId()));
    }

    boolean isSuccess = true;
    String diagnostics = null;

    this.appendMsg("all containers are launched successfully");
    LOG.info("all containers are launched successfully");
    try {
      // Wait all daemons starting
      while (!mpdListener.isAllMPDStarted()) {
        Utilities.sleep(PULL_INTERVAL);
      }
      boolean mpiExecSuccess = launchMpiExec();

      // When the application completes, it should send a finish application
      // signal
      // to the RM
      LOG.info("Application completed. Signalling finish to RM");

      if (mpiExecSuccess) {
        isSuccess = true;
      } else {
        isSuccess = false;
        diagnostics = "Diagnostics." + ", total=" + numTotalContainers
            + ", completed=" + numCompletedContainers.get() + ", failed="
            + numFailedContainers.get();
      }
    } catch (Exception e) {
      isSuccess = false;
      LOG.error("error occurs while starting MPD", e);
      e.printStackTrace();
      this.appendMsg("error occurs while starting MPD:"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      diagnostics = e.getMessage();
    }

    unregisterApp(isSuccess ? FinalApplicationStatus.SUCCEEDED
        : FinalApplicationStatus.FAILED, diagnostics);

    return isSuccess;
  }

  /**
   * Encountered some problem at the beginning, this method is used for
   * debugging
   *
   * @throws IOException
   */
  private void debug_launch_mpiexec() throws IOException {
    String pwdPath = System.getenv().get("PWD");
    File pwd = new File(pwdPath);
    if (!pwd.exists()) {
      LOG.info("Current working folder " + pwdPath + " not exist, create it.");
      pwd.mkdirs();
    } else {
      LOG.info("Current working folder " + pwdPath + " still exists.");
    }

    LOG.info("Try to execute mpiexec -launcher ssh -hosts sandking04:2,sandking05:2 /home/hadoop/hadoop-2.4.1/tmp/mpiexecs/appattempt_1408272055127_0008_000001/MPIExec");
    String[] params = {
        "mpiexec",
        "-launcher",
        "ssh",
        "-hosts",
        "sandking04:2,sandking05:2",
    "/home/hadoop/hadoop-2.4.1/tmp/mpiexecs/appattempt_1408272055127_0008_000001/MPIExec" };
    String[] envp = { "PATH=/home/hadoop/hadoop-2.4.1/bin:/home/hadoop/hadoop-2.4.1/sbin:/home/hadoop/jdk1.7.0_25/bin:/home/hadoop/jdk1.7.0_25/jre/bin:/home/hadoop/protobuf-2.5.0/bin:/home/hadoop/mpich-3.1.2/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games" };
    File file = new File("/home/hadoop");
    final Process proc = Runtime.getRuntime().exec(params, null, file);
    Thread stdinThread = new Thread(new Runnable() {
      @Override
      public void run() {
        Scanner pcStdout = new Scanner(proc.getInputStream());
        while (pcStdout.hasNextLine()) {
          String line = "[stdout] " + pcStdout.nextLine();
          LOG.info(line);
          appendMsg(line);
        }
      }
    });
    stdinThread.start();

    Thread stderrThread = new Thread(new Runnable() {
      @Override
      public void run() {
        Scanner pcStderr = new Scanner(proc.getErrorStream());
        while (pcStderr.hasNextLine()) {
          String line = "[stderr] " + pcStderr.nextLine();
          LOG.info(line);
          appendMsg(line);
        }
      }
    });
    stderrThread.start();
    try {
      LOG.info("Returned " + proc.waitFor());
    } catch (InterruptedException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
  }

  /**
   * Setup the request that will be sent to the RM for the container ask.
   *
   * @return the setup ResourceRequest to be sent to RM
   */
  private ContainerRequest setupContainerAskForRM() {
    // setup requirements for hosts
    // using * as any host will do for the distributed shell app
    // set the priority for the request
    Priority pri = Records.newRecord(Priority.class);
    // TODO - what is the range for priority? how to decide?
    pri.setPriority(requestPriority);

    // Set up resource type requirements
    // For now, memory and CPU are supported so we set memory and cpu
    // requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(containerMemory);
    capability.setVirtualCores(1);

    ContainerRequest request = new ContainerRequest(capability, null, null, pri);
    LOG.info("Requested container ask: " + request.toString());
    return request;
  }

  /**
   * Get the http port of the application master
   *
   * @return the http port
   */
  public int getHttpPort() {
    return clientService.getHttpPort();
  }

  public BlockingQueue<String> getMpiMsgs() {
    return mpiMsgs;
  }

  public void appendMsg(String message) {
    Boolean flag = mpiMsgs.offer(message);
    if (!flag) {
      LOG.warn("message queue is full");
    }
  }

  /**
   * Unregister this ApplicationMaster to RM.
   */
  private void unregisterApp(FinalApplicationStatus status, String diagnostics) {
    try {
      LOG.info("Stop all containers");
      for (Container container : rmAsyncHandler.getAcquiredContainers()) {
        nmClientAsync.stopContainerAsync(container.getId(),
            container.getNodeId());
      }
      LOG.info("Unregister AM, and wait for services to stop.");
      rmClientAsync.unregisterApplicationMaster(status, diagnostics,
          appMasterTrackingUrl);
      rmClientAsync.stop();
      nmClientAsync.stop();
      LOG.info("AMRM, NM two services stopped");
    } catch (Exception e) {
      LOG.error("Error unregistering AM.");
      e.printStackTrace();
    }
  }

  /**
   * Application Master launches "mpiexec" process locally
   *
   * @return Whether the MPI executable successfully terminated
   * @throws IOException
   *
   */
  private boolean launchMpiExec() throws IOException {
    LOG.info("Launching mpiexec from the Application Master...");

    StringBuilder commandBuilder = new StringBuilder(
        "mpiexec -launcher ssh -hosts ");
    Set<String> hosts = hostToProcNum.keySet();
    boolean first = true;
    for (String host : hosts) {
      if (first) {
        first = false;
      } else {
        commandBuilder.append(",");
      }
      commandBuilder.append(host);
      commandBuilder.append(":");
      commandBuilder.append(hostToProcNum.get(host));
    }

    commandBuilder.append(" ");
    commandBuilder.append(mpiExecDir);
    commandBuilder.append("/MPIExec");
    if (!mpiOptions.isEmpty()) {
      commandBuilder.append(" ");
      // replace the fileName with the hdfs path
      Set<String> fileNames = fileToDestination.keySet();
      Iterator<String> itNames = fileNames.iterator();
      while (itNames.hasNext()) {
        String fileName = itNames.next();
        mpiOptions = mpiOptions.replaceAll(fileName,
            this.fileToDestination.get(fileName));
      }
      // replace the result with container local location
      Set<String> resultNames = resultToDestination.keySet();
      Iterator<String> itResult = resultNames.iterator();
      while (itResult.hasNext()) {
        String resultName = itResult.next();
        mpiOptions = mpiOptions.replaceAll(resultName,
            resultToDestination.get(resultName).getContainerLocal());
      }
      LOG.info(String.format("mpi options:", mpiOptions));

      commandBuilder.append(mpiOptions);
    }
    LOG.info("Executing command:" + commandBuilder.toString());
    Utilities.printRelevantParams("Before Launching", conf);
    File mpiPWD = new File(mpiExecDir);
    Runtime rt = Runtime.getRuntime();
    final Process pc = rt.exec(commandBuilder.toString(), null, mpiPWD);

    Thread stdinThread = new Thread(new Runnable() {
      @Override
      public void run() {
        Scanner pcStdout = new Scanner(pc.getInputStream());
        while (pcStdout.hasNextLine()) {
          String line = "[stdout] " + pcStdout.nextLine();
          LOG.info(line);
          appendMsg(line);
        }
      }
    });
    stdinThread.start();

    Thread stderrThread = new Thread(new Runnable() {
      @Override
      public void run() {
        Scanner pcStderr = new Scanner(pc.getErrorStream());
        while (pcStderr.hasNextLine()) {
          String line = "[stderr] " + pcStderr.nextLine();
          LOG.info(line);
          appendMsg(line);
        }
      }
    });
    stderrThread.start();

    try {
      int ret = pc.waitFor();
      LOG.info("MPI Process returned with value: " + ret);
      LOG.info("Shutting down daemons...");
      for (String host : containerHosts) {

      }
      LOG.info("All daemons shut down! :-D");
      if (ret != 0) {
        return false;
      } else {
        return true;
      }
    } catch (InterruptedException e) {
      LOG.error("mpiexec Thread is nterruptted!", e);
    }
    return false;
  }

  /**
   * Async Method telling NMClientAsync to launch specific container
   *
   * @param container
   * @param fileSplits
   * @param results
   * @return
   */
  public Boolean launchContainerAsync(Container container,
      List<FileSplit> fileSplits, Collection<MPIResult> results) {

    LOG.info("Setting up container launch container for containerid="
        + container.getId());

    /*
     * String jobUserName =
     * System.getenv(ApplicationConstants.Environment.USER.name());
     * ctx.setUser(jobUserName);
     * LOG.info("Setting user in ContainerLaunchContext to: " + jobUserName);
     */

    // Set the local resources for each container
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    assert (!hdfsMPIExecLocation.isEmpty());
    LocalResource mpiRsrc = Records.newRecord(LocalResource.class);
    mpiRsrc.setType(LocalResourceType.FILE);
    mpiRsrc.setVisibility(LocalResourceVisibility.PUBLIC);
    try {
      mpiRsrc.setResource(ConverterUtils.getYarnUrlFromURI(new URI(
          hdfsMPIExecLocation)));
    } catch (URISyntaxException e) {
      LOG.error("Error when trying to use mpi application path specified in env"
          + ", path=" + hdfsMPIExecLocation);
      e.printStackTrace();
      return false;
    }
    mpiRsrc.setTimestamp(hdfsMPIExecTimestamp);
    mpiRsrc.setSize(hdfsMPIExecLen);
    localResources.put("MPIExec", mpiRsrc);
    assert (!hdfsAppJarLocation.isEmpty());
    LocalResource appJarRsrc = Records.newRecord(LocalResource.class);
    appJarRsrc.setType(LocalResourceType.FILE);
    appJarRsrc.setVisibility(LocalResourceVisibility.PUBLIC);
    try {
      appJarRsrc.setResource(ConverterUtils.getYarnUrlFromURI(new URI(
          hdfsAppJarLocation)));
    } catch (URISyntaxException e) {
      LOG.error("Error when trying to use appmaster.jar path specified in env"
          + ", path=" + hdfsAppJarLocation);
      e.printStackTrace();
      return false;
    }
    appJarRsrc.setTimestamp(hdfsAppJarTimeStamp);
    appJarRsrc.setSize(hdfsAppJarLen);
    localResources.put("AppMaster.jar", appJarRsrc);

    // Set the env variables to be setup in the env where the container will be
    // run
    LOG.info("Set the environment for the application master");
    Map<String, String> env = new HashMap<String, String>();

    env.put("CLASSPATH", System.getenv("CLASSPATH"));
    env.put("MPIEXECDIR", mpiExecDir);

    env.put(MPIConstants.CONTAININPUT, Utilities.encodeSplit(fileSplits));
    env.put(MPIConstants.APPATTEMPTID, appAttemptID.toString());
    env.put(MPIConstants.CONTAINOUTPUT, Utilities.encodeMPIResult(results));

    env.put("CONTAINER_ID", String.valueOf(container.getId().getId()));
    env.put("APPMASTER_HOST", System.getenv(NM_HOST_ENV));
    env.put("APPMASTER_PORT", String.valueOf(mpdListener.getServerPort()));

    env.put("PATH", System.getenv("PATH"));

    containerToStatus.put(container.getId().toString(), MPDStatus.UNDEFINED);
    // Set the necessary command to execute on the allocated container
    LOG.info("Setting up container command");
    Vector<CharSequence> vargs = new Vector<CharSequence>(5);
    vargs.add("${JAVA_HOME}" + "/bin/java");
    vargs.add("-Xmx" + containerMemory + "m");
    // log are specified by the nodeManager's container-log4j.properties and
    // nodemanager can specify the MPI_AM_LOG_LEVEL and MPI_AM_LOG_SIZE
    /*
     * String logLevel = conf.get(MPIConfiguration.MPI_CONTAINER_LOG_LEVEL,
     * MPIConfiguration.DEFAULT_MPI_CONTAINER_LOG_LEVEL); long logSize =
     * conf.getLong(MPIConfiguration.MPI_CONTAINER_LOG_SIZE,
     * MPIConfiguration.DEFAULT_MPI_CONTAINER_LOG_SIZE);
     * Utilities.addLog4jSystemProperties(logLevel, logSize, vargs);
     */
    String javaOpts = conf.get(
        MPIConfiguration.MPI_CONTAINER_JAVA_OPTS_EXCEPT_MEMORY, "");
    if (!StringUtils.isBlank(javaOpts)) {
      vargs.add(javaOpts);
    }
    vargs.add("org.apache.hadoop.yarn.mpi.server.Container");
    vargs.add("-p " + port);
    vargs.add("-f " + phrase);
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

    ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
        localResources, env, commands, null, null, null);
    // ctx.setTokens(UserGroupInformation.getCurrentUser().getCredentials().getAllTokens().duplicate());

    /*
     * StartContainerRequest startReq =
     * Records.newRecord(StartContainerRequest.class);
     * startReq.setContainerLaunchContext(ctx); List<StartContainerRequest>
     * startReqList = new ArrayList<StartContainerRequest>();
     * startReqList.add(startReq); StartContainersRequest startReqs =
     * StartContainersRequest.newInstance(startReqList);
     */

    try {
      nmClientAsync.startContainerAsync(container, ctx);
    } catch (Exception e) {
      LOG.error(
          "Start container failed for :" + ", containerId=" + container.getId(),
          e);
      e.printStackTrace();
      return false;
    }
    return true;
  }

  /**
   * Register the Application Master to the Resource Manager
   *
   * @return the registration response from the RM
   * @throws YarnException
   *           , IOException
   */
  private RegisterApplicationMasterResponse registerToRM()
      throws YarnException, IOException {
    return rmClientAsync.registerApplicationMaster(clientService
        .getBindAddress().getHostName(), clientService.getBindAddress()
        .getPort(), appMasterTrackingUrl);
  }

  /**
   * Internal class for running application class
   */
  private class RunningAppContext implements AppContext {

    @Override
    public ApplicationId getApplicationID() {
      return appAttemptID.getApplicationId();
    }

    @Override
    public ApplicationAttemptId getApplicationAttemptId() {
      return appAttemptID;
    }

    @Override
    public List<Container> getAllContainers() {
      return distinctContainers;
    }

    @Override
    public LinkedBlockingQueue<String> getMpiMsgQueue() {
      return mpiMsgs;
    }
  }
}
