package com.taobao.yarn.mpi.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
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
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
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
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import com.taobao.yarn.mpi.MPIConfiguration;
import com.taobao.yarn.mpi.MPIConstants;
import com.taobao.yarn.mpi.allocator.ContainersAllocator;
import com.taobao.yarn.mpi.allocator.DistinctContainersAllocator;
import com.taobao.yarn.mpi.allocator.MultiMPIProcContainersAllocator;
import com.taobao.yarn.mpi.util.FileSplit;
import com.taobao.yarn.mpi.util.InputFile;
//import com.taobao.yarn.mpi.util.LOG;
import com.taobao.yarn.mpi.util.MPDException;
import com.taobao.yarn.mpi.util.MPIResult;
import com.taobao.yarn.mpi.util.Utilities;

public class ApplicationMaster extends CompositeService {

  private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);
  // Configuration
  private final Configuration conf;
  // YARN RPC to communicate with the Resource Manager or Node Manager
  private final YarnRPC rpc;
  // Handle to talk to the ResourceManager
  private AMRMClient rmClient = null;
  // Handle to talk to the NodeManager
  private NMClient nmClient = null;
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
  private boolean appDone = false;
  // Counter for completed containers ( complete denotes successful or failed )
  private final AtomicInteger numCompletedContainers = new AtomicInteger();
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
  private List<Container> allContainers;
  private String phrase = "";
  private int port = 5000;
  // A queue that keep track of the mpi messages
  private final LinkedBlockingQueue<String> mpiMsgs = new LinkedBlockingQueue<String>(MPIConstants.MAX_LINE_LOGS);
  //processes per container
  private int ppc = 1;
  // true if all the containers download the same file
  private boolean isAllSame = true;

  private static final String NM_HOST_ENV = "NM_HOST";
  private static final String CONTAINER_ID =
      ApplicationConstants.Environment.CONTAINER_ID.toString();

  //MPI Input Data location
  private final ConcurrentHashMap<String, InputFile> fileToLocation = new ConcurrentHashMap<String, InputFile>();

  //MPI output Data location in the container
  private final ConcurrentHashMap<String, String> fileToDestination = new ConcurrentHashMap<String, String>();

  private final ConcurrentHashMap<String, List<FileStatus>> fileDownloads = new ConcurrentHashMap<String, List<FileStatus>>();

  //MPI result
  private final ConcurrentHashMap<String, MPIResult> resultToDestination = new ConcurrentHashMap<String, MPIResult>();

  // Running Container Status
  private final Map<String, MPDStatus> containerToStatus = new ConcurrentHashMap<String, MPDStatus>();
  // An RPC Service listening the container status
  private MPDListenerImpl mpdListener;
  // mpi clent service including the web application and an RPC Service transfering the logs of the ApplicationMaster
  private MPIClientService clientService;
  // Pull status interval
  private static final int PULL_INTERVAL = 1000;
  // Running App Context for passing to the web interface
  private final AppContext appContext = new RunningAppContext();
  private final FileSystem dfs;

  /**
   * @param args Command line args
   */
  public static void main(String[] args) {
    boolean result = false;
    ApplicationMaster appMaster = null;
    try {
      appMaster = new ApplicationMaster();
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
    }finally{
      if (appMaster != null) {
        //wait until the mpiMsgs is empty
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
   * @param args Command line args
   * @return Whether init successful and run should be invoked
   * @throws ParseException
   * @throws IOException
   */
  public boolean parseArgs(String[] args) throws ParseException, IOException {

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
    if (!envs.containsKey(CONTAINER_ID)) {
      // Only for test purpose
      if (cliParser.hasOption("app_attempt_id")) {
        String appIdStr = cliParser.getOptionValue("app_attempt_id", "");
        appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
      } else {
        LOG.error("Application Attempt Id not set in the environment");
        throw new IllegalArgumentException("Application Attempt Id not set in the environment");
      }
    } else {
      ContainerId containerId =
          ConverterUtils.toContainerId(envs.get(CONTAINER_ID));
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

    numTotalContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));

    if (envs.containsKey(MPIConstants.MPIOPTIONS)) {
      mpiOptions = envs.get(MPIConstants.MPIOPTIONS);
      LOG.info("Got extra MPI options: \"" + mpiOptions + "\"");
    }
    String mpiInputs = envs.get(MPIConstants.MPIINPUTS);
    if (!StringUtils.isBlank(mpiInputs)) {
      //TODO hard coding with '@',the regular is same with client
      String []inputs = StringUtils.split(mpiInputs, "@");
      if (inputs != null && inputs.length > 0) {
        for (String fileName : inputs ) {
          InputFile input = new InputFile();
          String inputEvn = envs.get(fileName);
          LOG.debug(String.format("path of %s : %s", fileName, inputEvn));
          if(!StringUtils.isBlank(inputEvn)) {
            String inputSplit[] = StringUtils.split(inputEvn,";");
            input.setLocation(inputSplit[0]);
            input.setSame(Boolean.valueOf(inputSplit[1]));
            fileToLocation.put(fileName, input);
            Path inputPath = new Path(input.getLocation());
            inputPath = dfs.makeQualified(inputPath);
            List<FileStatus> downLoadFiles = Utilities.listRecursive(inputPath, dfs, null);
            fileDownloads.put(fileName, downLoadFiles);
            if (!input.isSame()) {
              if (downLoadFiles != null && downLoadFiles.size() > 0) {
                //if the download mode is same,and the file count is less than container count, we assgin file count to container count
                if(downLoadFiles.size() < numTotalContainers){
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
      //TODO hard coding with '@',the regular is same with client
      String []results = StringUtils.split(mpiResults, "@");
      if (results != null && results.length > 0) {
        for (String fileName : results) {
          String location = envs.get(fileName);
          MPIResult mResult = new MPIResult();
          mResult.setDfsLocation(location);
          String local = Utilities.getApplicationDir(conf, appAttemptID.toString()) + fileName;
          mResult.setContainerLocal(local);
          resultToDestination.put(fileName, mResult);
          LOG.debug(String.format("path of %s : %s", fileName, mResult.toString()));
        }
      }
    }

    if (envs.containsKey(NM_HOST_ENV)) {
      appMasterHostname = envs.get(NM_HOST_ENV);
      LOG.info("Environment " + NM_HOST_ENV + " is " + appMasterHostname);
    }

    mpiExecDir = Utilities.getMpiExecDir(conf, appAttemptID);

    containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));
    LOG.info("Container memory is " + containerMemory + " MB");

    requestPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));

    phrase = Utilities.getRandomPhrase(16);
    // TODO Port range, max is 65535, min is 5000, should be configurable
    port = appAttemptID.getApplicationId().getId() % 60536 + 5000;

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
   * split the downLoadFiles
   * @param downLoadFiles
   * @param fileToLocation
   * @param containerSize
   * @return
   */
  private ConcurrentHashMap<Integer,List<FileSplit>> getFileSplit(final ConcurrentHashMap<String, List<FileStatus>> downLoadFiles,
      final ConcurrentHashMap<String, InputFile> fileToLocation, final List<Container> containers){
    int containerSize = containers.size();
    ConcurrentHashMap<Integer, List<FileSplit>> mSplits = new ConcurrentHashMap<Integer, List<FileSplit>>();
    //init the mSplits
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
          String downFileName = Utilities.getApplicationDir(conf, appAttemptID.toString()) + fileName;
          fileToDestination.put(fileName, downFileName);
          sameSplit.setDownFileName(downFileName);
          mSplits.get(containerId).add(sameSplit);
        }
      }else {
        ConcurrentHashMap<Integer, ConcurrentHashMap<String, FileSplit>> notSame = new  ConcurrentHashMap<Integer, ConcurrentHashMap<String, FileSplit>>();
        for (int i = 0, len = paths.size(); i < len; i++) {
          Integer index = i % containerSize;
          ConcurrentHashMap<String, FileSplit> mapSplit = null;
          Integer containerID = containers.get(index).getId().getId();
          if (notSame.containsKey(containerID)){
            mapSplit = notSame.get(containerID);
          }else {
            mapSplit = new ConcurrentHashMap<String, FileSplit>();
            notSame.put(containerID, mapSplit);
          }
          if (mapSplit.containsKey(fileName)) {
            mapSplit.get(fileName).addPath(paths.get(i));
          }else {
            FileSplit fsNotSame = new FileSplit();
            fsNotSame.setFileName(fileName);
            List<Path> ps = new ArrayList<Path>();
            ps.add(paths.get(i));
            fsNotSame.setSplits(ps);
            String destination = Utilities.getApplicationDir(conf, appAttemptID.toString()) + fileName;
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
    rmClient = AMRMClient.createAMRMClient();
    rmClient.init(conf);
    rmClient.start();

    // also init NMClient here
    LOG.info("Creating AM<->NM Protocol...");
    nmClient = NMClient.createNMClient();
    nmClient.init(conf);
    nmClient.start();

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
    appMasterTrackingUrl = appMasterHostname + ":" + clientService.getHttpPort();
    LOG.info("Application Master tracking url is " + appMasterTrackingUrl);
  }

  /**
   * Main run function for the application master
   * @throws IOException
   */
  public boolean run() throws IOException {
    LOG.info("Starting ApplicationMaster");
    // Connect to ResourceManager

    // TODO Setup local RPC Server to accept status requests directly from clients
    // TODO use the rpc port info to register with the RM for the client to send requests to this app master
    initAndStartRPCServices();

    // Register self with ResourceManager
    try {
      RegisterApplicationMasterResponse response = registerToRM();
      // Dump out information about cluster capability as seen by the
      // resource manager
      int maxMem = response.getMaximumResourceCapability().getMemory();
      //LOG.info("Min mem capabililty of resources in this cluster " + minMem);
      LOG.info("Max mem capabililty of resources in this cluster " + maxMem);
      if (containerMemory > maxMem) {
        LOG.info("Container memory specified above max threshold of cluster. Using max value."
            + ", specified=" + containerMemory
            + ", max=" + maxMem);
        containerMemory = maxMem;
      }
    } catch (Exception e) {
      LOG.error("Error registering to ResourceManger.");
      e.printStackTrace();
    }

    // Setup heartbeat emitter
    // TODO poll RM every now and then with an empty request to let RM know that
    // we are alive. The heartbeat interval after which an AM is timed out by the
    // RM is defined by a config setting: RM_AM_EXPIRY_INTERVAL_MS with default
    // defined by DEFAULT_RM_AM_EXPIRY_INTERVAL_MS. The allocate calls to the RM
    // count as heartbeats so, for now, this additional heartbeat emitter is not required.
    List<FutureTask<Boolean>> launchResults = new ArrayList<FutureTask<Boolean>>();
    ContainersAllocator allocator = null;
    try {
      allocator = createContainersAllocator();
      allContainers = allocator.allocateContainers(numTotalContainers);
    } catch (Exception e) {
      LOG.error("Error allocating containers.");
      e.printStackTrace();
    }
    numTotalContainers = allContainers.size();
    LOG.info(numTotalContainers + " containers allocated.");
    // TODO Available where we use MultiMPIProcContainersAllocator strategy
    hostToProcNum = allocator.getHostToProcNum();
    AtomicInteger rmRequestID = new AtomicInteger(allocator.getCurrentRequestId());
    //key(Integer) represent the containerID;value(List<FileSplit>) represent the files which need to be downloaded
    ConcurrentHashMap<Integer,List<FileSplit>> splits = getFileSplit(fileDownloads, fileToLocation, allContainers);
    for (Container allocatedContainer : allContainers) {
      String host = allocatedContainer.getNodeId().getHost();
      containerHosts.add(host);
      LOG.info("Launching command on a new container"
          + ", containerId=" + allocatedContainer.getId()
          + ", containerNode=" + allocatedContainer.getNodeId().getHost()
          + ":" + allocatedContainer.getNodeId().getPort()
          + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
          //+ ", containerState" + allocatedContainer.getState()
          + ", containerResourceMemory"
          + allocatedContainer.getResource().getMemory());
      LaunchContainer runnableLaunchContainer = new LaunchContainer(
          allocatedContainer, nmClient,
          splits.get(Integer.valueOf(allocatedContainer.getId().getId())),
          resultToDestination.values());
      LOG.info("Created LaunchContainer.");
      // launch and start the container on a separate thread to keep the
      // main thread unblocked as all containers may not
      // be allocated/launched at one go.
      FutureTask<Boolean> launchTask =
          new FutureTask<Boolean>(runnableLaunchContainer);
      LOG.info("Created LaunchTask.");
      Thread launchThread = new Thread(launchTask);
      LOG.info("Created Thread.");
      launchThread.start();
      LOG.info("Thread started.");
      launchResults.add(launchTask);
      LOG.info("Added launch result.");
      mpdListener.addContainer(allocatedContainer.getId().getId());
    }

    Boolean allLaunchSuccess = true;
    try {
      for (FutureTask<Boolean> taskResult : launchResults) {
        allLaunchSuccess = taskResult.get();
      }
    } catch (Exception e) {
      allLaunchSuccess = false;
      LOG.error("launch error:", e);
      this.appendMsg(org.apache.hadoop.util.StringUtils.stringifyException(e));
    }

    boolean isSuccess = true;
    String diagnostics = null;
    //all tasks are launched successfully
    if (allLaunchSuccess) {
      this.appendMsg("all containers are launched successfully");
      LOG.info("all containers are launched successfully");
      try {
        // Wait all SMPD for staring
        while (!mpdListener.isAllMPDStarted()) {
          Utilities.sleep(PULL_INTERVAL);
        }
        launchMpiExec();
        int loopCounter = -1;
        while (numCompletedContainers.get() < numTotalContainers
            && !appDone) {
          loopCounter++;
          Utilities.sleep(PULL_INTERVAL);
          //check whether all the smpd process is healthy
          boolean allHealthy = mpdListener.isAllHealthy();
          if (allHealthy) {
            LOG.debug("Sending empty request to RM, to let RM know we are " + 
                      "alive");
            AllocateResponse amResp = rmClient.allocate(
                (float) numCompletedContainers.get() / numTotalContainers);

            // Check what the current available resources in the cluster are
            // TODO should we do anything if the available resources are not
            // enough?
            Resource availableResources = amResp.getAvailableResources();
            if (LOG.isDebugEnabled()) {
              LOG.debug("Current available resources in the cluster " + availableResources);
            }

            // Check the completed containers
            List<ContainerStatus> completedContainers = amResp.getCompletedContainersStatuses();
            if (LOG.isDebugEnabled()) {
              LOG.debug("Got response from RM for container ask, completedCnt=" + completedContainers.size());
            }
            for (ContainerStatus containerStatus : completedContainers) {
              LOG.info("Got container status for containerID= " + containerStatus.getContainerId()
                  //+ ", state=" + containerStatus.getState()
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

            if (LOG.isDebugEnabled()) {
              LOG.debug("Current application state: loop=" + loopCounter
                  + ", appDone=" + appDone
                  + ", total=" + numTotalContainers
                  + ", completed=" + numCompletedContainers
                  + ", failed=" + numFailedContainers);
            }
            // TODO Add a timeout handling layer, for misbehaving mpi application
          }
        }  // end while
        // When the application completes, it should send a finish application signal
        // to the RM
        LOG.info("Application completed. Signalling finish to RM");

        if (numFailedContainers.get() == 0) {
          isSuccess = true;
        } else {
          isSuccess = false;
          diagnostics = "Diagnostics."
              + ", total=" + numTotalContainers
              + ", completed=" + numCompletedContainers.get()
              + ", failed=" + numFailedContainers.get();
        }
      } catch (Exception e) {
        isSuccess=false;
        LOG.error("error occurs while starting MPD", e);
        e.printStackTrace();
        this.appendMsg("error occurs while starting MPD:" + org.apache.hadoop.util.StringUtils.stringifyException(e));
        diagnostics = e.getMessage();
      }
    } else {
      //the applicationMaster finish with failure and realease the containers
      isSuccess=false;
      diagnostics = "Error occurs while launching containers.";
    }


    unregisterApp(isSuccess ? FinalApplicationStatus.SUCCEEDED
                            : FinalApplicationStatus.FAILED, diagnostics);

    return isSuccess;
  }

  /**
   * Get the http port of the application master
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

  /**Unregister this ApplicationMaster to RM.
   */
  private void unregisterApp(FinalApplicationStatus status,
      String diagnostics) {
    try { 
      rmClient.unregisterApplicationMaster(status, diagnostics,
          appMasterTrackingUrl );
    } catch (Exception e) {
      LOG.error("Error unregistering AM.");
      e.printStackTrace();
    }
  }

  /**
   * Application Master launches "mpiexec" process locally
   * @throws IOException
   */
  private void launchMpiExec() throws IOException {
    LOG.info("Launching mpiexec from the Application Master...");

    StringBuilder commandBuilder = new StringBuilder("mpiexec -phrase ");
    commandBuilder.append(phrase);
    commandBuilder.append(" -port ");
    commandBuilder.append(port);
    commandBuilder.append(" -hosts ");
    commandBuilder.append(allContainers.size());
    for (Container container : allContainers) {
      String host = container.getNodeId().getHost();
      commandBuilder.append(" ");
      commandBuilder.append(host);
      commandBuilder.append(" ");
      commandBuilder.append(ppc);
    }
    commandBuilder.append(" ");
    commandBuilder.append(mpiExecDir);
    commandBuilder.append("/MPIExec");
    if (!mpiOptions.isEmpty()) {
      commandBuilder.append(" ");
      //replace the fileName with the hdfs path
      Set<String> fileNames = fileToDestination.keySet();
      Iterator<String> itNames = fileNames.iterator();
      while (itNames.hasNext()) {
        String fileName = itNames.next();
        mpiOptions=mpiOptions.replaceAll(fileName, this.fileToDestination.get(fileName));
      }
      //replace the result with container local location
      Set<String> resultNames = resultToDestination.keySet();
      Iterator<String> itResult = resultNames.iterator();
      while (itResult.hasNext()) {
        String resultName = itResult.next();
        mpiOptions=mpiOptions.replaceAll(resultName, resultToDestination.get(resultName).getContainerLocal());
      }
      LOG.info(String.format("mpi options:", mpiOptions));

      commandBuilder.append(mpiOptions);
    }
    LOG.info("Executing command:" + commandBuilder.toString());
    Runtime rt = Runtime.getRuntime();

    String[] envStrs = generateEnvStrs();
    final Process pc = rt.exec(commandBuilder.toString(), envStrs);

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

    Thread shutdownThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          int ret = pc.waitFor();
          LOG.info("MPI Process returned with value: " + ret);
          LOG.info("Shutting down daemons...");
          Runtime rt = Runtime.getRuntime();
          for (String host : containerHosts) {
            try {
              String command = "smpd -shutdown " + host + " -phrase " + phrase + " -port " + port + " -yarn";
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
          LOG.error("mpiexec Thread is nterruptted!", e);
        }
      }
    });
    shutdownThread.start();
  }

  private String[] generateEnvStrs() {
    String userHomeDir = conf.get(MPIConfiguration.MPI_NM_STARTUP_USERDIR, "/home/hadoop");
    Map<String, String> envs = System.getenv();
    String[] envStrs = new String[envs.size()];
    int i = 0;
    for (Entry<String, String> env : envs.entrySet()) {
      if (env.getKey().equals("HOME")) {
        envStrs[i] = env.getKey() + "=" + userHomeDir;
      } else {
        envStrs[i] = env.getKey() + "=" + env.getValue();
      }
      i++;
    }
    return envStrs;
  }

  /**
   * Thread to connect to the {@link ContainerManagementProtocol} and
   * launch the container that will execute the shell command.
   */
  private class LaunchContainer implements Callable<Boolean> {

    Log LOG = LogFactory.getLog(LaunchContainer.class);
    // Allocated container
    Container container;
    // Handle to communicate with ResourceManager
    AMRMClient rmClient = null;
    // Handle to communicate with NodeManager
    NMClient nmClient = null;

    //files needing to download
    private final List<FileSplit> fileSplits;
    //mpi results needing to be uploaded to hdfs
    private final Collection<MPIResult> results;

    /**
     * @param lcontainer Allocated container
     */
    public LaunchContainer(Container lcontainer, NMClient client,
        List<FileSplit> fileSplits, Collection<MPIResult> results) {
      this.container = lcontainer;
      this.nmClient = client;
      this.fileSplits =  fileSplits;
      this.results = results;
    }

    /**
     * Connects to CM, sets up container launch context
     * for shell command and eventually dispatches the container
     * start request to the CM.
     */
    @Override
    public Boolean call() {

      LOG.info("Setting up container launch container for containerid=" + container.getId());

      /*
      String jobUserName = System.getenv(ApplicationConstants.Environment.USER.name());
      ctx.setUser(jobUserName);
      LOG.info("Setting user in ContainerLaunchContext to: " + jobUserName);
      */

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
        return false;
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
        return false;
      }
      appJarRsrc.setTimestamp(hdfsAppJarTimeStamp);
      appJarRsrc.setSize(hdfsAppJarLen);
      localResources.put("AppMaster.jar", appJarRsrc);

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

      env.put(MPIConstants.CONTAININPUT, Utilities.encodeSplit(fileSplits));
      env.put(MPIConstants.APPATTEMPTID, appAttemptID.toString());
      env.put(MPIConstants.CONTAINOUTPUT, Utilities.encodeMPIResult(results));

      env.put("CONTAINER_ID", String.valueOf(container.getId().getId()));
      env.put("APPMASTER_HOST", System.getenv(NM_HOST_ENV));
      env.put("APPMASTER_PORT", String.valueOf(mpdListener.getServerPort()));

      containerToStatus.put(container.getId().toString(), MPDStatus.UNDEFINED);
      // Set the necessary command to execute on the allocated container
      LOG.info("Setting up container command");
      Vector<CharSequence> vargs = new Vector<CharSequence>(5);
      vargs.add("${JAVA_HOME}" + "/bin/java");
      vargs.add("-Xmx" + containerMemory + "m");
      // log are specified by the nodeManager's container-log4j.properties and nodemanager can specify the MPI_AM_LOG_LEVEL and MPI_AM_LOG_SIZE
      /*
      String logLevel = conf.get(MPIConfiguration.MPI_CONTAINER_LOG_LEVEL, MPIConfiguration.DEFAULT_MPI_CONTAINER_LOG_LEVEL);
      long logSize = conf.getLong(MPIConfiguration.MPI_CONTAINER_LOG_SIZE, MPIConfiguration.DEFAULT_MPI_CONTAINER_LOG_SIZE);
      Utilities.addLog4jSystemProperties(logLevel, logSize, vargs);
      */
      String javaOpts = conf.get(MPIConfiguration.MPI_CONTAINER_JAVA_OPTS_EXCEPT_MEMORY,"");
      if (!StringUtils.isBlank(javaOpts)) {
        vargs.add(javaOpts);
      }
      vargs.add("com.taobao.yarn.mpi.server.Container");
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
      //ctx.setTokens(UserGroupInformation.getCurrentUser().getCredentials().getAllTokens().duplicate());


      /*
      StartContainerRequest startReq = Records.newRecord(StartContainerRequest.class);
      startReq.setContainerLaunchContext(ctx);
      List<StartContainerRequest> startReqList
          = new ArrayList<StartContainerRequest>();
      startReqList.add(startReq);
      StartContainersRequest startReqs
          = StartContainersRequest.newInstance(startReqList);
      */

      try {
        nmClient.startContainer(container, ctx);
      } catch (Exception e) {
        LOG.error("Start container failed for :"
            + ", containerId=" + container.getId(), e);
        // TODO do we need to release this container?
        e.printStackTrace();
        return false;
      }
      return true;
    }
  }

  /**
   * Register the Application Master to the Resource Manager
   * @return the registration response from the RM
   * @throws YarnException, IOException
   */
  private RegisterApplicationMasterResponse registerToRM()
      throws YarnException, IOException {

    return rmClient.registerApplicationMaster(
        clientService.getBindAddress().getHostName(),
        clientService.getBindAddress().getPort(),
        appMasterTrackingUrl);
  }

  /**
   * Create the container allocation strategy by configuration
   * The method will setup ask for containers from RM and send request for containers to RM
   * Until we get our fully allocated quota, we keep on polling RM for containers
   * Keep looping until all the containers are launched and shell script executed on them
   * ( regardless of success/failure).
   * @return ContainerAllocation reference
   */
  private ContainersAllocator createContainersAllocator() throws YarnException {
    ContainersAllocator allocator = ContainersAllocator.newInstanceByName(
        System.getenv(MPIConstants.ALLOCATOR), rmClient, requestPriority,
        containerMemory, appAttemptID);

    return allocator;
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
      return allContainers;
    }

    @Override
    public LinkedBlockingQueue<String> getMpiMsgQueue() {
      return mpiMsgs;
    }
  }
}
