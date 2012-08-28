package com.taobao.yarn.mpi.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import com.taobao.yarn.mpi.MPIConstants;
import com.taobao.yarn.mpi.server.ApplicationMaster;


/**
 * Client for MPI application submission to YARN.
 */
public class Client {

  private static final Log LOG = LogFactory.getLog(Client.class);
  // Configuration
  private final Configuration conf;
  // RPC to communicate to RM
  private final YarnRPC rpc;
  // Handle to talk to the Resource Manager/Applications Manager
  private ClientRMProtocol applicationsManager;
  // Application master specific info to register a new Application with RM/ASM
  private String appName = "MPICH2-";
  // App master priority
  private int amPriority = 0;
  // Queue for App master
  private String amQueue = "";
  // Amt. of memory resource to request for to run the App Master
  private int amMemory = 10;
  // Application master jar file
  private String appMasterJar = "";
  // Main class to invoke application master
  private final String appMasterMainClass = "com.taobao.yarn.mpi.server.ApplicationMaster";
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
  private String mpiApplication;
  // Debug flag
  boolean debugFlag = false;

  /**
   * @param args Command line arguments
   */
  public static void main(String[] args) {
    boolean result = false;
    try {
      Client client = new Client();
      LOG.info("Initializing Client");
      boolean doRun = client.init(args);
      if (!doRun) {
        System.exit(0);
      }
      result = client.run();
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
   */
  public Client(Configuration conf) throws Exception  {
    // Set up the configuration and RPC
    this.conf = conf;
    rpc = YarnRPC.create(conf);
  }

  /**
   * Constructor, create YarnRPC
   */
  public Client() throws Exception  {
    this(new Configuration());
  }

  /**
   * Helper function to print out usage
   * @param opts Parsed command line options
   */
  private void printUsage(Options opts) {
    new HelpFormatter().printHelp("Client", opts);
  }

  /**
   * Parse command line options
   * @param args Parsed command line options
   * @return Whether the init was successful to run the client
   * @throws ParseException
   */
  public boolean init(String[] args) throws ParseException {

    Options opts = new Options();
    opts.addOption("priority", true, "Application Priority. Default 0");
    opts.addOption("queue", true, "RM Queue in which this application is to be submitted");
    opts.addOption("timeout", true, "Application timeout in milliseconds, default is 86400000ms, i.e. 24hours");
    opts.addOption("master_memory", true, "Amount of memory in MB to be requested to run the application master");
    opts.addOption("mpi_application", true, "Location of the mpi application to be executed");
    opts.addOption("container_priority", true, "Priority for the shell command containers");
    opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run the shell command");
    opts.addOption("num_containers", true, "No. of containers on which the mpi program needs to be executed");
    opts.addOption("debug", false, "Dump out debug information");
    opts.addOption("help", false, "Print usage");
    CommandLine cliParser = new GnuParser().parse(opts, args);

    if (args.length == 0) {
      printUsage(opts);
      throw new IllegalArgumentException("No args specified for client to initialize");
    }

    if (cliParser.hasOption("help")) {
      printUsage(opts);
      return false;
    }

    if (cliParser.hasOption("debug")) {
      debugFlag = true;
    }

    amPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
    amQueue = cliParser.getOptionValue("queue", "default");
    amMemory = Integer.parseInt(cliParser.getOptionValue("master_memory", "10"));

    if (amMemory <= 0) {
      throw new IllegalArgumentException("Invalid memory specified for application master, exiting."
          + " Specified memory=" + amMemory);
    }

    appMasterJar = JobConf.findContainingJar(ApplicationMaster.class);
    LOG.info("Application Master's jar is " + appMasterJar);

    // MPI executable program
    if (!cliParser.hasOption("mpi_application")) {
      throw new IllegalArgumentException("No mpi executable program specified, exiting.");
    }
    mpiApplication = cliParser.getOptionValue("mpi_application");
    appName += mpiApplication;

    containerPriority = Integer.parseInt(cliParser.getOptionValue("container_priority", "0"));

    containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));

    numContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));

    if (containerMemory < 0 || numContainers < 1) {
      throw new IllegalArgumentException("Invalid no. of containers or container memory specified, exiting."
          + " Specified containerMemory=" + containerMemory
          + ", numContainer=" + numContainers);
    }

    clientTimeout = Integer.parseInt(cliParser.getOptionValue("timeout", "86400000"));

    return true;
  }

  /**
   * Main run function for the client
   * @return true if application completed successfully
   * @throws IOException
   */
  public boolean run() throws IOException {
    LOG.info("Starting Client");

    // Connect to ResourceManager
    connectToASM();
    assert(applicationsManager != null);

    // Use ClientRMProtocol handle to general cluster information
    GetClusterMetricsRequest clusterMetricsReq = Records.newRecord(GetClusterMetricsRequest.class);
    GetClusterMetricsResponse clusterMetricsResp = applicationsManager.getClusterMetrics(clusterMetricsReq);
    LOG.info("Got Cluster metric info from ASM, numNodeManagers="
        + clusterMetricsResp.getClusterMetrics().getNumNodeManagers());

    GetClusterNodesRequest clusterNodesReq = Records.newRecord(GetClusterNodesRequest.class);
    GetClusterNodesResponse clusterNodesResp = applicationsManager.getClusterNodes(clusterNodesReq);
    LOG.info("Got Cluster node info from ASM");
    for (NodeReport node : clusterNodesResp.getNodeReports()) {
      LOG.info("Got node report from ASM for"
          + ", nodeId=" + node.getNodeId()
          + ", nodeAddress" + node.getHttpAddress()
          + ", nodeRackName" + node.getRackName()
          + ", nodeNumContainers" + node.getNumContainers()
          + ", nodeHealthStatus" + node.getNodeHealthStatus());
    }

    GetQueueInfoRequest queueInfoReq = Records.newRecord(GetQueueInfoRequest.class);
    queueInfoReq.setQueueName(this.amQueue);
    GetQueueInfoResponse queueInfoResp = applicationsManager.getQueueInfo(queueInfoReq);
    QueueInfo queueInfo = queueInfoResp.getQueueInfo();
    LOG.info("Queue info"
        + ", queueName=" + queueInfo.getQueueName()
        + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
        + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
        + ", queueApplicationCount=" + queueInfo.getApplications().size()
        + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());

    GetQueueUserAclsInfoRequest queueUserAclsReq = Records.newRecord(GetQueueUserAclsInfoRequest.class);
    GetQueueUserAclsInfoResponse queueUserAclsResp = applicationsManager.getQueueUserAcls(queueUserAclsReq);
    List<QueueUserACLInfo> listAclInfo = queueUserAclsResp.getUserAclsInfoList();
    for (QueueUserACLInfo aclInfo : listAclInfo) {
      for (QueueACL userAcl : aclInfo.getUserAcls()) {
        LOG.info("User ACL Info for Queue"
            + ", queueName=" + aclInfo.getQueueName()
            + ", userAcl=" + userAcl.name());
      }
    }

    // Get a new application id
    GetNewApplicationResponse newApp = getApplication();
    ApplicationId appId = newApp.getApplicationId();

    // TODO get min/max resource capabilities from RM and change memory ask if needed
    // If we do not have min/max, we may not be able to correctly request
    // the required resources from the RM for the app master
    // Memory ask has to be a multiple of min and less than max.
    // Dump out information about cluster capability as seen by the resource manager
    int minMem = newApp.getMinimumResourceCapability().getMemory();
    int maxMem = newApp.getMaximumResourceCapability().getMemory();
    LOG.info("Min mem capabililty of resources in this cluster " + minMem);
    LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

    // A resource ask has to be atleast the minimum of the capability of the cluster, the value has to be
    // a multiple of the min value and cannot exceed the max.
    // If it is not an exact multiple of min, the RM will allocate to the nearest multiple of min
    if (amMemory < minMem) {
      LOG.info("AM memory specified below min threshold of cluster. Using min value."
          + ", specified=" + amMemory
          + ", min=" + minMem);
      amMemory = minMem;
    } else if (amMemory > maxMem) {
      LOG.info("AM memory specified above max threshold of cluster. Using max value."
          + ", specified=" + amMemory
          + ", max=" + maxMem);
      amMemory = maxMem;
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

    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

    LOG.info("Copy App Master jar from local filesystem and add to local environment");
    // Copy the application master jar to the filesystem
    // Create a local resource to point to the destination jar path
    FileSystem fs = FileSystem.get(conf);
    Path appJarSrc = new Path(appMasterJar);
    LOG.info("Source path: " + appJarSrc.toString());
    String pathSuffix = appName + "/" + appId.getId() + "/AppMaster.jar";
    Path appJarDst = new Path(fs.getHomeDirectory(), pathSuffix);
    LOG.info("Destination path: " + appJarDst.toString());
    fs.copyFromLocalFile(false, true, appJarSrc, appJarDst);
    FileStatus appJarDestStatus = fs.getFileStatus(appJarDst);

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

    // Set local resource info into app master container launch context
    amContainer.setLocalResources(localResources);

    LOG.info("Copy MPI application from local filesystem to remote.");
    assert(!mpiApplication.isEmpty());
    Path mpiAppSrc = new Path(mpiApplication);
    String mpiAppSuffix = appName + "/" + appId.getId() + "/MPIExec";
    Path mpiAppDst = new Path(fs.getHomeDirectory(), mpiAppSuffix);
    fs.copyFromLocalFile(false, true, mpiAppSrc, mpiAppDst);
    FileStatus mpiAppFileStatus = fs.getFileStatus(mpiAppDst);

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

    // Add AppMaster.jar location to classpath
    // At some point we should not be required to add
    // the hadoop specific classpaths to the env.
    // It should be provided out of the box.
    // For now setting all required classpaths including
    // the classpath to "." for the application jar
    StringBuilder classPathEnv = new StringBuilder("${CLASSPATH}:./*");
    for (String c : conf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      classPathEnv.append(':');
      classPathEnv.append(c.trim());
    }

    // add the runtime classpath needed for tests to work
    String testRuntimeClassPath = Client.getTestRuntimeClasspath();
    classPathEnv.append(':');
    classPathEnv.append(testRuntimeClassPath);

    env.put("CLASSPATH", classPathEnv.toString());

    amContainer.setEnvironment(env);

    // Set the necessary command to execute the application master
    Vector<CharSequence> vargs = new Vector<CharSequence>(30);

    // Set java executable command
    LOG.info("Setting up app master command");
    vargs.add("${JAVA_HOME}" + "/bin/java");
    // Set Xmx based on am memory size
    vargs.add("-Xmx" + amMemory + "m");
    // Set class name
    vargs.add(appMasterMainClass);
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
    amContainer.setCommands(commands);

    // Set up resource type requirements
    // For now, only memory is supported so we set memory requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(amMemory);
    amContainer.setResource(capability);

    // Service data is a binary blob that can be passed to the application
    // Not needed in this scenario
    // amContainer.setServiceData(serviceData);

    // The following are not required for launching an application master
    // amContainer.setContainerId(containerId);

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
    // SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest);
    // Ignore the response as either a valid response object is returned on success
    // or an exception thrown to denote some form of a failure
    LOG.info("Submitting application to ASM");
    applicationsManager.submitApplication(appRequest);

    // TODO
    // Try submitting the same request again
    // app submission failure?

    // Monitor the application
    return monitorApplication(appId);
  }

  /**
   * Monitor the submitted application for completion.
   * Kill application if time expires.
   * @param appId Application Id of application to be monitored
   * @return true if application completed successfully
   * @throws YarnRemoteException
   */
  private boolean monitorApplication(ApplicationId appId) throws YarnRemoteException {

    while (true) {

      // Check app status every 1 second.
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.debug("Thread sleep in monitoring loop interrupted");
      }

      // Get application report for the appId we are interested in
      GetApplicationReportRequest reportRequest = Records.newRecord(GetApplicationReportRequest.class);
      reportRequest.setApplicationId(appId);
      GetApplicationReportResponse reportResponse = applicationsManager.getApplicationReport(reportRequest);
      ApplicationReport report = reportResponse.getApplicationReport();

      LOG.info("Got application report from ASM for"
          + ", appId=" + appId.getId()
          + ", clientToken=" + report.getClientToken()
          + ", appDiagnostics=" + report.getDiagnostics()
          + ", appMasterHost=" + report.getHost()
          + ", appQueue=" + report.getQueue()
          + ", appMasterRpcPort=" + report.getRpcPort()
          + ", appStartTime=" + report.getStartTime()
          + ", yarnAppState=" + report.getYarnApplicationState().toString()
          + ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
          + ", appTrackingUrl=" + report.getTrackingUrl()
          + ", appUser=" + report.getUser());

      YarnApplicationState state = report.getYarnApplicationState();
      FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
      if (YarnApplicationState.FINISHED == state) {
        if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
          LOG.info("Application has completed successfully. Breaking monitoring loop");
          return true;
        }
        else {
          LOG.info("Application did finished unsuccessfully."
              + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
              + ". Breaking monitoring loop");
          return false;
        }
      }
      else if (YarnApplicationState.KILLED == state
          || YarnApplicationState.FAILED == state) {
        LOG.info("Application did not finish."
            + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
            + ". Breaking monitoring loop");
        return false;
      }

      if (System.currentTimeMillis() > (clientStartTime + clientTimeout)) {
        LOG.info("Reached client specified timeout for application. Killing application");
        killApplication(appId);
        return false;
      }
    }  // end while
  }

  /**
   * Kill a submitted application by sending a call to the ASM
   * @param appId Application Id to be killed.
   * @throws YarnRemoteException
   */
  private void killApplication(ApplicationId appId) throws YarnRemoteException {
    KillApplicationRequest request = Records.newRecord(KillApplicationRequest.class);
    // TODO clarify whether multiple jobs with the same app id can be submitted and be running at
    // the same time.
    // If yes, can we kill a particular attempt only?
    request.setApplicationId(appId);
    // KillApplicationResponse response = applicationsManager.forceKillApplication(request);
    // Response can be ignored as it is non-null on success or
    // throws an exception in case of failures
    applicationsManager.forceKillApplication(request);
  }

  /**
   * Connect to the Resource Manager/Applications Manager
   * @return Handle to communicate with the ASM
   * @throws IOException
   */
  private void connectToASM() throws IOException {

    /*
        UserGroupInformation user = UserGroupInformation.getCurrentUser();
        applicationsManager = user.doAs(new PrivilegedAction<ClientRMProtocol>() {
            public ClientRMProtocol run() {
                InetSocketAddress rmAddress = NetUtils.createSocketAddr(conf.get(
                    YarnConfiguration.RM_SCHEDULER_ADDRESS,
                    YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS));
                LOG.info("Connecting to ResourceManager at " + rmAddress);
                Configuration appsManagerServerConf = new Configuration(conf);
                appsManagerServerConf.setClass(YarnConfiguration.YARN_SECURITY_INFO,
                ClientRMSecurityInfo.class, SecurityInfo.class);
                ClientRMProtocol asm = ((ClientRMProtocol) rpc.getProxy(ClientRMProtocol.class, rmAddress, appsManagerServerConf));
                return asm;
            }
        });
     */
    YarnConfiguration yarnConf = new YarnConfiguration(conf);
    InetSocketAddress rmAddress = yarnConf.getSocketAddr(
        YarnConfiguration.RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_PORT);
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    applicationsManager = ((ClientRMProtocol) rpc.getProxy(
        ClientRMProtocol.class, rmAddress, conf));
  }

  /**
   * Get a new application from the ASM
   * @return New Application
   * @throws YarnRemoteException
   */
  private GetNewApplicationResponse getApplication() throws YarnRemoteException {
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
      LOG.info("Could not find the necessary resource to generate class path for tests. Error=" + e.getMessage());
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
}
