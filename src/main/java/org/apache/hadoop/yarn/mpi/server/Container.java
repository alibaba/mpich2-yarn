package org.apache.hadoop.yarn.mpi.server;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.mpi.MPIConfiguration;
import org.apache.hadoop.yarn.mpi.MPIConstants;
import org.apache.hadoop.yarn.mpi.util.FileSplit;
import org.apache.hadoop.yarn.mpi.util.LocalFileUtils;
import org.apache.hadoop.yarn.mpi.util.MPDException;
import org.apache.hadoop.yarn.mpi.util.MPIResult;
import org.apache.hadoop.yarn.mpi.util.Utilities;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class Container {

  private static final Log LOG = LogFactory.getLog(Container.class);

  // The port of SMPD process
  private String smpdPort;
  private String phrase;

  private TaskReporter taskReporter;

  private final ExecutorService executorDownload;

  private static final int POOL_SIZE = 4;

  private String localDir;

  private final Configuration conf;
  private MPDProtocol protocol;
  private String appMasterHost;
  private int appMasterPort;
  private ContainerId containerId = null;

  private String appAttemptID;

  private Boolean downloadSave = false;

  private Collection<MPIResult> results;

  public Container(){
    conf = new MPIConfiguration();
    executorDownload=Executors.newFixedThreadPool(POOL_SIZE, new ThreadFactoryBuilder()
    .setDaemon(true)
    .setNameFormat("Download Thread #%d")
    .build());
  }


  public boolean init(String[] args) throws ParseException, IOException {
    Options options = new Options();
    options.addOption("p", "port", true, "The port of the SMPD daemon process");
    options.addOption("f", "phrase", true, "The pass phrase of the SMPD daemon process");

    CommandLine cliParser = new GnuParser().parse(options, args);
    if (!cliParser.hasOption("port")) {
      throw new ParseException("Port is not defined");
    }
    smpdPort = cliParser.getOptionValue("port");

    if (!cliParser.hasOption("phrase")) {
      throw new ParseException("Phrase is not defined");
    }
    phrase = cliParser.getOptionValue("phrase");

    containerId = new ContainerId(ConverterUtils.toContainerId(System.getenv(
        ApplicationConstants.Environment.CONTAINER_ID.toString())));
    if (containerId == null) {
      LOG.error("No container ID in env.");
      throw new ParseException("Container Id is not defined");
    }

    appMasterHost = System.getenv("APPMASTER_HOST");
    appMasterPort = Integer.valueOf(System.getenv("APPMASTER_PORT"));
    //    conf = new Configuration();
    InetSocketAddress addr = new InetSocketAddress(appMasterHost, appMasterPort);
    protocol = RPC.getProxy(
        MPDProtocol.class, MPDProtocol.versionID, addr, conf);
    protocol.reportStatus(containerId, MPDStatus.INITIALIZED);
    taskReporter = new TaskReporter(protocol, conf, containerId);
    taskReporter.setDaemon(true);
    taskReporter.start();

    Map<String, String> envs = System.getenv();
    localDir = Utilities.getDownLoadDir(
        conf, envs.get(MPIConstants.APPATTEMPTID), containerId);
    LOG.info(String.format("localDir:%s", localDir));
    appAttemptID=envs.get(MPIConstants.APPATTEMPTID);
    downloadSave = conf.getBoolean(MPIConfiguration.MPI_CONTAINER_DOWNLOAD_SAVE, false);
    String containerOutput = envs.get(MPIConstants.CONTAINOUTPUT);
    results = Utilities.decodeMPIResult(containerOutput);
    return true;
  }

  /**
   * download files if necessary
   * @throws IOException
   * @throws ExecutionException
   */
  public Boolean download() throws IOException,InterruptedException, ExecutionException{
    Map<String, String> envs = System.getenv();
    String fileSplits = envs.get(MPIConstants.CONTAININPUT);
    List<FileSplit> splits = Utilities.decodeSplt(fileSplits, FileSystem.get(conf));
    List<ContainerDownLoad> downLoads = new ArrayList<ContainerDownLoad>();
    boolean allDownLoadSuccess = true;
    boolean mergeSuccess = true;

    if (splits != null && splits.size() > 0) {
      File dirExist = new File(localDir);
      if (dirExist.exists()) {
        dirExist.delete();
      }
      LocalFileUtils.mkdirs(localDir);
      List<String> downloadResult = new ArrayList<String>();
      for (FileSplit fSplit : splits) {
        downloadResult.clear();
        downLoads.clear();
        LOG.info(String.format("begin to download the following files:%s", fSplit.getSplits()));
        int i=1;
        for (Path path : fSplit.getSplits()) {
          String downLoadOut = localDir + System.currentTimeMillis() + "-"+ i++;
          ContainerDownLoad download = new ContainerDownLoad(path, FileSystem.get(conf), downLoadOut, conf);
          downLoads.add(download);
        }
        LOG.info(String.format("download size: %d", downLoads.size()));
        List<Future<String>> results = this.executorDownload.invokeAll(downLoads);

        LOG.info(String.format("result size: %d", results.size()));

        for (Future<String> result : results) {
          if (result.get() == null){
            allDownLoadSuccess = false;
          }else {
            downloadResult.add(result.get());
          }
        }
        if (allDownLoadSuccess) {
          LOG.info(String.format("download the following files:%s successfully", fSplit.getSplits()));
          LOG.info(String.format("begin to merge the following  files:%s,and the merge file name:%s", downloadResult, fSplit.getDownFileName()));
          ContainerMerge merge = new ContainerMerge(downloadResult, fSplit.getDownFileName());
          FutureTask<Boolean> mergeTask = new FutureTask<Boolean>(merge);
          Thread mergeThread = new Thread(mergeTask);
          mergeThread.start();
          mergeSuccess = mergeTask.get();
          if(mergeSuccess){
            LOG.info(String.format("merge the following  files:%s successfully", downloadResult));
          }else{
            LOG.error(String.format("fail to merge the following  files:%s", downloadResult));
          }
        }else{
          LOG.info(String.format("fail to download the following files:%s ", fileSplits));
        }

        if (!allDownLoadSuccess || !mergeSuccess) {
          break;
        }
      }
    }
    if (allDownLoadSuccess && mergeSuccess) {
      return true;
    }else {
      return false;
    }

  }

  // upload file from container to hdfs
  public boolean upload() throws IOException{
    if (results != null && results.size() > 0) {
      Iterator<MPIResult> itResult = results.iterator();
      while (itResult.hasNext()) {
        MPIResult mr = itResult.next();
        FileSystem localFs = FileSystem.getLocal(conf);
        FileSystem dfs = FileSystem.get(conf);
        Path localPath = new Path(mr.getContainerLocal());
        Path resultPath = new Path(mr.getDfsLocation());
        if (localFs.exists(localPath)) {
          if (!dfs.isDirectory(resultPath) && dfs.exists(resultPath)){
            throw new MPDException(String.format("file %s exists", resultPath.toString()));
          }else{
            dfs.copyFromLocalFile(false, false, localPath, resultPath);
          }
        }
      }
    }

    return true;
  }

  /**
   * Copy necessary files needed for the MPI program
   */
  public void copyMPIExecutable() {
    Map<String, String> envs = System.getenv();
    String mpiExecDir = envs.get("MPIEXECDIR");
    LocalFileUtils.mkdirs(mpiExecDir);
    File mpiexecCwd = new File("./MPIExec");
    File mpiexecSame = new File(mpiExecDir + "/MPIExec");
    LocalFileUtils.copyFile(mpiexecCwd, mpiexecSame);
    mpiexecSame.setExecutable(true);
  }

  public Boolean run() throws IOException{
    Runtime rt = Runtime.getRuntime();
    // Hacked the smpd_cmd_args.c, to add an option set bService
    String cmdLine = "smpd -phrase " + phrase
        + " -port " + smpdPort + " -yarn";
    LOG.info("Launching SMPD Command: " + cmdLine);
    final Process pc = rt.exec(cmdLine);
    // If we get the reference of the process, we get the running daemon.
    if (pc != null) {
      protocol.reportStatus(containerId, MPDStatus.MPD_STARTED);
    }else {
      LOG.error("error occurs while creating the smpd process");
    }

    Thread stdOutThread = new Thread(new Runnable() {
      @Override
      public void run() {
        Scanner stdOut = new Scanner(pc.getInputStream());
        while (stdOut.hasNextLine()) {
          LOG.info(stdOut.next());
        }
      }
    });
    stdOutThread.start();

    Thread stdErrThread = new Thread(new Runnable() {
      @Override
      public void run() {
        Scanner stdErr = new Scanner(pc.getErrorStream());
        while (stdErr.hasNextLine()) {
          String err = stdErr.nextLine();
          System.err.println(String.format("Continer %s error:%s",
              containerId.toString(), err));
        }
      }
    });
    stdErrThread.start();
    Boolean runSuccess = true;
    try {
      int ret = pc.waitFor();
      if (ret != 0) {
        runSuccess = false;
        LOG.error(String.format(
            "Container %s, smpd crash, smpd returned value: %d",
            containerId.toString(), ret));
        protocol.reportStatus(containerId, MPDStatus.MPD_CRASH);
      } else {
        runSuccess = true;
        protocol.reportStatus(containerId, MPDStatus.FINISHED);
        LOG.info(String.format(
            "Container %s, smpd finish successfully", containerId.toString()));
      }
    } catch (InterruptedException e) {
      LOG.error("Process Interrupted.", e);
    }
    return runSuccess;
  }

  public String getLocalDir() {
    return localDir;
  }

  /**
   * @param args
   * @throws IOException
   * @throws ParseException
   * @throws InterruptedException
   * @throws ExecutionException
   */
  public static void main(String[] args) {
    try {
      printDebugInfo();
    } catch (Exception e) {
      LOG.error("Error print debug info.");
      e.printStackTrace();
    }

    final Container container = new Container();
    try {
      if (container.init(args)) {
        // add the shutdownHood after init
        if (!container.getDownloadSave()){
          Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
              String deleteDir = Utilities.getApplicationDir(container.getConf(), container.getAppAttemptID());
              try {
                FileUtil.fullyDelete(new File(deleteDir));
                LOG.info(String.format("clean the folder:%s successfully", deleteDir));
              } catch (Exception e2) {
                LOG.error(String.format("error happens when cleaning the folder: %s", deleteDir),e2);
              }
            }
          });
        }

        if (container.download()) {
          LOG.info("download successfully");
          container.copyMPIExecutable();
          LOG.info("copy mpi program successfully");
          Boolean runSuccess = container.run();
          if (runSuccess) {
            container.upload();
          }
        }else {
          container.getProtocol().reportStatus(container.getContainerId(), MPDStatus.ERROR_FINISHED);
          LOG.error("downlaod failed!");
          System.exit(-1);
        }
      } else {
        container.getProtocol().reportStatus(container.getContainerId(), MPDStatus.ERROR_FINISHED);
        LOG.error("Container init failed!");
        System.exit(-1);
      }
    } catch (Exception e) {
      LOG.error("Error executing MPI task in container.");
      e.printStackTrace();
      container.getProtocol().reportStatus(
          container.getContainerId(), MPDStatus.ERROR_FINISHED);
    }
  }

  /**
   * Print the environment and working directory information for debugging.
   * @throws IOException
   */
  private static void printDebugInfo() throws IOException {
    File directory = new File(".");
    System.err.println(directory.getCanonicalPath());
    File mpiexec = new File("./MPIExec");
    System.err.println(mpiexec.getCanonicalPath());

    Map<String, String> envs = System.getenv();
    Set<Entry<String, String>> entries = envs.entrySet();
    for (Entry<String, String> entry : entries) {
      System.err.println("key=" + entry.getKey() + "; value=" + entry.getValue());
    }
  }

  public Configuration getConf() {
    return conf;
  }

  public String getAppAttemptID() {
    return appAttemptID;
  }

  public Boolean getDownloadSave() {
    return downloadSave;
  }


  public MPDProtocol getProtocol() {
    return protocol;
  }


  public ContainerId getContainerId() {
    return containerId;
  }
}
