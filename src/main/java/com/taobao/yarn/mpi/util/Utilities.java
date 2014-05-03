package com.taobao.yarn.mpi.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Records;

import com.taobao.yarn.mpi.MPIConfiguration;
import com.taobao.yarn.mpi.api.MPIClientProtocol;

public final class Utilities {
  private static Log LOG = LogFactory.getLog(Utilities.class);

  private static Random rnd = new Random();

  /**
   * This is a static class
   */
  private Utilities() { }

  /**
   * Sleep for millis milliseconds, which throws no exception
   * @param millis milliseconds
   */
  public static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      LOG.warn("Thread Interrupted ...", e);
    }
  }

  /**
   * Setup the request that will be sent to the RM for the container ask.
   * @param numContainers Containers to ask for from RM
   * @return the setup ResourceRequest to be sent to RM
   */
  public static ResourceRequest setupContainerAskForRM(
      int numContainers,
      int requestPriority,
      int containerMemory) {

    // set the priority for the request
    Priority pri = Records.newRecord(Priority.class);
    // TODO - what is the range for priority? how to decide?
    pri.setPriority(requestPriority);

    // Set up resource type requirements
    // For now, only memory is supported so we set memory requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(containerMemory);

    // setup requirements for hosts, whether a particular rack/host is needed
    // Refer to apis under org.apache.hadoop.net for more details on how to get figure out
    // rack/host mapping. using * as any host will do for the distributed shell app
    ResourceRequest request = ResourceRequest.newInstance(
        pri, "*", capability, numContainers);
    return request;
  }

  /**
   * Ask RM to allocate given no. of containers to this Application Master
   * @param requestedContainers Containers to ask for from RM
   * @return Response from RM to AM with allocated containers
   * @throws YarnException, IOException
   */
  public static AllocateResponse sendContainerAskToRM(
      AtomicInteger rmRequestID,
      ApplicationAttemptId appAttemptID,
      ApplicationMasterProtocol resourceManager,
      List<ResourceRequest> requestedContainers,
      List<ContainerId> releasedContainers,
      float progress) throws YarnException, IOException {
    AllocateRequest req = AllocateRequest.newInstance(
        rmRequestID.incrementAndGet(), progress,
        requestedContainers, releasedContainers, null);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending request to RM for containers"
          + ", requestedSet=" + requestedContainers.size()
          + ", releasedSet=" + releasedContainers.size()
          + ", progress=" + req.getProgress());
    }

    for (ResourceRequest  rsrcReq : requestedContainers) {
      LOG.info("Requested container ask: " + rsrcReq.toString());
    }
    for (ContainerId id : releasedContainers) {
      LOG.info("Released container, id=" + id.getId());
    }

    AllocateResponse resp = resourceManager.allocate(req);
    return resp;
  }

  /**
   * Get the random phrase of 0-9, A-Z, a-z
   * @param length the length of the phrase
   * @return the phrase
   */
  public static String getRandomPhrase(int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      char c = (char) rnd.nextInt(62);
      if (c < (char) 10) {
        c += '0';  // '0' to ''9'
      } else if (c < (char) 36) {
        c += 55;  // 'A'(65) to 'Z'(90)
      } else {
        c += 61;  // 'a'(97) to 'z'(122)
      }
      sb.append(c);
    }
    return sb.toString();
  }

  /**
   * get the fileStatus of a certain path recursively
   * @param path representing the file or folder
   * @param fs the default filesystem
   * @param fileStatuses
   * @return
   * @throws IOException
   * @throws FileNotFoundException
   */
  public static List<FileStatus> listRecursive(Path path, FileSystem fs, List<FileStatus> fileStatuses) throws FileNotFoundException, IOException{
    if (fileStatuses == null) {
      fileStatuses = new ArrayList<FileStatus>();
    }
    FileStatus[] files = fs.listStatus(path);
    if (files != null && files.length>0) {
      for (FileStatus f: files) {
        if (fs.isDirectory(f.getPath())) {
          listRecursive(f.getPath(), fs, fileStatuses);
        }else {
          fileStatuses.add(f);
        }
      }
    }
    return fileStatuses;

  }

  /**
   * encode the fileSplits to string
   * @param files
   * @return
   */
  public static String encodeSplit(List<FileSplit> fileSplits) {
    StringBuilder splitStr = new StringBuilder();
    if (fileSplits != null && fileSplits.size() > 0) {
      for (FileSplit file : fileSplits){
        List<String> names = new ArrayList<String>();
        for (Path p: file.getSplits()) {
          names.add(p.toString());
        }
        //TODO replace the hard coding with '@',';','|'
        splitStr.append(file.getFileName()).append("@").append(file.getDownFileName()).append("@").append(StringUtils.join(names, ';')).append("|");
      }
      return splitStr.substring(0, splitStr.length()-1);
    }else{
      return "";
    }

  }

  /**
   * decode the fileSplit
   * @param s
   * @param fs
   * @return
   */
  public static List<FileSplit> decodeSplt(String s, FileSystem fs) {
    List<FileSplit> splits=new ArrayList<FileSplit>();
    if (!StringUtils.isBlank(s)) {
      //TODO replace the hard coding with '@',';','|',the regular is same with encodeSplit
      String[] splitFiles = StringUtils.split(s, "|");
      if (splitFiles !=null && splitFiles.length > 0) {
        for (String file: splitFiles) {
          FileSplit fSplit = new FileSplit();
          String fileParts[] = StringUtils.split(file,"@");
          if (fileParts.length == 3) {
            fSplit.setFileName(fileParts[0]);
            fSplit.setDownFileName(fileParts[1]);
            String fileLocation[] = StringUtils.split(fileParts[2], ";");
            for (String location : fileLocation) {
              if(!StringUtils.isBlank(location)){
                Path path = new Path(location);
                path = fs.makeQualified(path);
                fSplit.addPath(path);
              }
            }
            splits.add(fSplit);
          }
        }
      }
    }
    return splits;
  }

  public static String encodeMPIResult(Collection<MPIResult> results) {
    StringBuilder strResult = new StringBuilder(50);
    if (results != null && results.size() > 0) {
      for (MPIResult r : results) {
        //TODO replace the hard coding with '@',';'
        strResult.append(r.toString()).append("@");
      }
      return strResult.substring(0, strResult.length()-1).toString();
    }else{
      return "";
    }
  }

  public static Collection<MPIResult> decodeMPIResult(String result) {
    Collection<MPIResult> results = new ArrayList<MPIResult>();
    if (!StringUtils.isBlank(result)) {
      //TODO replace the hard coding with '@',';'
      String strResult[] = StringUtils.split(result, "@");
      if (!ArrayUtils.isEmpty(strResult)) {
        for (String r : strResult) {
          String strMr[] = StringUtils.split(r, ";");
          MPIResult mpi = new MPIResult();
          mpi.setContainerLocal(strMr[0]);
          mpi.setDfsLocation(strMr[1]);
          results.add(mpi);
        }
      }
    }
    return results;
  }

  /**
   * convert FileStatus to Path
   * @param statuses
   * @return
   */
  public static List<Path> convertToPath(List<FileStatus> statuses) {
    List<Path> paths = new ArrayList<Path>();
    if (statuses != null){
      for (FileStatus status: statuses) {
        paths.add(status.getPath());
      }
    }
    return paths;
  }

  /**
   * get the inputStream of the given path
   * @param p the given path
   * @param srcFs file system
   * @param conf
   * @return
   * @throws IOException
   */
  public static InputStream forMagic(Path p, FileSystem srcFs, Configuration conf) throws IOException {
    FSDataInputStream i = srcFs.open(p);

    // support empty file
    if (srcFs.getFileStatus(p).getLen() == 0) {
      return srcFs.open(p);
    }

    // check codec
    CompressionCodecFactory cf = new CompressionCodecFactory(conf);
    CompressionCodec codec = cf.getCodec(p);
    if (codec != null) {
      return codec.createInputStream(i);
    }

    switch(i.readShort()) {
    case 0x1f8b: // RFC 1952
      i.seek(0);
      return new GZIPInputStream(i);
    case 0x5345: // 'S' 'E'
      if (i.readByte() == 'Q') {
        i.close();
        return new TextRecordInputStream(srcFs.getFileStatus(p),conf);
      }
      break;
    }
    i.seek(0);
    return i;
  }

  /**
   * get MPI executable local directory
   * @param conf
   * @return local directory
   */
  public static String getMpiExecDir(Configuration conf, ApplicationAttemptId appAttemptID) {
    String execDir = null;
    StringBuilder mpiExecBuilder = new StringBuilder(100);
    mpiExecBuilder.append(conf.get("hadoop.tmp.dir" , "/tmp")).append("/mpiexecs/");
    execDir = conf.get("mpi.local.dir", mpiExecBuilder.toString()) + appAttemptID.toString();
    return execDir;
  }

  /**
   * get the download data dir
   * @param conf
   * @param appAttemptID
   * @param containerId
   * @return
   */
  public static String getDownLoadDir(Configuration conf, String appAttemptID, int containerId ){
    StringBuilder post = new StringBuilder(100);
    post.append("/mpidownload/").append(containerId).append("/");
    StringBuilder dir = new StringBuilder(100);
    dir.append(conf.get("mpi.local.dir", conf.get("hadoop.tmp.dir" , "/tmp") + "/mpidata")).append("/").append(appAttemptID);
    return dir.toString() + post.toString();

  }

  public static String getApplicationDir(Configuration conf, String appAttemptID) {
    StringBuilder dir = new StringBuilder(100);
    dir.append(conf.get("mpi.local.dir", conf.get("hadoop.tmp.dir" , "/tmp") + "/mpidata")).append("/").append(appAttemptID).append("/");
    return dir.toString();
  }

  /**
   * Kill a submitted application by sending a call to the ASM
   * @param applicationsManager Client<->RM
   * @param appId Application Id to be killed.
   * @throws YarnException, IOException
   */
  public static void killApplication(ApplicationClientProtocol applicationsManager, ApplicationId appId)
      throws YarnException, IOException {
    KillApplicationRequest request = Records.newRecord(KillApplicationRequest.class);
    // TODO clarify whether multiple jobs with the same app id can be submitted
    // and be running at the same time. If yes, can we kill a particular attempt only?
    request.setApplicationId(appId);
    LOG.info("Killing appliation with id: " + appId.toString());
    // Response can be ignored as it is non-null on success or throws an exception in case of failures
    applicationsManager.forceKillApplication(request);
  }

  /**
   * Connect to the Resource Manager/Applications Manager
   * @return Handle to communicate with the ASM
   * @throws IOException
   */
  public static ApplicationClientProtocol connectToASM(YarnConfiguration conf) throws IOException {
    YarnRPC rpc = YarnRPC.create(conf);
    InetSocketAddress rmAddress = conf.getSocketAddr(
        YarnConfiguration.RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_PORT);
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    ApplicationClientProtocol applicationsManager = ((ApplicationClientProtocol) rpc.getProxy(
        ApplicationClientProtocol.class, rmAddress, conf));
    return applicationsManager;
  }

  /**
   * Get the destination file path of the HDFS
   * @param dfs HDFS
   * @param appName Application name
   * @param appId Application Id
   * @param filename file name
   * @return the Path instance
   */
  public static Path getAppFile(MPIConfiguration conf, String appName, ApplicationId appId, String filename) {
    String pathSuffix = appName + "/" + appId.getId() + "/" + filename;
    Path result = new Path(conf.get(MPIConfiguration.MPI_SCRATCH_DIR, MPIConfiguration.DEFAULT_MPI_SCRATCH_DIR),
        pathSuffix);
    return result;
  }

  /**
   * get Report of ApplicationMaster
   * @param appId
   * @param applicationsManager
   * @return
   * @throws YarnException, IOException
   */
  public static ApplicationReport getApplicationReport(ApplicationId appId, ApplicationClientProtocol applicationsManager)
      throws YarnException, IOException {
    RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
    GetApplicationReportRequest request = recordFactory.newRecordInstance(GetApplicationReportRequest.class);
    request.setApplicationId(appId);
    GetApplicationReportResponse response = applicationsManager.getApplicationReport(request);
    ApplicationReport applicationReport = response.getApplicationReport();
    return applicationReport;
  }

  /**
   * Connect to the ApplicationMaster
   * @return Handle to communicate with the AM
   * @throws IOException
   */
  public static MPIClientProtocol connectToAM(YarnConfiguration conf, String amAddress, int amPort) throws IOException {
    MPIClientProtocol applicationManager = null;
    if (!StringUtils.isBlank(amAddress) && !amAddress.equalsIgnoreCase("N/A")) {
      InetSocketAddress addr = new InetSocketAddress(amAddress, amPort);
      applicationManager = RPC.getProxy(MPIClientProtocol.class, MPIClientProtocol.versionID, addr, conf);
      LOG.info("Connecting to ApplicationMaster at " + addr);
    }
    return applicationManager;
  }

  /*
  public static void addLog4jSystemProperties(
      String logLevel, long logSize,Vector<CharSequence> vargs) {
    //We will be confused with the values of MRJobConfig.TASK_LOG_DIR and MRJobConfig.TASK_LOG_SIZE, see container-log4j.properties
    vargs.add("-Dlog4j.configuration=container-log4j.properties");
    vargs.add("-D" + MRJobConfig.TASK_LOG_DIR + "=" +
        ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    vargs.add("-D" + MRJobConfig.TASK_LOG_SIZE + "=" + logSize);
    vargs.add("-Dhadoop.root.logger=" + logLevel + ",CLA");
  }
  */

}
