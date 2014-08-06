package org.apache.hadoop.yarn.mpi.server;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.mpi.MPIConfiguration;


public class ContainerDownLoad implements Callable<String> {

  private static final Log LOG = LogFactory.getLog(ContainerDownLoad.class);

  private Path path;

  private FileSystem fs;

  private String output;

  private Configuration conf;

  private int downLoadRetry = 0;

  public ContainerDownLoad(Path path, FileSystem fs, String output, Configuration conf) {
    this.path = path;
    this.fs = fs;
    this.output = output;
    this.conf = conf;
    this.downLoadRetry = conf.getInt(MPIConfiguration.MPI_DOWNLOAD_RETRY, 3);
  }

 /**
  * it's meaning download error when return null
  */
  public String call() throws Exception {
    int retry = 0;
    while (retry++ < this.downLoadRetry) {
      InputStream in = null;
      OutputStream out = null;
      try {
        File exist = new File(output);
        if (exist.exists()) {
          exist.delete();
        }
        in = fs.open(path);
        out = new FileOutputStream(output);
        IOUtils.copyBytes(in, out, conf, false);
        retry = Integer.MAX_VALUE-10;//download successfully
      } catch (Exception e) {
        LOG.error(String.format("download from %s to %s error %d times ", path.toString(), output, retry),e);
      }finally{
        if (in != null) {
          in.close();
        }
        if (out != null) {
          out.close();
        }

      }
    }

    if(retry == this.downLoadRetry) {
      output = null;//fail to download
    }

    return output;
  }
}
