package com.taobao.yarn.mpi.server;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.yarn.mpi.LocalFileUtils;

public class Container {

  private static final Log LOG = LogFactory.getLog(Container.class);

  private String port;
  private String phrase;

  public boolean init(String[] args) throws ParseException {
    Options options = new Options();
    options.addOption("p", "port", true, "The port of the SMPD daemon process");
    options.addOption("f", "phrase", true, "The pass phrase of the SMPD daemon process");

    CommandLine cliParser = new GnuParser().parse(options, args);
    if (!cliParser.hasOption("port")) {
      throw new ParseException("Port is not defined");
    }
    port = cliParser.getOptionValue("port");

    if (!cliParser.hasOption("phrase")) {
      throw new ParseException("Phrase is not defined");
    }
    phrase = cliParser.getOptionValue("phrase");

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

  public void run() throws IOException{
    Runtime rt = Runtime.getRuntime();
    // TODO We need to hack the smpd_cmd_args.c, to add an option set bService
    final Process pc = rt.exec("smpd -phrase " + phrase
        + " -port " + port + " -debug");

    Thread stdOutThread = new Thread(new Runnable() {
      @Override
      public void run() {
        Scanner stdOut = new Scanner(pc.getInputStream());
        while (stdOut.hasNextLine()) {
          System.out.println(stdOut.nextLine());
        }
      }
    });
    stdOutThread.start();

    Thread stdErrThread = new Thread(new Runnable() {
      @Override
      public void run() {
        Scanner stdErr = new Scanner(pc.getErrorStream());
        while (stdErr.hasNextLine()) {
          System.err.println(stdErr.nextLine());
        }
      }
    });
    stdErrThread.start();
  }

  /**
   * @param args
   * @throws IOException
   * @throws ParseException
   */
  public static void main(String[] args) throws IOException, ParseException {
    printDebugInfo();

    Container container = new Container();
    try {
      if (container.init(args)) {
        // FIXME We need a plugin to fetch data
        container.copyMPIExecutable();
        container.run();
      } else {
        LOG.error("Container init failed!");
        System.exit(-1);
      }
    } catch (IOException e) {
      LOG.error(e.getMessage());
      throw e;
    } catch (ParseException e) {
      LOG.error(e.getMessage());
      throw e;
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
}
