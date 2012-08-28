package com.taobao.yarn.mpi.server;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;

import com.taobao.yarn.mpi.LocalFileUtils;

public class Container {

  /**
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    printDebugInfo();
    // FIXME We need a plugin to fetch data
    copyFiles();

    Runtime rt = Runtime.getRuntime();
    // TODO This need need hack the smpd_cmd_args.c, to add an option set bService
    // FIXME hard coding password
    final Process pc = rt.exec("smpd -phrase 123456 -debug");

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
   * Copy necessary files needed for the MPI program
   */
  private static void copyFiles() {
    Map<String, String> envs = System.getenv();
    String mpiExecDir = envs.get("MPIEXECDIR");
    LocalFileUtils.mkdirs(mpiExecDir);
    File mpiexecCwd = new File("./MPIExec");
    File mpiexecSame = new File(mpiExecDir + "/MPIExec");
    LocalFileUtils.copyFile(mpiexecCwd, mpiexecSame);
    mpiexecSame.setExecutable(true);
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
