package org.apache.hadoop.yarn.mpi;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Utilities for operating local files
 */
public class LocalFileUtils {
  /**
   * Copy a file
   * @param src Source file
   * @param dst Destination file
   */
  public static void copyFile(File src, File dst) {
    try{
      InputStream in = new FileInputStream(src);
      OutputStream out = new FileOutputStream(dst);

      byte[] buf = new byte[1024];
      int len;
      while ((len = in.read(buf)) > 0){
        out.write(buf, 0, len);
      }
      in.close();
      out.close();
    }
    catch(FileNotFoundException ex){
      System.err.println(ex.getMessage() + " in the specified directory.");
    }
    catch(IOException e){
      System.err.println(e.getMessage());
    }
  }

  /**
   * Copy a file
   * @param srcPath Source path
   * @param dstPath Destination path
   */
  public static void copyFile(String srcPath, String dstPath){
    File src = new File(srcPath);
    File dst = new File(dstPath);
    copyFile(src, dst);
  }

  /**
   * Making directory recursively
   * @param path The directory
   * @return Whether successful
   */
  public static boolean mkdirs(String path) {
    File file = new File(path);
    return file.mkdirs();
  }
}
