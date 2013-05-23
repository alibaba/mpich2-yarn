package com.taobao.yarn.mpi.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;


public class FileSplit {

  private String fileName;

  private String downFileName;

  private List<Path> splits = new ArrayList<Path>();

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public List<Path> getSplits() {
    return splits;
  }

  public void setSplits(List<Path> splits) {
    this.splits = splits;
  }

  public void addPath(Path path) {
    splits.add(path);
  }

  public String getDownFileName() {
    return downFileName;
  }

  public void setDownFileName(String downFileName) {
    this.downFileName = downFileName;
  }
}
