package org.apache.hadoop.yarn.mpi.util;


public class InputFile {


  private boolean same = false;

  //the file location
  private String location;

  public boolean isSame() {
    return same;
  }

  public void setSame(boolean same) {
    this.same = same;
  }

  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  public String toString(){
    return location + ";" + same ;
  }
}
