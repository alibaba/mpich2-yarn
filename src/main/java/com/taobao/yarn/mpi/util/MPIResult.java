
package com.taobao.yarn.mpi.util;


public class MPIResult {
  // local location of the container
  private String containerLocal;

  private String dfsLocation;

  public String getContainerLocal() {
    return containerLocal;
  }

  public void setContainerLocal(String containerLocal) {
    this.containerLocal = containerLocal;
  }

  public String getDfsLocation() {
    return dfsLocation;
  }

  public void setDfsLocation(String dfsLocation) {
    this.dfsLocation = dfsLocation;
  }

  public String toString(){
    return containerLocal + ";" + dfsLocation ;
  }

}
