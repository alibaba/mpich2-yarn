
package com.taobao.yarn.mpi.util;

import java.io.Serializable;

public class MPDException extends RuntimeException implements Serializable{

  private static final long serialVersionUID = 1477339123959712036L;

  public MPDException() {
    super();
  }

  public MPDException(String message) {
    super(message);
  }

  public MPDException(Throwable cause) {
    super(cause);
  }

  public MPDException(String message, Throwable cause) {
    super(message, cause);
  }

}
