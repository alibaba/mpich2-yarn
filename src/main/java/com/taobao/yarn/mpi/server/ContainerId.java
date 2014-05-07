package com.taobao.yarn.mpi.server;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class ContainerId implements Writable {

  private org.apache.hadoop.yarn.api.records.ContainerId id;

  private static final int MAX_BUFFER_SIZE = 4096;

  public ContainerId() { this.id = null; }

  public ContainerId(org.apache.hadoop.yarn.api.records.ContainerId id) {
    this.id = id;
  }

  public void readFields(DataInput in) throws IOException {
    StringBuilder sb = new StringBuilder();
    char c = in.readChar();
    while (c != '$') {
      sb.append(c);
      c = in.readChar();
    }
    this.id = ConverterUtils.toContainerId(sb.toString());
  }

  public void write(DataOutput out) throws IOException {
    out.writeChars(toString() + "$");
  }

  public String toString() { return this.id.toString(); }
}
