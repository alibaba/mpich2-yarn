package org.apache.hadoop.yarn.mpi.server;

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

  @Override
  public void readFields(DataInput in) throws IOException {
    StringBuilder sb = new StringBuilder();
    char c = in.readChar();
    while (c != '$') {
      sb.append(c);
      c = in.readChar();
    }
    this.id = ConverterUtils.toContainerId(sb.toString());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeChars(toString() + "$");
  }

  @Override
  public String toString() { return this.id.toString(); }

  @Override
  public int hashCode() { return toString().hashCode(); }

  @Override
  public boolean equals(Object o) {
    return toString().equals(((ContainerId) o).toString());
  }
}
