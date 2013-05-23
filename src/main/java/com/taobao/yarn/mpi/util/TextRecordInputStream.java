package com.taobao.yarn.mpi.util;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;

public class TextRecordInputStream extends InputStream {
  SequenceFile.Reader r;
  WritableComparable<?> key;
  Writable val;

  DataInputBuffer inbuf;
  DataOutputBuffer outbuf;

  public TextRecordInputStream(FileStatus f,Configuration lconf) throws IOException {
    final Path fpath = f.getPath();
    r = new SequenceFile.Reader(lconf,
        SequenceFile.Reader.file(fpath));
    key = ReflectionUtils.newInstance(
        r.getKeyClass().asSubclass(WritableComparable.class), lconf);
    val = ReflectionUtils.newInstance(
        r.getValueClass().asSubclass(Writable.class), lconf);
    inbuf = new DataInputBuffer();
    outbuf = new DataOutputBuffer();
  }

  @Override
  public int read() throws IOException {
    int ret;
    if (null == inbuf || -1 == (ret = inbuf.read())) {
      if (!r.next(key, val)) {
        return -1;
      }
      byte[] tmp = key.toString().getBytes();
      outbuf.write(tmp, 0, tmp.length);
      outbuf.write('\t');
      tmp = val.toString().getBytes();
      outbuf.write(tmp, 0, tmp.length);
      outbuf.write('\n');
      inbuf.reset(outbuf.getData(), outbuf.getLength());
      outbuf.reset();
      ret = inbuf.read();
    }
    return ret;
  }

  @Override
  public void close() throws IOException {
    r.close();
    super.close();
  }
}
