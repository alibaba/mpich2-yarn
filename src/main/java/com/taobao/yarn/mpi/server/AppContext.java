package com.taobao.yarn.mpi.server;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;

/**
 * Context interface for sharing information across components in YARN App
 */
public interface AppContext {

  ApplicationId getApplicationID();

  ApplicationAttemptId getApplicationAttemptId();

  List<Container> getAllContainers();

  LinkedBlockingQueue<String> getMpiMsgQueue();
}
