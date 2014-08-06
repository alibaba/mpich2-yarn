package org.apache.hadoop.yarn.mpi.webapps;

import org.apache.hadoop.yarn.mpi.server.AppContext;

import com.google.inject.Inject;
import com.google.inject.servlet.RequestScoped;

@RequestScoped
public class App {
  final AppContext context;

  @Inject
  App(AppContext context) {
    this.context = context;
  }
}
