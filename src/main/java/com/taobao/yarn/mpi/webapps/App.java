package com.taobao.yarn.mpi.webapps;

import com.google.inject.Inject;
import com.google.inject.servlet.RequestScoped;
import com.taobao.yarn.mpi.server.AppContext;

@RequestScoped
public class App {
  final AppContext context;

  @Inject
  App(AppContext context) {
    this.context = context;
  }
}
