/*
 * Copyright (c) 2017 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.example;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.collect.ImmutableMap;
import com.yahoo.yqlplus.example.apis.Programs;

import java.io.IOException;

@SuppressWarnings("serial")
@WebServlet(loadOnStartup = 1, urlPatterns = {"/weather"})
public class WeatherServlet extends HttpServlet {
  @Override 
  public void doGet(HttpServletRequest request, HttpServletResponse response) 
      throws IOException {
      response.getOutputStream().print(Programs.runProgram(request.getServletPath(), ImmutableMap.<String, Object>of("text", request.getParameter("text"))));
  }
}
