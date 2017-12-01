/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

public class Sample {
  private String category;
  private int id;

  public Sample(String category, int id) {
      this.category = category;
      this.id = id;
  }

  public String getCategory() {
      return category;
  }

  public void setCategory(String category) {
      this.category = category;
  }

  public int getId() {
      return id;
  }

  public void setId(int id) {
      this.id = id;
  }
}
