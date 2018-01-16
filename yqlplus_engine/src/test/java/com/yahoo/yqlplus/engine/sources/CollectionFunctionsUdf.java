/*
 * Copyright (c) 2018 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import java.util.List;

import com.yahoo.yqlplus.api.Exports;
import com.yahoo.yqlplus.api.annotations.Export;
import com.yahoo.yqlplus.engine.api.Record;

public class CollectionFunctionsUdf implements Exports {

  @Export
  public static Record[] asArray(final List<Record> list) {
    return list.toArray(new Record[list.size()]);
  }
}
