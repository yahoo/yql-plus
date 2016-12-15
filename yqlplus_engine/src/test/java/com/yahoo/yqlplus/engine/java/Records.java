/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.java;

import com.yahoo.yqlplus.engine.api.Record;

import java.util.Iterator;

public class Records {

    static int getRecordSize(Record record) {
        Iterator<String> iter = record.getFieldNames().iterator();
        int num = 0;
        while (iter.hasNext()) {
            num++;
            iter.next();
        }
        return num;
    }
}
