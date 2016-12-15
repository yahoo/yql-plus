/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.java;

public class Minion {
    public String master_id;
    public String minion_id;

    public Minion(String master_id, String minion_id) {
        this.master_id = master_id;
        this.minion_id = minion_id;
    }
}
