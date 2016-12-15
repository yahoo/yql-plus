/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.java;

public class Image {

    public String imageId;
    public String filename;

    public Image(String id, String filename) {
        this.imageId = id;
        this.filename = filename;
    }
}
