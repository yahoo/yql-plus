/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.java;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Query;

import java.util.List;

public class ImageSource implements Source {

    private final List<Image> items;
    private final Multimap<String, Image> imageMap = ArrayListMultimap.create();

    ImageSource(List<Image> items) {
        this.items = items;
        for (Image item : items) {
            imageMap.put(item.imageId, item);
        }
    }

    @Query
    public List<Image> scan() {
        return items;
    }

    @Query
    public Iterable<Image> get(@Key("imageId") String imageId) {
        if (null == imageId) {
            throw new IllegalArgumentException("Null imageId");
        }
        return imageMap.get(imageId);
    }
}
