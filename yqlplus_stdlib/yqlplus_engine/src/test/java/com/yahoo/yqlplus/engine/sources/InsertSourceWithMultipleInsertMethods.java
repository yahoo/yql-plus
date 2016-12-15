/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.DefaultValue;
import com.yahoo.yqlplus.api.annotations.Insert;
import com.yahoo.yqlplus.api.annotations.Set;
import java.util.List;

public class InsertSourceWithMultipleInsertMethods implements Source {

    @Insert
    public Movie insertMovie1(@Set("uuid") @DefaultValue("DEFAULT_UUID") String uuid,
                              @Set("title") String title,
                              @Set("category") String category,
                              @Set("prodDate") String prodDate,
                              @Set("duration") Integer duration,
                              @Set("reviews") List<String> reviews) {
        return new Movie(uuid, title, category, prodDate, duration, reviews, true, null, null);
    }

    @Insert
    public Movie insertMovie2(@Set("uuid") @DefaultValue("DEFAULT_UUID") String uuid,
                              @Set("title") String title,
                              @Set("category") String category,
                              @Set("prodDate") String prodDate,
                              @Set("duration") Integer duration,
                              @Set("reviews") List<String> reviews) {
        return new Movie(uuid, title, category, prodDate, duration, reviews, true, null, null);
    }
}
