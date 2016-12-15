/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.*;

import java.util.List;

public class MovieSourceDefaultValueWithoutSet implements Source {

    @Insert
    public Movie insertMovie(final @Set("uuid") @DefaultValue("DEFAULT_INSERT_UUID") String uuid,
                             final @Set("title") String title,
                             final @Set("category") String category,
                             final @Set("prodDate") String prodDate,
                             final @DefaultValue("122") Integer duration,
                             final @Set("reviews") @DefaultValue("DEFAULT_INSERT_REVIEW_1, DEFAULT_INSERT_REVIEW_2") List<String> reviews,
                             final @Set("newRelease") @DefaultValue("true") Boolean newRelease,
                             final @Set("rating") @DefaultValue("0x22") Byte rating) {
        throw new AssertionError("Should never reach here");
    }

    @Update
    public Movie updateMovie(final @Set("category") @DefaultValue("DEFAULT_UPDATE_CATEGORY") String category,
                             final @Set("duration") Integer duration,
                             final @DefaultValue("DEFAULT_UPDATE_REVIEW_1, DEFAULT_UPDATE_REVIEW_2") List<String> reviews,
                             final @Key("uuid") String uuid) {
        throw new AssertionError("Should never reach here");
    }
}
