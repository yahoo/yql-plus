/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.*;
import com.yahoo.yqlplus.api.annotations.Set;

import java.util.*;

public class LongDoubleMovieSource implements Source {

    private final Map<String, LongMovie> movies = new LinkedHashMap<>();

    @Query
    public Iterable<LongMovie> getMovies() {
        return movies.values();
    }
    
    @Insert
    public LongMovie insertMovie(final @Set("uuid") @DefaultValue("DEFAULT_INSERT_UUID") String uuid,
                             final @Set("title") String title,
                             final @Set("category") String category,
                             final @Set("prodDate") String prodDate,
                             final @Set("duration") @DefaultValue("122") Integer duration,
                             final @Set("longDuration") @DefaultValue("122") Long longDuration,
                             final @Set("doubleDuration") @DefaultValue("122") Double doubleDuration,
                             final @Set("reviews") @DefaultValue("DEFAULT_INSERT_REVIEW_1, DEFAULT_INSERT_REVIEW_2") List<String> reviews,
                             final @Set("newRelease") @DefaultValue("true") Boolean newRelease,
                             final @Set("rating") @DefaultValue("0x22") Byte rating,
                             final @Set("cast") @DefaultValue("Various") String cast) {
        LongMovie movie = new LongMovie(uuid, title, category, prodDate, duration, longDuration, doubleDuration, reviews, newRelease, rating, cast);
        movies.put(uuid, movie);
        return movie;
    }
}
