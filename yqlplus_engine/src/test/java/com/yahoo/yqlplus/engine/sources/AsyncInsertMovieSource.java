/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.DefaultValue;
import com.yahoo.yqlplus.api.annotations.Insert;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.api.annotations.Set;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class AsyncInsertMovieSource implements Source {

    private final Map<String, Movie> movies = new LinkedHashMap<>();

    @Query
    public Iterable<Movie> getMovies() {
        return movies.values();
    }

    @Insert
    public ListenableFuture<Movie> insertMovie(
            @Set("uuid") @DefaultValue("DEFAULT_UUID") String uuid,
            @Set("title") String title,
            @Set("category") String category,
            @Set("prodDate") String prodDate,
            @Set("duration") @DefaultValue("122") Integer duration,
            @Set("reviews") List<String> reviews,
            @Set("newRelease") @DefaultValue("true") Boolean newRelease,
            @Set("rating") @DefaultValue("0x22") Byte rating,
            @Set("cast") @DefaultValue("Various") String cast) {
        Movie movie = new Movie(uuid, title, category, prodDate, duration, reviews, newRelease, rating, cast);
        movies.put(uuid, movie);
        return Futures.immediateFuture(movie);
    }
}
