/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Set;
import com.yahoo.yqlplus.api.annotations.Update;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class AsyncUpdateMovieSource implements Source {

    private final Map<String, Movie> movies = new LinkedHashMap<>();

    public void insertMovie(Movie movie) {
        movies.put(movie.getUuid(), movie);
    }

    @Update
    public ListenableFuture<Movie> updateAllMovies(@Set("title") String title,
                                                   @Set("category") String category,
                                                   @Key("uuid") String uuid) {
        Movie movie = movies.get(uuid);
        if (movie != null) {
            movie.setTitle(title);
            movie.setCategory(category);
            return Futures.immediateFuture(movie);
        } else {
            return null;
        }
    }
}
