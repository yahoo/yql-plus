/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Set;
import com.yahoo.yqlplus.api.annotations.Update;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class UpdateMovieSource implements Source {

    private final Map<String, Movie> movies = new LinkedHashMap<>();

    public void insertMovie(Movie movie) {
        movies.put(movie.getUuid(), movie);
    }

    @Update
    public Iterable<Movie> updateAllMovies(@Set("title") String title, @Set("category") String category) {
        Iterator<Movie> iterator = movies.values().iterator();
        while(iterator.hasNext()) {
            Movie movie = iterator.next();
            movie.setTitle(title);
            movie.setCategory(category);
        }
        return movies.values();
    }

    /**
     * Batch update
     *
     * @param title
     * @param category
     * @param uuids
     * @return
     */
    @Update
    public Iterable<Movie> updateMovies(@Set("title") String title, @Set("category") String category,
                                        @Key("uuid") List<String> uuids) {
        for (String uuid: uuids) {
            Movie movie = movies.get(uuid);
            if (movie != null) {
                movie.setTitle(title);
                movie.setCategory(category);
            }
        }
        return movies.values();
    }
}
