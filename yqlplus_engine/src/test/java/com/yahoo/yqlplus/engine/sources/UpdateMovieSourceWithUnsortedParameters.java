/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.DefaultValue;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Set;
import com.yahoo.yqlplus.api.annotations.Update;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class UpdateMovieSourceWithUnsortedParameters implements Source {

    private final Map<String, Movie> movies = new LinkedHashMap<>();

    public void insertMovie(Movie movie) {
        movies.put(movie.getUuid(), movie);
    }

    @Update
    public Movie updateMovie(
            @Set("category") @DefaultValue("DEFAULT_UPDATE_CATEGORY") String category,
            @Key("uuid") String uuid,
            @Set("duration") Integer duration,
            @Set("reviews") @DefaultValue("DEFAULT_UPDATE_REVIEW_1, DEFAULT_UPDATE_REVIEW_2") List<String> reviews) {
        Movie movie = movies.get(uuid);
        if (movie != null) {
            movie.setCategory(category);
            movie.setDuration(duration);
            movie.setReviews(reviews);
        }
        return movie;
    }

}
