/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.*;

import java.util.LinkedHashMap;
import java.util.Map;

public class MovieSourceWithLongUuid implements Source {

    private final Map<Long, Movie> movies = new LinkedHashMap<>();

    public void insertMovie(Movie movie) {
        movies.put(movie.getUuid(), movie);
    }

    @Query
    public Movie getMovie(@Key("uuid") Long uuid) {
        return movies.get(uuid);
    }

    public static class Movie {

        private final Long uuid;
        private String title;
        private String category;

        public Movie(Long uuid, String title, String category) {
            this.uuid = uuid;
            this.title = title;
            this.category = category;
        }

        public Long getUuid() {
            return uuid;
        }

        public String getTitle() {
            return title;
        }

        public String getCategory() {
            return category;
        }
    }

}
