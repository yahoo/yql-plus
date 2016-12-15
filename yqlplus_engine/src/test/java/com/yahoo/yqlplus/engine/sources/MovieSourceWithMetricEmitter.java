/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.yahoo.cloud.metrics.api.Duration;
import com.yahoo.cloud.metrics.api.TaskMetricEmitter;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.*;
import com.yahoo.yqlplus.api.annotations.Set;

import java.util.*;

public class MovieSourceWithMetricEmitter implements Source {

    private final Map<String, Movie> movies = new LinkedHashMap<>();

    @Insert
    public Movie insertMovie(final @Set("uuid") @DefaultValue("DEFAULT_INSERT_UUID") String uuid,
                             final @Set("title") String title,
                             final @Set("category") String category,
                             final @Set("prodDate") String prodDate,
                             final @Set("duration") @DefaultValue("122") Integer duration,
                             final @Set("reviews") @DefaultValue("DEFAULT_INSERT_REVIEW_1, DEFAULT_INSERT_REVIEW_2") List<String> reviews,
                             final @Set("newRelease") @DefaultValue("true") Boolean newRelease,
                             final @Set("rating") @DefaultValue("0x22") Byte rating,
                             final @Emitter TaskMetricEmitter taskMetricEmitter) {
        TaskMetricEmitter subTaskEmitter = null;
        Duration requestLatency = null;
        if (taskMetricEmitter != null) {
            subTaskEmitter =  taskMetricEmitter.start("subtask", "createResponse");
            requestLatency = taskMetricEmitter.start("requestLatency");
        }
        Movie movie = new Movie(uuid, title, category, prodDate, duration, reviews, newRelease, rating, null);
        movies.put(uuid, movie);
        if (subTaskEmitter != null) {
            subTaskEmitter.end();
        }
        if (requestLatency != null) {
            requestLatency.end();
        }
        return movie;
    }

    @Update
    public Movie updateMovie(final @Set("category") @DefaultValue("DEFAULT_UPDATE_CATEGORY") String category,
                             final @Set("duration") Integer duration,
                             final @Set("reviews") @DefaultValue("DEFAULT_UPDATE_REVIEW_1, DEFAULT_UPDATE_REVIEW_2") List<String> reviews,
                             final @Key("uuid") String uuid,
                             final @Emitter TaskMetricEmitter taskMetricEmitter) {
        TaskMetricEmitter subTaskEmitter =  taskMetricEmitter.start("subtask", "createResponse");
        Duration requestLatency = taskMetricEmitter.start("requestLatency");
        Movie movie = movies.get(uuid);
        if (movie != null) {
            movie.setCategory(category);
            movie.setDuration(duration);
            movie.setReviews(reviews);
        }
        subTaskEmitter.end();
        requestLatency.end();
        return movie;
    }

    @Update
    public Iterable<Movie> updateAllMovies(final @Set("category") String category,
                                           final @Emitter TaskMetricEmitter taskMetricEmitter) {
        TaskMetricEmitter subTaskEmitter =  taskMetricEmitter.start("subtask", "createResponse");
        Duration requestLatency = taskMetricEmitter.start("requestLatency");
        Iterator<Movie> iterator = movies.values().iterator();
        while(iterator.hasNext()) {
            Movie movie = iterator.next();
            movie.setCategory(category);
        }
        subTaskEmitter.end();
        requestLatency.end();
        return movies.values();
    }

    @Delete
    public Movie deleteMovie(final @Key("uuid") String uuid, final @Emitter TaskMetricEmitter taskMetricEmitter) {
        TaskMetricEmitter subTaskEmitter =  taskMetricEmitter.start("subtask", "createResponse");
        Duration requestLatency = taskMetricEmitter.start("requestLatency");
        Movie movie = movies.remove(uuid);
        subTaskEmitter.end();
        requestLatency.end();
        return movie;
    }
}
