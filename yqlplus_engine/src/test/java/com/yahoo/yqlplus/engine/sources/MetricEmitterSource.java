/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.google.common.collect.ImmutableList;
import com.yahoo.cloud.metrics.api.Duration;
import com.yahoo.cloud.metrics.api.TaskMetricEmitter;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Emitter;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.engine.java.Person;

import java.util.List;

public class MetricEmitterSource implements Source {

    @Query
    public List<Person> getPerson(final @Emitter TaskMetricEmitter taskMetricEmitter) {
        TaskMetricEmitter subTaskEmitter = taskMetricEmitter.start("subtask", "createResponse");
        Duration duration = taskMetricEmitter.start("requestLatency");
        subTaskEmitter.end();
        duration.end();
        return ImmutableList.of(new Person("1", "joe", 0));
    }
}