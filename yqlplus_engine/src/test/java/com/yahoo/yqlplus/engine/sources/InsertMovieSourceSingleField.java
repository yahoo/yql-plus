/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Insert;
import com.yahoo.yqlplus.api.annotations.Set;

public class InsertMovieSourceSingleField implements Source {

    @Insert
    public Movie insertMovie(@Set("uuid") String uuid) {
        return new Movie(uuid);
    }
}
