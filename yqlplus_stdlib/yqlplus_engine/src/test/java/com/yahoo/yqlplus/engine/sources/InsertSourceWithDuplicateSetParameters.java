/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.DefaultValue;
import com.yahoo.yqlplus.api.annotations.Insert;
import com.yahoo.yqlplus.api.annotations.Set;
import java.util.List;

public class InsertSourceWithDuplicateSetParameters implements Source {

    /**
     * Method declares two @Set annotated parameters with name 'prodDate', which must result in compilation failure.
     *
     * @param uuid
     * @param title
     * @param category
     * @param prodDate1
     * @param duration
     * @param reviews
     * @param prodDate2
     * @return
     */
    @Insert
    public Movie insertMovie(@Set("uuid") @DefaultValue("DEFAULT_UUID") String uuid,
                             @Set("title") String title,
                             @Set("category") String category,
                             @Set("prodDate") String prodDate1,
                             @Set("duration") Integer duration,
                             @Set("reviews") List<String> reviews,
                             @Set("prodDate") String prodDate2) {
        return new Movie(uuid, title, category, prodDate1, duration, reviews, true, null, null);
    }
}
