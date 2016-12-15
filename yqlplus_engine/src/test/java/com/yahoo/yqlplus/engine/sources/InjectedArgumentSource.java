/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Injected;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.engine.sources.UnrulyRequestSource.UnrulyRequestHandle;
import com.yahoo.yqlplus.engine.sources.UnrulyRequestSource.UnrulyRequestRecord;

public class InjectedArgumentSource implements Source {
    @Query
    public UnrulyRequestRecord scan(@Injected UnrulyRequestHandle handle) {
        return new UnrulyRequestRecord(handle.getRequestId());
    }

    @Query
    public UnrulyRequestRecord scan(int id) {
        return new UnrulyRequestRecord(id);
    }

}
