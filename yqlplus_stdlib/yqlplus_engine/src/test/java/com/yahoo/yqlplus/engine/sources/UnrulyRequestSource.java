/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.google.inject.Inject;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.ExecuteScoped;
import com.yahoo.yqlplus.api.annotations.Query;

public class UnrulyRequestSource implements Source {
	
    private UnrulyRequestHandle handle;

    @Inject
    UnrulyRequestSource(UnrulyRequestHandle handle) {
        this.handle = handle;
    }

    @Query
    public UnrulyRequestRecord scan() {
        return new UnrulyRequestRecord(handle.getRequestId());
    }
    
    public static class UnrulyRequestRecord {
        public final int id;

        public UnrulyRequestRecord(int id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            UnrulyRequestRecord that = (UnrulyRequestRecord) o;

            if (id != that.id) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return id;
        }

        @Override
        public String toString() {
            return "UnrulyRequestRecord{" +
                    "id=" + id +
                    '}';
        }
    }
    
    @ExecuteScoped
    public static class UnrulyRequestHandle {
        private int id;

        public UnrulyRequestHandle(int id) {
            this.id = id;
        }

        public int getRequestId() {
            return id;
        }
    }
}
