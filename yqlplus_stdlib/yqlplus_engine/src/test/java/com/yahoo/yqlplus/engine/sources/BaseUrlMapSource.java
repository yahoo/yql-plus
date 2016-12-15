/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Query;

public class BaseUrlMapSource implements Source {

    @Query
    public PropertyDO getPropertyByBaseUrl(@Key("baseUrl") final String baseUrl) {
        return new PropertyDO(baseUrl);
    }

    public static final class PropertyDO {
        private String baseUrl;

        public PropertyDO(String baseUrl) {
            this.baseUrl = baseUrl;
        }

        public String getBaseUrl() {
            return baseUrl;
        }

        @Override
        public String toString() {
            return "PropertyDO [baseUrl=" + baseUrl + "]";
        }
    }
}
