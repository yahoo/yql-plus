/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MapArgumentSource implements Source {

    @Query
    public MapInfo getMapInfo(Map<String, String> mapArgument) {
        if (null == mapArgument) {
            throw new IllegalArgumentException("Null mapArgument");
        }
        return new MapInfo(mapArgument);
    }

    public static class MapInfo {

        private final Map<String, String> mapArgument;

        public Map<String, String> getMap() {
            return mapArgument;
        }

        public MapInfo(Map<String, String> mapArgument) {
            this.mapArgument = mapArgument;
        }

        public int getMapSize() {
            return mapArgument.size();
        }

        public String getMapValues() {
            List<String> list = new ArrayList<>(mapArgument.values());
            Collections.sort(list);
            Iterator<String> iter = list.iterator();
            StringBuilder sb = new StringBuilder();
            while (iter.hasNext()) {
                sb.append(iter.next());
            }
            return sb.toString();
        }
    }
}
