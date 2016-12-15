/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Query;

public class KeyTypeSources {

    public static class KeyTypeRecord {
        public String id;
        public Integer value;
        public int value2;

        public KeyTypeRecord(String id, Integer value, int value2) {
            this.id = id;
            this.value = value;
            this.value2 = value2;
        }
    }

    public static class KeyTypeSource1 implements Source {
        @Query
        public KeyTypeRecord getRecord(@Key("id") Integer id) {
            return null;
        }
    }


    public static class KeyTypeSource2 implements Source {
        @Query
        public KeyTypeRecord getRecord(@Key("value") int id) {
            return new KeyTypeRecord(String.valueOf(id), id, id);
        }
    }

    public static class KeyTypeSource3 implements Source {
        @Query
        public KeyTypeRecord getRecord(@Key("value2") Integer id) {
            return null;
        }
    }

    public static class KeyTypeSource4 implements Source {
        @Query
        public KeyTypeRecord getRecord(@Key("fancypants") String id) {
            return null;
        }
    }


}
