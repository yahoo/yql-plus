package com.yahoo.yqlplus.compiler.runtime;

import java.util.Map;

public class MapFieldWriter implements FieldWriter {
    private final Map<String, Object> targetMap;

    public MapFieldWriter(Map<String, Object> targetMap) {
        this.targetMap = targetMap;
    }

    @Override
    public void put(String fieldName, Object value) {
        targetMap.put(fieldName, value);
    }
}
