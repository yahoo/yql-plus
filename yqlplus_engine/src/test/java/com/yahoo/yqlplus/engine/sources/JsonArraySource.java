/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.TextNode;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Query;

public class JsonArraySource implements Source {
    @Query
    public JsonResult getJsonArray(int count) {
        JsonNodeFactory jsonNodeFactory = JsonNodeFactory.instance;
        ArrayNode arrayNode = new ArrayNode(jsonNodeFactory);
        for (int i = 0; i < count; i++) {
            arrayNode.add(i);
        }
        JsonResult jsonResult = new JsonResult();
        jsonResult.jsonNode = arrayNode;
        return jsonResult;
    }
    
    @Query
    public JsonResult getJsonNode() {
        TextNode textnode =  new TextNode("textNode");
        JsonResult jsonResult = new JsonResult();
        jsonResult.jsonNode = textnode;
        return jsonResult;
    }
    
    public static class JsonResult {
        public JsonNode jsonNode;
    }
}
