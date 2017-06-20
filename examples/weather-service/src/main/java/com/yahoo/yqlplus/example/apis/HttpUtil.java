/*
 * Copyright (c) 2017 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.example.apis;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MediaType;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class HttpUtil {
    private static final Client httpClient = ClientBuilder.newClient();;
    private static final Gson GSON = new Gson();
    private static final JsonParser jsonParser = new JsonParser();
    
    public static Client getHttpClient() {
        return httpClient;
    }

    public static JsonObject getJsonResponse(String uri) throws InterruptedException, ExecutionException, TimeoutException {      
        String request = httpClient.target(uri)
                  .request(MediaType.APPLICATION_JSON_TYPE)
                  .get(String.class);
        return jsonParser.parse(request).getAsJsonObject();
    }
    
    public static JsonObject getJsonResponse(String uri, String name, Object value) throws InterruptedException, ExecutionException, TimeoutException {      
        String request = httpClient.target(uri)
                  .queryParam(name, value)
                  .request(MediaType.APPLICATION_JSON_TYPE)
                  .get(String.class);
        return jsonParser.parse(request).getAsJsonObject();
    }
    
    public static JsonObject getJsonResponse(String uri, String name1, Object value1, String name2, Object value2) throws InterruptedException, ExecutionException, TimeoutException {      
        String request = httpClient.target(uri)
                  .queryParam(name1, value1)
                  .queryParam(name2, value2)
                  .request(MediaType.APPLICATION_JSON_TYPE)
                  .get(String.class);
        return jsonParser.parse(request).getAsJsonObject();
    }
    
    public static Gson getGson() {
        return GSON;
    }
}
