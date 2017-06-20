/*
 * Copyright (c) 2017 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.example.apis;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Query;

public class GeoSource implements Source {
   static final String BASE_URL = "http://query.yahooapis.com/v1/public/yql";
   
   @Query
   public Place getPlace(@Key("text") String text) throws InterruptedException, ExecutionException, TimeoutException {
       JsonObject jsonObject = HttpUtil.getJsonResponse(BASE_URL, "q", "select * from geo.places where text='" + text + "'");
       JsonPrimitive jsonObj = jsonObject.getAsJsonObject("query").getAsJsonObject("results").getAsJsonObject("place")
               .getAsJsonPrimitive("woeid");
       return new Place(text, jsonObj.getAsString());
   }
   
   public static class Place {
       private String text;
       private String woeid;
       
       public Place(String text, String woeid) {
           this.text = text;
           this.woeid = woeid;
       }
       
       public String getText() {
           return this.text;
       }
       
       public String getWoeid() {
           return this.woeid;
       }

       @Override
       public String toString() {
           return "Place [text=" + text + ", woeid=" + woeid + "]";
       }
   }
}
