/*
 * Copyright (c) 2017 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.example.apis;

import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Query;

public class WeatherSource implements Source {
    static final String BASE_URL = "http://query.yahooapis.com/v1/public/yql";
    @Query
    public List<Forecast> getForecast(@Key("woeid") String woeid) throws InterruptedException, ExecutionException, TimeoutException, UnsupportedEncodingException {
        JsonObject jsonObject = HttpUtil.getJsonResponse(BASE_URL, "q", "select * from weather.forecast where u = 'f' and woeid = " + woeid);
        JsonArray jsonArray = (JsonArray)jsonObject.getAsJsonObject("query").getAsJsonObject("results").getAsJsonObject("channel").getAsJsonObject("item").get("forecast");
        Iterator<JsonElement> it = jsonArray.iterator();
        List<Forecast> forecasts = Lists.newArrayList();
        while(it.hasNext()) {
            JsonElement ele = it.next();
            ForecastBase tmp = HttpUtil.getGson().fromJson(ele.toString(), ForecastBase.class);
            forecasts.add(new Forecast(tmp.getCode(),  tmp.getDate(), tmp.getDay(), tmp.getHigh(),
                    tmp.getLow(), tmp.getText(), woeid, "f"));
        }
        return forecasts;
    }
    
    @Query
    public List<Forecast> getForecast(@Key("woeid") String woeid, @Key("u") String u) throws InterruptedException, ExecutionException, TimeoutException, UnsupportedEncodingException {
        JsonObject jsonObject = HttpUtil.getJsonResponse(BASE_URL.replace("{woeid}", woeid).replace("{u}", u));
        JsonArray jsonArray = (JsonArray)jsonObject.getAsJsonObject("query").getAsJsonObject("results").getAsJsonObject("channel").getAsJsonObject("item").get("forecast");
        Iterator<JsonElement> it = jsonArray.iterator();
        List<Forecast> forecasts = Lists.newArrayList();
        while(it.hasNext()) {
            JsonElement ele = it.next();
            ForecastBase tmp = HttpUtil.getGson().fromJson(ele.toString(), ForecastBase.class);
            forecasts.add(new Forecast(tmp.getCode(),  tmp.getDate(), tmp.getDay(), tmp.getHigh(),
                    tmp.getLow(), tmp.getText(), woeid, u));
        }
        return forecasts;
    }
    
    public static class ForecastBase {
        protected final String code;
        protected final String date;
        protected final String day;
        protected final int high;
        protected final int low;
        protected final String text;

        public ForecastBase(String code, String date, String day, int high,
                int low, String text) {
            this.code = code;
            this.date = date;
            this.day = day;
            this.high = high;
            this.low = low;
            this.text = text;
        }

        public String getCode() {
            return code;
        }
        public String getDate() {
            return date;
        }
        public String getDay() {
            return day;
        }
        public int getHigh() {
            return high;
        }
        public int getLow() {
            return low;
        }
        public String getText() {
            return text;
        }

        @Override
        public String toString() {
            return "ForecastBase [code=" + code + ", date=" + date + ", day="
                    + day + ", high=" + high + ", low=" + low + ", text="
                    + text + "]";
        }
    }

    public static class Forecast extends ForecastBase {
        protected final String woeid;
        protected final String u;
        public Forecast(String code, String date, String day, int high,
                int low, String text, String woeid, String u) {
            super(code, date, day, high, low, text);
            this.woeid = woeid;
            this.u = u;
        }

        public String getWoeid() {
            return woeid;
        }

        public String getU() {
            return u;
        }
    }
}
