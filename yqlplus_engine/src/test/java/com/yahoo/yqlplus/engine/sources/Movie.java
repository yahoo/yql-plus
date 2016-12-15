/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import java.util.List;

public class Movie {

    private final String uuid;
    private String title;
    private String category;
    private final String prodDate;
    private Integer duration;
    private List<String> reviews;
    private final Boolean newRelease;
    private final Byte rating;
    private final String cast;

    public Movie(String uuid) {
        this(uuid, null, null, null, null, null, null, null, null);
    }

    public Movie(String uuid, String title, String category, String prodDate, Integer duration, List<String> reviews,
                 Boolean newRelease, Byte rating, String cast) {
        this.uuid = uuid;
        this.title = title;
        this.category = category;
        this.prodDate = prodDate;
        this.duration = duration;
        this.reviews = reviews;
        this.newRelease = newRelease;
        this.rating = rating;
        this.cast = cast;
    }

    public String getUuid() {
        return uuid;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getTitle() {
        return title;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getCategory() {
        return category;
    }

    public String getProdDate() {
        return prodDate;
    }

    public void setDuration(Integer duration) {
        this.duration = duration;
    }

    public Integer getDuration() {
        return duration;
    }

    public void setReviews(List<String> reviews) {
        this.reviews = reviews;
    }

    public List<String> getReviews() {
        return reviews;
    }

    public boolean isNewRelease() {
        return newRelease;
    }

    public byte getRating() {
        return rating;
    }

    public String getCast() { return cast; }
}
