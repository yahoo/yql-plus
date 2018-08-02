/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.sample.integration;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class SimpleRecord {
    private String id;
    private String name;
    private int score;

    public SimpleRecord(String id, String name, int score) {
        this.id = id;
        this.name = name;
        this.score = score;
    }

    public SimpleRecord() {
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getScore() {
        return score;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setScore(int score) {
        this.score = score;
    }

    @JsonIgnore
    public String getOtherId() {
        return id;
    }

    @JsonIgnore
    public Integer getIid() {
        return Integer.parseInt(id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SimpleRecord person = (SimpleRecord) o;

        if (score != person.score) return false;
        if (!id.equals(person.id)) return false;
        return name.equals(person.name);
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + score;
        return result;
    }

    @Override
    public String toString() {
        return "Person{" +
                "id='" + id + '\'' +
                ", value='" + name + '\'' +
                ", score=" + score +
                '}';
    }
}
