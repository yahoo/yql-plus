/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.java;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class Person {

    private String id;
    private String value;
    private int score;

    public Person(String id, String value, int score) {
        this.id = id;
        this.value = value;
        this.score = score;
    }

    public Person() {
    }

    public String getId() {
        return id;
    }

    public String getValue() {
        return value;
    }

    public int getScore() {
        return score;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setValue(String value) {
        this.value = value;
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

    @JsonIgnore
    public int getIidPrimitive() {
        return Integer.parseInt(id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Person person = (Person) o;

        if (score != person.score) return false;
        if (!id.equals(person.id)) return false;
        if (!value.equals(person.value)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + value.hashCode();
        result = 31 * result + score;
        return result;
    }

    @Override
    public String toString() {
        return "Person{" +
                "id='" + id + '\'' +
                ", value='" + value + '\'' +
                ", score=" + score +
                '}';
    }
}
