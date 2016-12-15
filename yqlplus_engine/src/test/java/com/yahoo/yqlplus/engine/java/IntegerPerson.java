/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.java;

public class IntegerPerson {

    private Integer id;
    private Integer value;
    private int score;

    public IntegerPerson(Integer id, Integer value, int score) {
        this.id = id;
        this.value = value;
        this.score = score;
    }

    public Integer getId() {
        return id;
    }

    public Integer getValue() {
        return value;
    }

    public int getScore() {
        return score;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IntegerPerson)) {
            return false;
        }
        IntegerPerson otherPerson = (IntegerPerson) o;
        return (score == otherPerson.score && id.equals(otherPerson.id) && value.equals(otherPerson.value));
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + value.hashCode();
        result = 31 * result + score;
        return result;
    }

}
