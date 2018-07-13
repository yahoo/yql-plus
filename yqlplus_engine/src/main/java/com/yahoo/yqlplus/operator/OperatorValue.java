/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.operator;

import com.yahoo.yqlplus.api.types.Annotations;

public class OperatorValue implements Value {
    private String name;
    private Step step;
    private final boolean hasDatum;
    private final Annotations annotations;

    public OperatorValue(boolean hasDatum, Annotations annotations) {
        this.hasDatum = hasDatum;
        this.annotations = annotations;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    void setStep(Step step) {
        this.step = step;
    }

    public boolean hasDatum() {
        return hasDatum;
    }

    public Annotations getAnnotations() {
        return annotations;
    }

    @Override
    public Step getSource() {
        return step;
    }

    @Override
    public String toString() {
        return "OperatorValue{" +
                "name='" + name + '\'' +
                ", hasDatum=" + hasDatum +
                ", annotations=" + annotations +
                '}';
    }
}
