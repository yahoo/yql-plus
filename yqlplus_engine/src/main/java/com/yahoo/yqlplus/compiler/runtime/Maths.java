/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.runtime;

import com.yahoo.yqlplus.flow.internal.dynalink.FlowBootstrapper;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;

public final class Maths {
    public static final Maths INSTANCE = new Maths();

    private MethodHandle dynamicMath = FlowBootstrapper.publicBootstrap(null, "dyn:callMethod:binaryMath",
            MethodType.methodType(Object.class, Maths.class, ArithmeticOperation.class, Object.class, Object.class)).dynamicInvoker();
    private MethodHandle dynamicNegate = FlowBootstrapper.publicBootstrap(null, "dyn:callMethod:negate",
            MethodType.methodType(Object.class, Maths.class, Object.class)).dynamicInvoker();

    private Maths() {

    }

    public int binaryMath(ArithmeticOperation op, int l, int r) {
        switch (op) {
            case ADD:
                return l + r;
            case SUB:
                return l - r;
            case MULT:
                return l * r;
            case DIV:
                return l / r;
            case MOD:
                return l % r;
            case POW:
                return (int) Math.pow(l, r);
            default:
                throw new UnsupportedOperationException();
        }
    }

    public long binaryMath(ArithmeticOperation op, long l, long r) {
        switch (op) {
            case ADD:
                return l + r;
            case SUB:
                return l - r;
            case MULT:
                return l * r;
            case DIV:
                return l / r;
            case MOD:
                return l % r;
            case POW:
                return (long) Math.pow(l, r);
            default:
                throw new UnsupportedOperationException();
        }
    }

    public float binaryMath(ArithmeticOperation op, float l, float r) {
        switch (op) {
            case ADD:
                return l + r;
            case SUB:
                return l - r;
            case MULT:
                return l * r;
            case DIV:
                return l / r;
            case MOD:
                return l % r;
            case POW:
                return (float) Math.pow(l, r);
            default:
                throw new UnsupportedOperationException();
        }
    }

    public double binaryMath(ArithmeticOperation op, double l, double r) {
        switch (op) {
            case ADD:
                return l + r;
            case SUB:
                return l - r;
            case MULT:
                return l * r;
            case DIV:
                return l / r;
            case MOD:
                return l % r;
            case POW:
                return Math.pow(l, r);
            default:
                throw new UnsupportedOperationException();
        }
    }

    public Object dynamicMath(ArithmeticOperation op, Object l, Object r) {
        if (l == null || r == null) {
            return null;
        }
        try {
            return dynamicMath.invokeExact(this, op, l, r);
        } catch (Error | RuntimeException e) {
            throw e;
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    public int negate(int input) {
        return -input;
    }

    public long negate(long input) {
        return -input;
    }

    public float negate(float input) {
        return -input;
    }

    public double negate(double input) {
        return -input;
    }

    public Object dynamicNegate(Object input) {
        if (input == null) {
            return null;
        }
        try {
            return dynamicNegate.invokeExact(this, input);
        } catch (Error | RuntimeException e) {
            throw e;
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

}
