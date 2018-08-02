/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.java;

public class PrimitiveRecord {
    public char p_char;
    public Character b_char;

    public byte p_byte;
    public Byte b_byte;

    public short p_short;
    public Short b_short;

    public int p_int;
    public Integer b_int;

    public long p_long;
    public Long b_long;

    public boolean p_boolean;
    public Boolean b_boolean;

    public float p_float;
    public Float b_float;

    public double p_double;
    public Double b_double;

    public PrimitiveRecord(char c, byte b, short s, int i, long l, boolean bool, float f, double d) {
        this.p_char = c;
        this.b_char = c;
        this.p_byte = b;
        this.b_byte = b;
        this.p_short = s;
        this.b_short = s;
        this.p_int = i;
        this.b_int = i;
        this.p_long = l;
        this.b_long = l;
        this.p_float = f;
        this.b_float = f;
        this.p_double = d;
        this.b_double = d;
    }

    public PrimitiveRecord(Character c, Byte b, Short s, Integer i, Long l, Boolean bool, Float f, Double d) {
        if (c != null) {
            this.p_char = c;
        }
        this.b_char = c;
        if (b != null) {
            this.p_byte = b;
        }
        this.b_byte = b;
        if (s != null) {
            this.p_short = s;
        }
        this.b_short = s;
        if (i != null) {
            this.p_int = i;
        }
        this.b_int = i;
        if (l != null) {
            this.p_long = l;
        }
        this.b_long = l;
        if (f != null) {
            this.p_float = f;
        }
        this.b_float = f;
        if (d != null) {
            this.p_double = d;
        }
        this.b_double = d;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PrimitiveRecord that = (PrimitiveRecord) o;

        if (p_boolean != that.p_boolean) return false;
        if (p_byte != that.p_byte) return false;
        if (p_char != that.p_char) return false;
        if (Double.compare(that.p_double, p_double) != 0) return false;
        if (Float.compare(that.p_float, p_float) != 0) return false;
        if (p_int != that.p_int) return false;
        if (p_long != that.p_long) return false;
        if (p_short != that.p_short) return false;
        if (b_boolean != null ? !b_boolean.equals(that.b_boolean) : that.b_boolean != null) return false;
        if (b_byte != null ? !b_byte.equals(that.b_byte) : that.b_byte != null) return false;
        if (b_char != null ? !b_char.equals(that.b_char) : that.b_char != null) return false;
        if (b_double != null ? !b_double.equals(that.b_double) : that.b_double != null) return false;
        if (b_float != null ? !b_float.equals(that.b_float) : that.b_float != null) return false;
        if (b_int != null ? !b_int.equals(that.b_int) : that.b_int != null) return false;
        if (b_long != null ? !b_long.equals(that.b_long) : that.b_long != null) return false;
        return b_short != null ? b_short.equals(that.b_short) : that.b_short == null;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = (int) p_char;
        result = 31 * result + (b_char != null ? b_char.hashCode() : 0);
        result = 31 * result + (int) p_byte;
        result = 31 * result + (b_byte != null ? b_byte.hashCode() : 0);
        result = 31 * result + (int) p_short;
        result = 31 * result + (b_short != null ? b_short.hashCode() : 0);
        result = 31 * result + p_int;
        result = 31 * result + (b_int != null ? b_int.hashCode() : 0);
        result = 31 * result + (int) (p_long ^ (p_long >>> 32));
        result = 31 * result + (b_long != null ? b_long.hashCode() : 0);
        result = 31 * result + (p_boolean ? 1 : 0);
        result = 31 * result + (b_boolean != null ? b_boolean.hashCode() : 0);
        result = 31 * result + (p_float != +0.0f ? Float.floatToIntBits(p_float) : 0);
        result = 31 * result + (b_float != null ? b_float.hashCode() : 0);
        temp = Double.doubleToLongBits(p_double);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (b_double != null ? b_double.hashCode() : 0);
        return result;
    }
}
