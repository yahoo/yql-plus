/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.runtime;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Utility class to generate N choose K combinations from a list.
 */
public class Chooser<T> {
    public Iterable<List<T>> chooseN(int n, List<T> input) {
        if (n == input.size()) {
            final List<List<T>> result = Lists.newArrayListWithCapacity(1);
            final List<T> copy = Lists.newArrayList(input);
            result.add(copy);
            return result;
        } else if (n == 1) {
            return Iterables.transform(input, new Function<T, List<T>>() {

                @Override
                public List<T> apply(T input) {
                    final List<T> res = Lists.newArrayListWithCapacity(1);
                    res.add(input);
                    return res;
                }
            });
        } else if (n > input.size()) {
            return ImmutableList.of();
        } else {
            // should use an exact sized list, or better yet don't materialize the list
            List<List<T>> output = Lists.newArrayList();
            chooseN(n, 0, Lists.newArrayListWithCapacity(n), input, output);
            return output;
        }
    }

    private void chooseN(int n, int i, List<T> terms, List<T> input, List<List<T>> output) {
        if (n == terms.size()) {
            output.add(Lists.newArrayList(terms));
        } else {
            final int last = terms.size();
            for (int j = i; j < input.size(); ++j) {
                terms.add(input.get(j));
                chooseN(n, j + 1, terms, input, output);
                terms.remove(last);
            }
        }
    }

    public static <T> Chooser<T> create() {
        return new Chooser<>();
    }
}
