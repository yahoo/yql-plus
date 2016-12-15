/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

/*
 * The JS runtime library for the JS Flow generator.
 *
 */

function resultToArray(result) {
    if (result.length == undefined) {
        return [result];
    } else {
        return result;
    }
}
//         out.println("var %s = doJoin(%s, %s, %s, %s, %s);", inner, leftKeyFunc, rightKeyFunc, left.getName(), right.getName());

function merge(target, source) {
    for (var key in source) {
        if (source.hasOwnProperty(key)) {
            target[key] = source[key];
        }
    }
}

function doJoin(inner, leftKeyFunc, rightKeyFunc, left, right) {
    var table = {};
    for (var i = 0; i < right.length; i++) {
        var row = right[i];
        var key = rightKeyFunc(row);
        table[key] = i;
    }
    var result = [];
    for (var i = 0; i < left.length; i++) {
        var row = left[i];
        var key = leftKeyFunc(row);
        var matched_idx = table[key];
        if (matched_idx != undefined) {
            var rrow = {};
            merge(rrow, row);
            merge(rrow, right[matched_idx]);
            result.push(rrow);
        } else if (!inner) {
            result.push(row);
        }
    }
    return result;
}

function print(msg) {
    java.lang.System.err.println(msg);
}

function createJoin(number, cb) {
    var _arguments = {};
    var counter = number;
    return new Packages.org.mozilla.javascript.Synchronizer(function (args) {
        merge(_arguments, args);
        counter -= 1;
        if (counter == 0) {
            cb(_arguments);
        }
    });
}

function start(func, args) {
    JS_POOL.submit({ run: function () {
        func.apply(null, args);
    }});
}
