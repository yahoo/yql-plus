/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

exports.fancy = function (cb) {
    cb([
        {id: 'a', name: "Bob"},
        {id: 'b', name: "Joe"},
        {id: 'c', name: "Smith"}
    ]);
};

exports.ugly = function (keys, cb) {
    cb(keys.map(function (key) {
        return {id: key, name: key};
    }));
};

exports.split = function (input, fieldName, cb) {
    var result = [];
    var arr = input.split(',');
    for (var i = 0; i < arr.length; i++) {
        var o = {};
        o[fieldName] = arr[i];
        result.push(o);
    }
    cb(result);
};


exports.addfield = function (input, name, value, cb) {
    var result = [];
    for (var i = 0; i < input.length; ++i) {
        // we want a shallow copy of each row
        var row = {};
        merge(row, input[i]);
        row[name] = value;
        result.push(row);
    }
    cb(result);
};