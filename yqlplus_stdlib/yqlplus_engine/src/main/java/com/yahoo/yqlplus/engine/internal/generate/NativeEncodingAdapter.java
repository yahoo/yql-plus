/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.generate;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.yahoo.tbin.TBinEncoder;
import com.yahoo.yqlplus.engine.api.InvocationResultHandler;
import com.yahoo.yqlplus.engine.api.NativeEncoding;
import com.yahoo.yqlplus.engine.api.NativeInvocationResultHandler;

import java.io.OutputStream;

public class NativeEncodingAdapter implements InvocationResultHandler {
    static final MappingJsonFactory JSON_FACTORY = new MappingJsonFactory();
    private final NativeEncoding encoding;
    private final NativeInvocationResultHandler resultHandler;
    private final NativeSerialization serializer;

    public NativeEncodingAdapter(NativeEncoding encoding, NativeInvocationResultHandler resultHandler, NativeSerialization serializer) {
        this.encoding = encoding;
        this.resultHandler = resultHandler;
        this.serializer = serializer;
    }

    @Override
    public void fail(Throwable t) {
        resultHandler.fail(t);
    }

    @Override
    public void succeed(String name, Object value) {
        try {
            OutputStream target = resultHandler.createStream(name);
            switch(encoding) {
                case TBIN: {
                    TBinEncoder generator = new TBinEncoder(target);
                    serializer.writeTBin(generator, value);
                    target.close();
                    break;
                }
                case JSON: {
                    JsonGenerator generator = JSON_FACTORY.createGenerator(target);
                    serializer.writeJson(generator, value);
                    generator.close();
                    break;
                }
                default:
                    throw new UnsupportedOperationException("Unsupported native serialization type: " + encoding);
            }
            resultHandler.succeed(name);
        } catch(Exception e) {
            resultHandler.fail(e);
        } catch(Error e) {
            resultHandler.fail(e);
            throw e;
        }
    }

    @Override
    public void fail(String name, Throwable t) {
        resultHandler.fail(name, t);
    }

    @Override
    public void end() {
        resultHandler.end();
    }
}
