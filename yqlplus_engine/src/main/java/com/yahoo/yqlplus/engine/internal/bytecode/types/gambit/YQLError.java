/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode.types.gambit;

import com.google.common.collect.Lists;

import java.util.List;


public class YQLError {
    public static YQLError create(Throwable failure) {
        if (failure instanceof YQLRuntimeException) {
            return ((YQLRuntimeException) failure).getError();
        }
        YQLError error = new YQLError();
        error.setMessage(failure.getMessage());
        error.setDetails(YQLError.extractDetails(failure));
        error.setCause(failure);
        return error;
    }

    private static List<StackTrace> extractDetails(Throwable failure) {
        // TODO: be lazy about extracting these details
        List<StackTrace> result = Lists.newArrayList();
        Throwable current = failure;
        while (current != null) {
            if (current instanceof YQLRuntimeException) {
                YQLError error = ((YQLRuntimeException) current).getError();
                result.addAll(error.getDetails());
                break;
            }
            StackTrace trace = extractTrace(failure);
            if (trace != null) {
                result.add(trace);
            }
            current = current.getCause();
        }
        return result;
    }

    private static StackTrace extractTrace(Throwable failure) {
        StackTraceElement[] trace = failure.getStackTrace();
        if (trace == null || trace.length == 0) {
            return null;
        }
        StackFrame[] frames = new StackFrame[trace.length];
        for (int i = 0; i < trace.length; ++i) {
            frames[i] = extractFrame(trace[i]);
        }
        StackTrace out = new StackTrace();
        out.setMessage(failure.getMessage());
        out.setFrames(frames);
        return out;
    }

    private static StackFrame extractFrame(StackTraceElement stackTraceElement) {
        StackFrame frame = new StackFrame();
        frame.setLine(stackTraceElement.getLineNumber());
        frame.setName(stackTraceElement.getFileName() + ":" + stackTraceElement.getClassName() + ":" + stackTraceElement.getMethodName());
        return frame;
    }


    public static class StackFrame {
        private Integer line;
        private Integer offset;
        private String name;

        public Integer getLine() {
            return line;
        }

        public void setLine(Integer line) {
            this.line = line;
        }

        public Integer getOffset() {
            return offset;
        }

        public void setOffset(Integer offset) {
            this.offset = offset;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static class StackTrace {
        private String message;
        private StackFrame[] frames;

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public StackFrame[] getFrames() {
            return frames;
        }

        public void setFrames(StackFrame[] frames) {
            this.frames = frames;
        }
    }

    private Integer code;
    private String type;
    private String message;
    private List<StackTrace> details;
    private Throwable cause;

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public List<StackTrace> getDetails() {
        return details;
    }

    public void setDetails(List<StackTrace> details) {
        this.details = details;
    }

    public Throwable getCause() {
        return cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
    }
}
