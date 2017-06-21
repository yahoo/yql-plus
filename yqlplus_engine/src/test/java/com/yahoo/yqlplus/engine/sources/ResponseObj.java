/*
 * Copyright (c) 2017 Yahoo Holdings
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

public class ResponseObj {

    private boolean status;
    private String errorCode;
    private String id;

    public boolean isStatus() {
        return status;
    }

    public void setStatus(boolean status) {
        this.status = status;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
      return "ResponseObj [status=" + status + ", errorCode=" + errorCode
          + ", id=" + id + "]";
    }
}
