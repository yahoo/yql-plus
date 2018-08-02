package com.yahoo.yqlplus.engine.java;

import com.yahoo.cloud.metrics.api.MetricEvent;
import com.yahoo.cloud.metrics.api.MetricSink;
import com.yahoo.cloud.metrics.api.RequestEvent;
import com.yahoo.cloud.metrics.api.RequestMetricSink;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MetricTestSink implements MetricSink, RequestMetricSink {
    BlockingQueue<RequestEvent> requestEvents = new LinkedBlockingQueue<>();

    @Override
    public void emitRequest(RequestEvent reuqestEvent) {
        requestEvents.offer(reuqestEvent);
    }

    @Override
    public void emit(MetricEvent metricEvent) {
        // TODO Auto-generated method stub
    }

    public RequestEvent getRequestEvent() throws InterruptedException {
        return requestEvents.poll(30, TimeUnit.MILLISECONDS);
    }
}
