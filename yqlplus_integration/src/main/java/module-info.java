module com.yahoo.yqlplus.integration {
    requires java.logging;
    requires java.sql;

    requires com.yahoo.yqlplus.api;
    requires com.yahoo.yqlplus.engine;
    requires com.google.common;
    requires undertow.core;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.core;

    exports com.yahoo.yqlplus.integration.sources;
}