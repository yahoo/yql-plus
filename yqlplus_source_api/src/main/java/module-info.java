module com.yahoo.yqlplus.api {
    requires java.logging;

    requires com.google.common;

    exports com.yahoo.yqlplus.api;
    exports com.yahoo.yqlplus.api.index;
    exports com.yahoo.yqlplus.api.trace;
    exports com.yahoo.yqlplus.api.types;
    exports com.yahoo.yqlplus.api.annotations;
}
