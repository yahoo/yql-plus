module com.yahoo.yqlplus.guice {
    requires javax.inject;
    requires com.yahoo.yqlplus.api;
    requires com.yahoo.yqlplus.language;
    requires com.yahoo.yqlplus.engine;
    requires com.yahoo.yqlplus.stdlib;
    requires com.google.guice;
    requires com.google.common;
    exports com.yahoo.yqlplus.guice;
}