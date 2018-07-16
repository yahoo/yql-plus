module com.yahoo.yqlplus.engine {
    requires java.logging;
    requires java.sql;

    requires javax.inject;

    requires org.objectweb.asm;
    requires org.objectweb.asm.util;
    requires com.google.common;
    requires com.google.guice;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.core;

    requires com.yahoo.yqlplus.api;
    requires com.yahoo.yqlplus.language;

    // automatic modules
    requires metrics.api;
    requires antlr4.runtime;
    requires dynalink;

    exports com.yahoo.yqlplus.engine.rules;
    exports com.yahoo.yqlplus.engine;
    exports com.yahoo.yqlplus.engine.library;
    exports com.yahoo.yqlplus.engine.api;
    exports com.yahoo.yqlplus.engine.compiler.code;
    exports com.yahoo.yqlplus.engine.compiler.runtime;
    exports com.yahoo.yqlplus.engine.indexed;
    exports com.yahoo.yqlplus.engine.source;
}