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

    exports com.yahoo.yqlplus.engine.scope;
    exports com.yahoo.yqlplus.engine.tools;
    exports com.yahoo.yqlplus.engine.guice;
    exports com.yahoo.yqlplus.engine.rules;

    // guice and modules do not get along so well. generated code also.
    opens com.yahoo.yqlplus.engine.internal.bytecode;
    exports com.yahoo.yqlplus.engine.internal.bytecode;
}