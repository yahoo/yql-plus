module com.yahoo.yqlplus.language {
    requires jsr305;

    requires com.google.common;
    requires com.google.guice;
    requires com.fasterxml.jackson.core;
    requires com.fasterxml.jackson.databind;

    requires antlr4.runtime;

    exports com.yahoo.yqlplus.language.logical;
    exports com.yahoo.yqlplus.language.operator;
    exports com.yahoo.yqlplus.language.parser;
}