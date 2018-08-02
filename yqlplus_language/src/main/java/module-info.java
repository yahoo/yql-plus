module com.yahoo.yqlplus.language {
    requires com.google.common;
    requires com.google.guice;

    requires antlr4.runtime;

    exports com.yahoo.yqlplus.language.logical;
    exports com.yahoo.yqlplus.language.operator;
    exports com.yahoo.yqlplus.language.parser;
}