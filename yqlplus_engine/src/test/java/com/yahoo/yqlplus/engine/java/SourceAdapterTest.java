package com.yahoo.yqlplus.engine.java;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.api.types.YQLTypeException;
import com.yahoo.yqlplus.engine.YQLPlusCompiler;
import com.yahoo.yqlplus.engine.sources.Movie;
import org.testng.annotations.Test;

import java.util.List;

public class SourceAdapterTest extends ProgramTestBase {
    @Test(expectedExceptions = YQLTypeException.class, expectedExceptionsMessageRegExp = ".*@Key column 'category' is a List [(]batch[)]; a method must either be entirely-batch or entirely-not")
    public void requireBatchMismatchTest() throws Exception {
        YQLPlusCompiler compiler = createCompiler("source", new Source() {
            @Query
            public Movie query(@Key("uuid") String key, @Key("category") List<String> keys) {
                return null;
            }
        });
        compiler.compile("PROGRAM (@uuid string, @updateDuration int32);\n" +
                "SELECT * FROM source WHERE key = '1' OUTPUT AS out;");
    }

    @Test(expectedExceptions = YQLTypeException.class, expectedExceptionsMessageRegExp = ".*@Key column 'category' is a single value but other parameters are batch; a method must either be entirely-batch or entirely-not")
    public void requireBatchMismatchTest2() throws Exception {
        YQLPlusCompiler compiler = createCompiler("source", new Source() {
                    @Query
                    public Movie query(@Key("uuid") List<String> keys, @Key("category") String key) {
                        return null;
                    }
                });
        compiler.compile("PROGRAM (@uuid string, @updateDuration int32);\n" +
                "SELECT * FROM source WHERE key = '1' OUTPUT AS out;");
    }

    @Test(expectedExceptions = YQLTypeException.class, expectedExceptionsMessageRegExp = ".*@Key[(]'uuid'[)] used multiple times")
    public void requireKeyUsedMultipleTimes() throws Exception {
        YQLPlusCompiler compiler = createCompiler("source", new Source() {
                    @Query
                    public Movie query(@Key("uuid") String key, @Key("uuid") String key2) {
                        return null;
                    }
                });
        compiler.compile("PROGRAM (@uuid string, @updateDuration int32);\n" +
                "SELECT * FROM source WHERE key = '1' OUTPUT AS out;");
    }
}
