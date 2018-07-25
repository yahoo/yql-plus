package com.yahoo.yqlplus.engine.java;

import com.google.common.collect.ImmutableMap;
import com.yahoo.yqlplus.api.Exports;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Export;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.engine.YQLPlusCompiler;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

@Test()
public class RowDependencyTest extends ProgramTestBase {
    public static class Message {
        private String id;
        private String body;

        public Message(String id, String body) {
            this.id = id;
            this.body = body;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getBody() {
            return body;
        }

        public void setBody(String body) {
            this.body = body;
        }
    }
    public static class ExpensiveSource implements Source {
        @Query
        public List<Message> scan() {
            // This should not be called in the test below
            throw new IllegalStateException();
        }

        @Query
        public Message lookup(@Key("id") String id) {
            return new Message(id, id);
        }
    }

    public static class Gate implements Exports {
        @Export
        public boolean test(String name) {
            return name.equals("ok");
        }

        @Export
        public boolean fail(String name) {
            throw new IllegalArgumentException();
        }
    }


    // Verify that a row-independent filter is executed once and gates the rest of the query execution
    @Test
    public void requireRowIndependentExtraction() throws Exception {
        YQLPlusCompiler compiler = createCompiler("source", ExpensiveSource.class, "gate", Gate.class);
        List<Message> result = compiler.compile("SELECT * FROM source WHERE gate.test('not_ok') OUTPUT AS f1;")
                .run(ImmutableMap.of()).getResult("f1").get().getResult();
         Assert.assertTrue(result.isEmpty());
    }

    // Verify we simplify an AND with unconditionally false input
    @Test
    public void requireRowIndependentExtractionConstant() throws Exception {
        YQLPlusCompiler compiler = createCompiler("source", ExpensiveSource.class, "gate", Gate.class);
        List<Message> result = compiler.compile("SELECT * FROM source WHERE false AND gate.fail('not_ok') OUTPUT AS f1;")
                .run(ImmutableMap.of()).getResult("f1").get().getResult();
        Assert.assertTrue(result.isEmpty());
    }

    // Verify we do run the query when condition is true
    @Test
    public void requireRowIndependentExecution() throws Exception {
        YQLPlusCompiler compiler = createCompiler("source", ExpensiveSource.class, "gate", Gate.class);
        List<Message> result = compiler.compile("SELECT * FROM source WHERE id IN ('1', '2') AND gate.test('ok') ORDER BY id OUTPUT AS f1;")
                .run(ImmutableMap.of()).getResult("f1").get().getResult();
        Assert.assertEquals(result.size(), 2);
        Assert.assertEquals(result.get(0).id, "1");
        Assert.assertEquals(result.get(1).id, "2");
    }
}
