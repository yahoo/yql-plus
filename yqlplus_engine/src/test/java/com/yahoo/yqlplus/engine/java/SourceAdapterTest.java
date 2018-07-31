package com.yahoo.yqlplus.engine.java;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Insert;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.api.annotations.Rank;
import com.yahoo.yqlplus.api.annotations.Set;
import com.yahoo.yqlplus.api.types.YQLTypeException;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.ProgramResult;
import com.yahoo.yqlplus.engine.YQLPlusCompiler;
import com.yahoo.yqlplus.engine.sources.Movie;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.logical.StatementOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Comparator;
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



    public static class IndexedSource implements Source {
        // prefer to query by ID rather than value when possible
        @Query
        @Rank(20)
        public static Person personsById(@Key("id") String id) {
            return new Person(id, "joe", 10);
        }

        @Query
        @Rank(10)
        public static Person personsByValue(@Key("value") String value) {
            return new Person("0", value, 1);
        }

        @Insert
        public static Person insertById(@Set("id") String id) {
            return new Person(id, "joe", 10);
        }

        @Insert
        public static Person insertByIdAndValue(@Set("id") String id, @Set("value") String value) {
            return new Person(id, value, 5);
        }

        @Insert
        public static Person insertByIdAndValueAndScore(@Set("id") String id, @Set("value") String value, @Set("score") int score) {
            return new Person(id, value, score);
        }

        @Insert
        public static List<Person> insertByValue(@Set("value") String value) {
            return  ImmutableList.of(new Person("0", value, 1));
        }
    }

    @Test
    public void requireRankAnnotation() throws Exception {
        YQLPlusCompiler compiler = createCompiler("source", IndexedSource.class);
        String programStr = "SELECT * FROM source WHERE id = '1' AND value = 'joe' OUTPUT AS f1;";
        CompiledProgram program = compiler.compile(programStr);
        ProgramResult rez = program.run(ImmutableMap.of());
        List<Person> f1 = rez.getResult("f1").get().getResult();
        Assert.assertEquals(f1.size(), 1);
        Assert.assertEquals(f1.get(0), new Person("1", "joe", 10));
    }

    @Test
    public void requireRankAnnotationNegative() throws Exception {
        YQLPlusCompiler compiler = createCompiler("source", IndexedSource.class);
        String programStr = "SELECT * FROM source WHERE id = '1' AND value = 'smith' OUTPUT AS f1;";
        CompiledProgram program = compiler.compile(programStr);
        ProgramResult rez = program.run(ImmutableMap.of());
        List<Person> f1 = rez.getResult("f1").get().getResult();
        Assert.assertEquals(f1.size(), 0);
    }

    @Test
    public void requireRankAnnotationLesser() throws Exception {
        YQLPlusCompiler compiler = createCompiler("source", IndexedSource.class);
        String programStr = "SELECT * FROM source WHERE value = 'smith' OUTPUT AS f1;";
        CompiledProgram program = compiler.compile(programStr);
        ProgramResult rez = program.run(ImmutableMap.of());
        List<Person> f1 = rez.getResult("f1").get().getResult();
        Assert.assertEquals(f1.size(), 1);
        Assert.assertEquals(f1.get(0), new Person("0", "smith", 1));
    }

    private static OperatorNode<ExpressionOperator> createRecord(Object... input) {
        List<String> keys = Lists.newArrayList();
        List<OperatorNode<ExpressionOperator>> values = Lists.newArrayList();
        for(int i = 0; i < input.length; i += 2) {
            keys.add((String)input[i]);
            values.add(OperatorNode.create(ExpressionOperator.LITERAL, input[i+1]));
        }
        return OperatorNode.create(ExpressionOperator.MAP, keys, values);
    }

    @Test
    public void requireMatchingInsert() throws Exception {
        // we have to create the operator AST for this test because it's not expressible in the language
        // but since it IS expressible in the operator tree and the SourceAdapter implements support for it
        // it seemed best to test it...
        YQLPlusCompiler compiler = createCompiler("source", IndexedSource.class);
        List<OperatorNode<StatementOperator>> statements = Lists.newArrayList();
        OperatorNode<SequenceOperator> insert = OperatorNode.create(SequenceOperator.INSERT,
                OperatorNode.create(SequenceOperator.SCAN, ImmutableList.of("source"), ImmutableList.of()),
                OperatorNode.create(SequenceOperator.EVALUATE,
                        OperatorNode.create(ExpressionOperator.ARRAY,
                                ImmutableList.of(
                                    createRecord("id", "1"),
                                    createRecord("value", "2"),
                                    createRecord("id", "100", "value", "bob"),
                                    createRecord("id", "101", "value", "second bob", "score", 1000)
                                )))
                );
        statements.add(OperatorNode.create(StatementOperator.EXECUTE, insert, "f1"));
        statements.add(OperatorNode.create(StatementOperator.OUTPUT, "f1"));
        OperatorNode<StatementOperator> programAst = OperatorNode.create(StatementOperator.PROGRAM, statements);
        CompiledProgram program = compiler.compile(programAst);
        ProgramResult rez = program.run(ImmutableMap.of());
        List<Person> f1 = rez.getResult("f1").get().getResult();
        f1.sort(Comparator.comparing(Person::getScore));
        Assert.assertEquals(f1.size(), 4);
        Assert.assertEquals(f1.get(0), new Person("0", "2", 1));
        Assert.assertEquals(f1.get(1), new Person("100", "bob", 5));
        Assert.assertEquals(f1.get(2), new Person("1", "joe", 10));
        Assert.assertEquals(f1.get(3), new Person("101", "second bob", 1000));
    }
}
