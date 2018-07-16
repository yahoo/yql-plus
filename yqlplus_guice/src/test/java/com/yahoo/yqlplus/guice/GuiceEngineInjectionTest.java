package com.yahoo.yqlplus.guice;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.ProgramResult;
import com.yahoo.yqlplus.engine.YQLPlusCompiler;
import com.yahoo.yqlplus.engine.YQLResultSet;
import com.yahoo.yqlplus.engine.api.Record;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Test
public class GuiceEngineInjectionTest {
    public static class Person {

        private String id;
        private String value;
        private int score;

        public Person(String id, String value, int score) {
            this.id = id;
            this.value = value;
            this.score = score;
        }

        public Person() {
        }

        public String getId() {
            return id;
        }

        public String getValue() {
            return value;
        }

        public int getScore() {
            return score;
        }

        public void setId(String id) {
            this.id = id;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public void setScore(int score) {
            this.score = score;
        }

        @JsonIgnore
        public String getOtherId() {
            return id;
        }

        @JsonIgnore
        public Integer getIid() {
            return Integer.parseInt(id);
        }

        @JsonIgnore
        public int getIidPrimitive() {
            return Integer.parseInt(id);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Person person = (Person) o;

            if (score != person.score) return false;
            if (!id.equals(person.id)) return false;
            return value.equals(person.value);
        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
            result = 31 * result + value.hashCode();
            result = 31 * result + score;
            return result;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "id='" + id + '\'' +
                    ", value='" + value + '\'' +
                    ", score=" + score +
                    '}';
        }
    }

    public static class PersonSource implements Source {
        private static final AtomicInteger index = new AtomicInteger(0);
        private List<Person> items;

        public PersonSource(List<Person> items) {
            this.items = items;
        }

        @Query
        public List<Person> scan() {
            index.getAndIncrement();
            return items;
        }

        public static void resetIndex() {
            index.set(0);
        }

        public static int getIndex() {
            return index.get();
        }
    }

    @Test
    public void requireGuiceInjectedSource() throws Exception {
        YQLPlusCompiler compiler = Guice.createInjector(new JavaEngineModule(),
                new SourceBindingModule("people", new PersonSource(ImmutableList.of(new Person("1", "bob", 0), new Person("2", "joe", 1), new Person("3", "smith", 2))))
        ).getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT * FROM people OUTPUT AS foo;");
        ProgramResult myResult = program.run(ImmutableMap.of());
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Record> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 3);

        // Assert that Class is maintained
        Assert.assertTrue(foo.get(0) instanceof Person, "Class " + Person.class.getName() + " not maintained by SELECT *");
        Person person = (Person) foo.get(0);
        Assert.assertEquals(person.getId(), "1");
        Assert.assertEquals(person.getValue(), "bob");
        Assert.assertEquals(person.getScore(), 0);

    }
}
