/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.java;

import com.google.common.collect.*;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Insert;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.api.annotations.Set;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.ProgramResult;
import com.yahoo.yqlplus.engine.YQLPlusCompiler;
import com.yahoo.yqlplus.engine.api.Record;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class CompoundKeysTest {

    public static class Toy {
        public final String id;
        public final String category;
        public final int score;

        public Toy(String id, String category, int score) {
            this.id = id;
            this.category = category;
            this.score = score;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Toy toy = (Toy) o;

            if (score != toy.score) return false;
            if (!category.equals(toy.category)) return false;
            if (!id.equals(toy.id)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
            result = 31 * result + category.hashCode();
            result = 31 * result + score;
            return result;
        }

        @Override
        public String toString() {
            return "Toy{" +
                    "id='" + id + '\'' +
                    ", category='" + category + '\'' +
                    ", score=" + score +
                    '}';
        }
    }

    public static class CompoundToySource implements Source {
        private final AtomicInteger sequence = new AtomicInteger(0);
        private final Map<String, Toy> idMap = Maps.newTreeMap();
        private final Multimap<String, Toy> categoryMap = ArrayListMultimap.create();
        private final Multimap<String, Toy> scoreCategoryMap = ArrayListMultimap.create();

        @Query
        public synchronized Toy lookup(@Key("id") String id) {
            return idMap.get(id);
        }

        @Query
        public synchronized Collection<Toy> lookupByCategory(@Key("category") String category) {
            return categoryMap.get(category);
        }

        @Query
        public synchronized Collection<Toy> lookupByScoreCategory(@Key("category") String category, @Key("score") int score) {
            return scoreCategoryMap.get(score + ":" + category);
        }

        @Insert
        public synchronized Toy insert(@Set("category") String category, @Set("score") int score) {
            Toy toy = new Toy(assignId(), category, score);
            ingest(toy);
            return toy;
        }

        private String assignId() {
            return String.valueOf(sequence.incrementAndGet());
        }

        private void ingest(Toy toy) {
            idMap.put(toy.id, toy);
            categoryMap.put(toy.category, toy);
            scoreCategoryMap.put(toy.score + ":" + toy.category, toy);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> List<T> execute(String programText) throws Exception {
        return (List<T>) execute(programText, ImmutableMap.<String, Object>of());

    }

    Injector injector;

    @BeforeClass
    public void setUp() {
        injector = Guice.createInjector(
                new JavaTestModule(),
                new SourceBindingModule("toys", new CompoundToySource(), "serviceMetrics", new ServiceMetricsSource())
        );
    }

    private List<Record> execute(String programText, ImmutableMap<String, Object> arguments) throws Exception {
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile(programText);
        ProgramResult rez = program.run(arguments, true);
        return rez.getResult("f1").get().getResult();
    }

    @Test
    public void testCompoundSingleKey() throws Exception {
        execute("INSERT INTO toys (category, score) VALUES ('food', 1) RETURNING id OUTPUT AS f1;");
        execute("INSERT INTO toys (category, score) VALUES ('food', 1) RETURNING id OUTPUT AS f1;");
        execute("INSERT INTO toys (category, score) VALUES ('hats', 2) RETURNING id OUTPUT AS f1;");
        List<Toy> toys = execute("SELECT * FROM toys WHERE id = '1' OUTPUT AS f1;");
        Assert.assertEquals(toys, ImmutableList.of(new Toy("1", "food", 1)));
    }

    @Test
    public void testCompoundCompoundKey() throws Exception {
        execute("INSERT INTO toys (category, score) VALUES ('food', 1) RETURNING id OUTPUT AS f1;");
        execute("INSERT INTO toys (category, score) VALUES ('food', 1) RETURNING id OUTPUT AS f1;");
        execute("INSERT INTO toys (category, score) VALUES ('hats', 2) RETURNING id OUTPUT AS f1;");
        List<Toy> toys = execute("SELECT * FROM toys WHERE category = 'food' AND score = 1 OUTPUT AS f1;");
        Assert.assertEquals(toys, ImmutableList.of(new Toy("1", "food", 1), new Toy("2", "food", 1)));
    }
    
    @Test
    public void testCompountKeyAlign() throws Exception {
        String programStr = "PROGRAM (); \n"+
                            "SELECT *  \n"+
                            "FROM serviceMetrics \n"+
                            "WHERE start = 'begin' \n"+
                            "AND serviceName IN ('service1', 'service2', 'service3') "+
                            "AND machineId = 'foo' \n"+
                            "AND modelId = 'foo' \n"+
                            "OUTPUT AS serviceMetrics; ";
      
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile(programStr);
        ProgramResult rez = program.run(ImmutableMap.<String, Object>of(), true);
        List<ServiceMetrics> serviceMetrics = rez.getResult("serviceMetrics").get().getResult();
        List<ServiceMetrics> expectedServiceMetrics = Lists.newArrayList(new ServiceMetrics("begin", "service1", "foo", "foo"),
                                                                new ServiceMetrics("begin", "service2", "foo", "foo"),
                                                                new ServiceMetrics("begin", "service3", "foo", "foo"));
        Assert.assertTrue(expectedServiceMetrics.containsAll(serviceMetrics));
        Assert.assertTrue(serviceMetrics.containsAll(expectedServiceMetrics));
    } 
    
    public class ServiceMetricsSource implements Source {
        @Query
        public ServiceMetrics getMetrics(@Key("start") final String start,                                     
                                       @Key("machineId") final String machineId,
                                       @Key("modelId") final String modelId,
                                       @Key("serviceName") final String serviceName) {
            return new ServiceMetrics(start, serviceName, machineId, modelId);
        }
    }
    
    public static class ServiceMetrics {
        private final String start;
        private final String serviceName;
        private final String machineId;
        private final String modelId;
        public ServiceMetrics(String start, String serviceName, String machineId, String modelId) {
            this.start = start;
            this.serviceName = serviceName;
            this.machineId = machineId;
            this.modelId = modelId;
        }
        public String getstart() {
            return start;
        }
        public String getServiceName() {
            return serviceName;
        }
        public String getmachineId() {
            return machineId;
        }
        public String getmodelId() {
            return modelId;
        }
        @Override
        public int hashCode() {
          final int prime = 31;
          int result = 1;
          result = prime * result
              + ((modelId == null) ? 0 : modelId.hashCode());
          result = prime * result
              + ((machineId == null) ? 0 : machineId.hashCode());
          result = prime * result
              + ((serviceName == null) ? 0 : serviceName.hashCode());
          result = prime * result + ((start == null) ? 0 : start.hashCode());
          return result;
        }
        @Override
        public boolean equals(Object obj) {
          if (this == obj)
            return true;
          if (obj == null)
            return false;
          if (getClass() != obj.getClass())
            return false;
          ServiceMetrics other = (ServiceMetrics) obj;
          if (modelId == null) {
            if (other.modelId != null)
              return false;
          } else if (!modelId.equals(other.modelId))
            return false;
          if (machineId == null) {
            if (other.machineId != null)
              return false;
          } else if (!machineId.equals(other.machineId))
            return false;
          if (serviceName == null) {
            if (other.serviceName != null)
              return false;
          } else if (!serviceName.equals(other.serviceName))
            return false;
          if (start == null) {
            if (other.start != null)
              return false;
          } else if (!start.equals(other.start))
            return false;
          return true;
        }
        
        @Override
        public String toString() {
          return "start = " + start + "," +
              "serviceName = " + serviceName + "," + 
              "machineId = " + machineId + "," +
              "modelId = " + modelId;
        }
    }
}
