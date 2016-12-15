/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.java;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.ProgramResult;
import com.yahoo.yqlplus.engine.YQLPlusCompiler;
import com.yahoo.yqlplus.engine.YQLResultSet;
import com.yahoo.yqlplus.engine.api.Record;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

public class DeepJoinTest {

    public static class Zone {
        private String id;

        public Zone(String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }
    }

    public static class ZoneSource implements Source {

        @Query
        public List<Zone> scan() {
            return ImmutableList.of(
                    new Zone("east"),
                    new Zone("west"),
                    new Zone("north")
            );
        }
    }

    public static class Domain {
        private String name;

        public Domain(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }


    public static class Model  {
        private String zone;
        private String domain;
        private String service;
        private String name;
        private int count;

        public Model(String zone, String domain, String service, String name, int count) {
            this.zone = zone;
            this.domain = domain;
            this.service = service;
            this.name = name;
            this.count = count;
        }

        public String getzone() {
            return zone;
        }

        public String getDomain() {
            return domain;
        }

        public String getService() {
            return service;
        }

        public String getName() {
            return name;
        }

        public int getCount() {
            return count;
        }
    }

    public static class ModelSource implements Source {

        @Query
        public List<Model> listModels(@Key("zone") String zone) {
            switch(zone) {
                case "east":
                    return ImmutableList.of(
                            new Model("east", "webservice", "hodor", "blue", 1),
                            new Model("east", "webservice", "hodor", "green", 1)
                    );
                case "west":
                    return ImmutableList.of(
                            new Model("west", "webservice", "door", "blue", 1),
                            new Model("west", "webservice", "door", "green", 1)
                    );
                default:
                    return ImmutableList.of();
            }
        }
    }

    public static class Machine {
        private String id;
        private String zone;
        private String domain;
        private String service;
        private String name;
        private float cpu;
        private int mem;

        public Machine(String id, String zone, String domain, String service, String name, float cpu, int mem) {
            this.id = id;
            this.zone = zone;
            this.domain = domain;
            this.service = service;
            this.name = name;
            this.cpu = cpu;
            this.mem = mem;
        }

        public String getzone() {
            return zone;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getDomain() {
            return domain;
        }

        public void setDomain(String domain) {
            this.domain = domain;
        }

        public String getService() {
            return service;
        }

        public void setService(String service) {
            this.service = service;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public float getCpu() {
            return cpu;
        }

        public void setCpu(float cpu) {
            this.cpu = cpu;
        }

        public int getMem() {
            return mem;
        }

        public void setMem(int mem) {
            this.mem = mem;
        }
    }

    public static class MachineSource implements Source {
        @Query
        public List<Machine> byService(@Key("zone") String zoneId, @Key("domain") String domain, @Key("service") String service, @Key("name") String name) {
            switch(zoneId) {
                case "east":
                    switch (domain) {
                        case "webservice":
                            if(!"hodor".equals(service)) {
                                return ImmutableList.of();
                            }
                            switch(name) {
                                case "blue":
                                    return ImmutableList.of(
                                            new Machine("11", zoneId, domain, service, name, 1.0f, 10),
                                            new Machine("12", zoneId, domain, service, name, 1.0f, 10),
                                            new Machine("13", zoneId, domain, service, name, 1.0f, 10)
                                    );
                                case "green":
                                    return ImmutableList.of(
                                            new Machine("01", zoneId, domain, service, name, 1.0f, 20),
                                            new Machine("02", zoneId, domain, service, name, 1.0f, 20),
                                            new Machine("03", zoneId, domain, service, name, 1.0f, 20)
                                    );
                                default:
                                    return ImmutableList.of();
                            }
                        default:
                            return ImmutableList.of();
                    }
                case "west":
                    switch (domain) {
                        case "webservice":
                            if(!"door".equals(service)) {
                                return ImmutableList.of();
                            }
                            switch(name) {
                                case "blue":
                                    return ImmutableList.of(
                                            new Machine("111", zoneId, domain, service, name, 1.0f, 10),
                                            new Machine("112", zoneId, domain, service, name, 1.0f, 10),
                                            new Machine("113", zoneId, domain, service, name, 1.0f, 10)
                                    );
                                case "green":
                                    return ImmutableList.of(
                                            new Machine("101", zoneId, domain, service, name, 1.0f, 20),
                                            new Machine("102", zoneId, domain, service, name, 1.0f, 20),
                                            new Machine("103", zoneId, domain, service, name, 1.0f, 20)
                                    );
                                default:
                                    return ImmutableList.of();
                            }
                        default:
                            return ImmutableList.of();
                    }
            }
            return ImmutableList.of();
        }
    }

    public static class GenerateMachinesource implements Source {
        @Query
        public List<Machine> byService(@Key("zone") String zoneId, @Key("domain") String domain, @Key("service") String service, @Key("name") String name) {
            return ImmutableList.of(new Machine("11", zoneId, domain, service, name, 1.0f, 10));
        }
    }

    @Test
    public void testDeepJoin() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(),
                new SourceBindingModule("zones", ZoneSource.class,
                        "models", ModelSource.class,
                        "machines", MachineSource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile(
                        "SELECT p.*" +
                        "FROM zones c " +
                        " JOIN models i ON c.id = i.zone " +
                        " JOIN machines p ON p.zone = i.zone AND p.domain = i.domain AND p.service = i.service AND p.name = i.name " +
                "OUTPUT AS foo;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Record> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 12);
    }
    
    @Test
    public void testDeepJoinCaseInsensitive() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(),
                new SourceBindingModule("zones", ZoneSource.class,
                        "models", ModelSource.class,
                        "machines", MachineSource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile(
                        " SELECT p.*" +
                        " FROM zones c " +
                        " JOIN models i ON c.id = i.zone " +
                        " JOIN machines p ON p.zone = i.ZoNe AND p.domain = i.Domain AND p.serVice = i.serviCe AND p.NAME = i.NAME " +
                        " OUTPUT AS foo;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Record> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 12);
    }

    @Test
    public void testCompoundKey() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(),
                new SourceBindingModule("zones", ZoneSource.class,
                        "models", ModelSource.class,
                        "machines", MachineSource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile(
                "SELECT * " +
                        "FROM machines " +
                        "WHERE zone = 'east' AND domain = 'webservice' AND service = 'hodor' AND name IN ('blue', 'green') " +
                        "OUTPUT AS foo;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Record> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 6);
    }

    @Test
    public void testGeneratemachines() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule(),
                new SourceBindingModule("zones", ZoneSource.class,
                        "models", ModelSource.class,
                        "machines", GenerateMachinesource.class));
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile(
                "SELECT * " +
                        "FROM machines " +
                        "WHERE zone = 'east' AND domain = 'webservice' AND service = 'hodor' AND name IN ('blue', 'green') " +
                        "ORDER BY name " +
                        "OUTPUT AS foo;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Machine> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 2);
        Assert.assertEquals(foo.get(0).domain, "webservice");
        Assert.assertEquals(foo.get(1).domain, "webservice");
        Assert.assertEquals(foo.get(0).name, "blue");
        Assert.assertEquals(foo.get(1).name, "green");
    }
}
