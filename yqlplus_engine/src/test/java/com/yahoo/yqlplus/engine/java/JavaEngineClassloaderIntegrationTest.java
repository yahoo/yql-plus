/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.java;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.yahoo.sample.EngineContainerInterface;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * A test to capture the scenarios the engine will encounter when used in an app server.
 * <p/>
 * <p/>
 * 1 classloader for the API
 * <p/>
 * 1 classloader for the engine
 * <p/>
 * 1..n classloaders for the sources
 */
@Test
public class JavaEngineClassloaderIntegrationTest {
    static final MappingJsonFactory JSON_FACTORY = new MappingJsonFactory();

    public Pattern toPattern(String... packageNames) {
        return Pattern.compile("^(" + Joiner.on(")|(").join(Iterables.transform(Arrays.asList(packageNames), new Function<String, Object>() {
            @Override
            public Object apply(String input) {
                return Pattern.quote(input);
            }
        })) + ").*");
    }

    public static class MyLoader extends ClassLoader {
        private static final boolean NOISY = false;
        private final String name;
        private final Pattern pattern;
        private final Path root;
        private final Map<String, Class<?>> classes = Maps.newHashMap();

        MyLoader(String name, Pattern pattern, Path root) {
            this.name = name;
            this.pattern = pattern;
            this.root = root;
        }

        MyLoader(ClassLoader parent, String name, Pattern pattern, Path root) {
            super(parent);
            this.name = name;
            this.pattern = pattern;
            this.root = root;
        }

        public Class<?> loadClass(String name) throws ClassNotFoundException {
            return loadClass(name, true);
        }

        private Class<?> getClass(String name) throws ClassNotFoundException {
            if (classes.containsKey(name)) {
                return classes.get(name);
            }
            String path = name.replace(".", "/") + ".class";
            Path targetClass = root.resolve(path);
            try {
                byte[] data = Files.readAllBytes(targetClass);
                Class<?> clazz = defineClass(name, data, 0, data.length);
                classes.put(name, clazz);
                return clazz;
            } catch (IOException e) {
                throw new ClassNotFoundException(name);
            }

        }

        @Override
        protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            Class<?> clazz;
            if (pattern.matcher(name).matches()) {
                if (NOISY) {
                    System.err.println("D " + this.name + " " + name);
                }
                clazz = getClass(name);
                if (resolve) {
                    resolveClass(clazz);
                }
            } else {
                if (NOISY && "system".equals(this.name)) {
                    System.err.println("P " + this.name + " " + name);
                }
                clazz = super.loadClass(name, resolve);
            }
            return clazz;
        }
    }

    @Test(enabled = false)
    public void testCustomClassloaderGate() throws Exception {
        // OK, so our classloader hierarchy
        // system
        //    - java.lang
        // common
        //    - the api package
        // engine
        //    - everything in com.yahoo.yqlplus.engine
        // container
        //    - com.yahoo.sample.Container
        // each source
        //    - their package


        // system <- common <- engine <- container
        // system <- common <- source

        Path engine = Paths.get(".").toAbsolutePath().getParent();
        if (!"yqlplus_engine".equals(engine.getFileName().toString())) {
            engine = engine.resolve("yqlplus_engine");
        }
        Path test = engine.resolve("target/test-classes");
        Path engineCore = engine.resolve("target/classes");
        Path apiCore = engine.resolve("../yqlplus_source_api/target/classes");

        // lets us see what makes it to the system classloader
        ClassLoader leakLoader = new MyLoader("system", toPattern("com.yahoo.example"), apiCore);
        ClassLoader commonLoader = new MyLoader(leakLoader, "common", toPattern("com.yahoo.yqlplus.api"), apiCore);
        ClassLoader engineCoreLoader = new MyLoader(commonLoader, "engine", toPattern("com.yahoo.yqlplus.engine"), engineCore);
        ClassLoader sourceLoader = new MyLoader(commonLoader, "source", toPattern("com.yahoo.sample.integration"), test);
        ClassLoader containerLoader = new MyLoader(engineCoreLoader, "container", toPattern("com.yahoo.sample.Container"), test);

        Class<?> containerClass = containerLoader.loadClass("com.yahoo.sample.Container");
        EngineContainerInterface container = (EngineContainerInterface) containerClass.newInstance();
        Class<?> sourceClass = sourceLoader.loadClass("com.yahoo.sample.integration.SimpleSource");
        Map<String, JsonNode> result = container.run("SELECT * FROM simple OUTPUT as f1;", "simple", sourceClass);
        JsonNode node = result.get("f1");
        JsonNode tree = JSON_FACTORY.createParser("[{\"id\":\"1\",\"value\":\"joe\",\"score\":0}]").readValueAsTree();
        Assert.assertEquals(node, tree);
    }


}
