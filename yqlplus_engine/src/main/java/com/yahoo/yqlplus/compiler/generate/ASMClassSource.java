/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.generate;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.yahoo.yqlplus.api.types.YQLStructType;
import com.yahoo.yqlplus.api.types.YQLType;
import com.yahoo.yqlplus.compiler.code.BytecodeExpression;
import com.yahoo.yqlplus.compiler.code.ProgramValueTypeAdapter;
import com.yahoo.yqlplus.compiler.code.TypeWidget;
import com.yahoo.yqlplus.compiler.types.TypeAdaptingWidget;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Type;
import org.objectweb.asm.util.CheckClassAdapter;
import org.objectweb.asm.util.TraceClassVisitor;

import javax.inject.Inject;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class ASMClassSource {
    private static final AtomicLong ELEMENT_FACTORY = new AtomicLong(0);
    private Map<String, ClassLoader> loaderMap = Maps.newHashMap();
    private Set<ClassLoader> loaders = Sets.newIdentityHashSet();

    private final ConstantTable constantTable;
    private final ClassLoader loader = getClass().getClassLoader();
    private final ClassLoader compoundClassLoader;
    private final GeneratedClassLoader generatedClassLoader;
    private final String uniqueElement;
    private ASMProgramTypeAdapter types;

    private Set<String> usedNames = Sets.newHashSet();
    final List<UnitGenerator> units = Lists.newArrayList();
    boolean built = false;

    @Inject
    public ASMClassSource(Set<TypeAdaptingWidget> adapters, TypeAdaptingWidget defaultTypeAdapter) {
        this.uniqueElement = "gen" + ELEMENT_FACTORY.incrementAndGet();
        this.constantTable = new ConstantTable(this);
        this.types = new ASMProgramTypeAdapter(this, adapters, defaultTypeAdapter);
        this.compoundClassLoader = new CompoundClassLoader();
        this.generatedClassLoader = new GeneratedClassLoader(compoundClassLoader);
    }

    public ASMClassSource(ClassLoader parentClassloader) {
        this.uniqueElement = "gen" + ELEMENT_FACTORY.incrementAndGet();
        this.constantTable = new ConstantTable(this);
        this.compoundClassLoader = new CompoundClassLoader(parentClassloader);
        this.generatedClassLoader = new GeneratedClassLoader(compoundClassLoader);
    }

    public ASMClassSource createChildSource() {
        ASMClassSource childSource = new ASMClassSource(generatedClassLoader);
        childSource.types = types.createChild(childSource);
        return childSource;
    }

    public ASMClassSource createChildSource(ClassLoader classLoader) {
        ASMClassSource childSource = new ASMClassSource(classLoader);
        childSource.types = types.createChild(childSource);
        return childSource;
    }

    public String getUniqueElement() {
        return uniqueElement;
    }

    public String generateUniqueElement() {
        return "unit" + ELEMENT_FACTORY.incrementAndGet();
    }


    public void trace(OutputStream out) {
        List<UnitGenerator> reversed = Lists.newArrayList(units);
        Collections.reverse(reversed);
        for (UnitGenerator unit : reversed) {
            unit.trace(out);
        }
    }

    public void dump(OutputStream out) {
        generatedClassLoader.dump(new PrintStream(out));
    }


    public Type getType(Class<?> clazz) {
        addClass(clazz);
        return Type.getType(clazz);
    }

    public ProgramValueTypeAdapter getValueTypeAdapter() {
        return types;
    }

    public void addUnit(UnitGenerator unitGenerator) {
        Preconditions.checkState(!built, "ASMClassSource cannot be added to once built");
        if (usedNames.contains(unitGenerator.internalName)) {
            throw new ProgramCompileException("Duplicate class definition '%s'", unitGenerator.internalName);
        }
        usedNames.add(unitGenerator.internalName);
        units.add(unitGenerator);
    }

    public TypeWidget resultTypeFor(TypeWidget valueType) {
        return types.resultTypeFor(valueType);
    }

    public void analyze(OutputStream err) {
        generatedClassLoader.analyze(err);
    }

    public YQLType createYQLType(TypeWidget type) {
        return types.createYQLType(type);
    }


    private static class UnitPrep {
        final UnitGenerator unit;
        final List<String> names = Lists.newArrayList();

        UnitPrep(UnitGenerator unit) {
            this.unit = unit;
        }
    }

    public void build() throws ClassNotFoundException {
        Preconditions.checkState(!built, "ASMClassSource may only be built once");
        DynamicBootstrapUnit bootstrapper = new DynamicBootstrapUnit(this);
        bootstrapper.init();
        built = true;

        List<UnitPrep> unitPreps = Lists.newArrayList();
        List<UnitGenerator> reversed = Lists.newArrayList(units);
        Collections.reverse(reversed);
        for (UnitGenerator unit : reversed) {
            UnitPrep unitPrep = new UnitPrep(unit);
            unitPreps.add(unitPrep);
            ByteClassGenerator classes = new ByteClassGenerator();
            try {
                unit.generate(classes);
            } catch (NullPointerException |  ArrayIndexOutOfBoundsException | NegativeArraySizeException e) {
                // this is almost inevitably an error from visitMaxes
                // so let's dump some data
                unit.trace(System.err);
                throw e;
            }
            for (Map.Entry<String, byte[]> e : classes) {
                generatedClassLoader.put(e.getKey(), e.getValue());
                unitPrep.names.add(e.getKey());
            }
        }
        Collections.reverse(unitPreps);
        for (UnitPrep unit : unitPreps) {
            for (String name : unit.names) {
                try {
                    generatedClassLoader.loadClass(name);
                } catch (ClassNotFoundException | ClassFormatError | VerifyError e) {
                    byte[] data = generatedClassLoader.getBytes(name);
                    ClassReader reader = new ClassReader(data);
                    TraceClassVisitor tcv = new TraceClassVisitor(new PrintWriter(System.err));
                    reader.accept(tcv, 0);
                    ClassVisitor cc = new CheckClassAdapter(new ClassWriter(0), true);
                    new ClassReader(data).accept(cc, 0);
                    throw e;
                }
            }
        }

        for (UnitPrep unit : unitPreps) {
            for (String name : unit.names) {
                try {
                    Class<?> clazz = generatedClassLoader.getClass(name.replace('/', '.'));
                    unit.unit.prepare(clazz);
                } catch (ClassFormatError | VerifyError e) {
                    byte[] data = generatedClassLoader.getBytes(name);
                    ClassReader reader = new ClassReader(data);
                    TraceClassVisitor tcv = new TraceClassVisitor(new PrintWriter(System.err));
                    reader.accept(tcv, 0);
                    ClassVisitor cc = new CheckClassAdapter(new ClassWriter(0), true);
                    new ClassReader(data).accept(cc, 0);
                    throw e;
                }
            }
        }
    }

    public Class<?> getGeneratedClass(UnitGenerator unitGenerator) throws ClassNotFoundException {
        return generatedClassLoader.loadClass(unitGenerator.className);
    }


    void addClass(Class<?> clazz) {
        if (clazz.getClassLoader() != null) {
            loaders.add(clazz.getClassLoader());
            loaderMap.put(clazz.getName(), clazz.getClassLoader());
        }
    }

    public BytecodeExpression constant(Object value) {
        return types.constant(value);
    }

    public BytecodeExpression constant(TypeWidget type, Object value) {
        return constantTable.constant(type, value);
    }

    public TypeWidget adapt(YQLType type) {
        return types.defaultRepresentation(type);
    }


    public TypeWidget adaptInternal(Class<?> clazz) {
        addClass(clazz);
        return types.adaptInternal(clazz);
    }

    public TypeWidget resolveStruct(YQLStructType valueType) {
        return types.defaultRepresentation(valueType);
    }

    public TypeWidget adaptInternal(java.lang.reflect.Type type) {
        return types.adaptInternal(type);
    }

    private class CompoundClassLoader extends ClassLoader {
        private CompoundClassLoader() {
        }

        private CompoundClassLoader(ClassLoader parent) {
            super(parent);
        }

        @Override
        public Class<?> loadClass(String name) throws ClassNotFoundException {
            return loadClass(name, true);
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            // favor our classloader
            // TODO: how bad is it that we can't avoid resolving the class?
            ClassLoader target = loaderMap.get(name);
            if (target != this && target != null) {
                return target.loadClass(name);
            }
            try {
                return loader.loadClass(name);
            } catch (ClassNotFoundException e) {
                // continue
            }
            // search all the classloaders
            for (ClassLoader loader : loaders) {
                try {
                    return loader.loadClass(name);
                } catch (ClassNotFoundException e) {
                    // continue
                }
            }
            return super.loadClass(name, resolve);
        }
    }
}
