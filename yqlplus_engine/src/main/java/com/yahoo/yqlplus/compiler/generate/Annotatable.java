/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.generate;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yahoo.yqlplus.api.types.YQLTypeException;
import org.objectweb.asm.*;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;

public class Annotatable implements ObjectBuilder.Annotatable {
    final ASMClassSource source;
    final List<AnnotationDefinition> annotations = Lists.newArrayList();

    public Annotatable(ASMClassSource source) {
        this.source = source;
    }

    public AnnotationDefinition annotate(Class<?> clazz) {
        return annotate(source.getType(clazz));
    }

    public AnnotationDefinition annotate(Type type) {
        AnnotationDefinition annotationDefinition = new AnnotationDefinition(type);
        annotations.add(annotationDefinition);
        return annotationDefinition;
    }

    @Override
    public void annotate(Annotation annotation) {
        Class<? extends Annotation> annotationType = annotation.annotationType();
        ObjectBuilder.AnnotationBuilder builder = annotate(annotationType);
        readAnnotationProperties(annotation, annotationType, builder);
        for (Class<?> iface : annotationType.getInterfaces()) {
            if (iface.isAnnotation()) {
                readAnnotationProperties(annotation, iface, builder);
            }
        }
    }

    private void readAnnotationProperties(Annotation instance, Class<?> iface, ObjectBuilder.AnnotationBuilder builder) {
        for (Method field : iface.getDeclaredMethods()) {
            if (Modifier.isPublic(field.getModifiers()) && field.getParameterCount() == 0) {
                try {
                    builder.put(field.getName(), field.invoke(instance));
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new YQLTypeException(String.format("Unable to invoke Annotation read method %s.%s on %s: %s", field.getDeclaringClass().getName(), field.getName(), instance, e.getMessage()), e);
                }
            }
        }
    }

    public static class AnnotationDefinition implements ObjectBuilder.AnnotationBuilder {
        final Type type;
        final Map<String, Object> values;

        public AnnotationDefinition(Type type) {
            this.type = type;
            this.values = Maps.newLinkedHashMap();
        }

        @Override
        public Object put(String key, Object value) {
            return values.put(key, value);
        }

        @Override
        public Object get(Object key) {
            return values.get(key);
        }

        public void generate(FieldVisitor fv) {
            AnnotationVisitor av = fv.visitAnnotation(type.getDescriptor(), true);
            for (Map.Entry<String, Object> e : values.entrySet()) {
                av.visit(e.getKey(), e.getValue());
            }
            av.visitEnd();
        }

        public void generate(MethodVisitor fv) {
            AnnotationVisitor av = fv.visitAnnotation(type.getDescriptor(), true);
            for (Map.Entry<String, Object> e : values.entrySet()) {
                av.visit(e.getKey(), e.getValue());
            }
            av.visitEnd();
        }

        public void generate(ClassVisitor fv) {
            AnnotationVisitor av = fv.visitAnnotation(type.getDescriptor(), true);
            for (Map.Entry<String, Object> e : values.entrySet()) {
                av.visit(e.getKey(), e.getValue());
            }
            av.visitEnd();
        }
    }

    public void generateAnnotations(FieldVisitor fv) {
        for (AnnotationDefinition annotation : annotations) {
            annotation.generate(fv);
        }
    }

    public void generateAnnotations(MethodVisitor fv) {
        for (AnnotationDefinition annotation : annotations) {
            annotation.generate(fv);
        }
    }

    public void generateAnnotations(ClassVisitor fv) {
        for (AnnotationDefinition annotation : annotations) {
            annotation.generate(fv);
        }
    }
}
