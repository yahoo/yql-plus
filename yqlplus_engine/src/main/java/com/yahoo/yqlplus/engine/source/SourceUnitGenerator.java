/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.source;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Provider;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.*;
import com.yahoo.yqlplus.api.annotations.Set;
import com.yahoo.yqlplus.api.index.IndexDescriptor;
import com.yahoo.yqlplus.api.trace.Tracer;
import com.yahoo.yqlplus.api.types.YQLNamePair;
import com.yahoo.yqlplus.api.types.YQLStructType;
import com.yahoo.yqlplus.api.types.YQLType;
import com.yahoo.yqlplus.api.types.YQLTypeException;
import com.yahoo.yqlplus.engine.TaskContext;
import com.yahoo.yqlplus.engine.api.PropertyNotFoundException;
import com.yahoo.yqlplus.engine.api.Record;
import com.yahoo.yqlplus.engine.compiler.code.*;
import com.yahoo.yqlplus.engine.compiler.runtime.FieldWriter;
import com.yahoo.yqlplus.engine.internal.generate.PhysicalExprOperatorCompiler;
import com.yahoo.yqlplus.engine.internal.plan.DispatchSourceTypeAdapter;
import com.yahoo.yqlplus.engine.internal.plan.SourceType;
import com.yahoo.yqlplus.language.parser.Location;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;
import org.objectweb.asm.Opcodes;

import javax.annotation.Nullable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.*;

/**
 * Generated one or more classes adapting Source-API equipped classes.
 */
public class SourceUnitGenerator extends SourceApiGenerator {
    private static final String[] OPERATIONS = new String[]{"SELECT", "INSERT", "UPDATE", "DELETE"};

    public SourceUnitGenerator(GambitScope gambitScope) {
        super(gambitScope);
    }

    // sync / async
    // single / batch
    // "free" arguments
    // injectable arguments
    //    taskemitter
    //    tracer
    //    timeout
    //

    public ObjectBuilder createSourceAdapter(String sourceName, Class<?> sourceClass, TypeWidget sourceType, TypeWidget contextType, BytecodeExpression sourceProvider) {
        gambitScope.addClass(sourceClass);
        ObjectBuilder adapter = gambitScope.createObject();
        ObjectBuilder.ConstructorBuilder cb = adapter.getConstructor();
        ObjectBuilder.FieldBuilder fld = adapter.field("$source", sourceType);
        fld.addModifiers(Opcodes.ACC_FINAL);
        cb.exec(fld.get(cb.local("this")).write(cb.cast(sourceType,
                cb.invokeExact(Location.NONE, "get", Provider.class, AnyTypeWidget.getInstance(), sourceProvider))));
        adapter.addParameterField(fld);
        BytecodeExpression metric = gambitScope.constant(PhysicalExprOperatorCompiler.EMPTY_DIMENSION);
        metric = metricWith(cb, metric, "source", sourceName);
        for (String operation : OPERATIONS) {
            // adapter.addParameterField(adapter.finalField("$metricDimension$" + operation, metricWith(cb, metric, "operation", operation)));
            // current code breaks if we add operation dimension; don't for now  TODO: modify that code to ignore new dimensions
            adapter.addParameterField(adapter.finalField("$metricDimension$" + operation, metric));
        }
        return adapter;
    }

    interface SourceMethodBuilder extends SourceArgumentVisitor {
        void begin(ScopedBuilder body, TypeWidget rowType, boolean singleton, boolean async);

        void complete(ObjectBuilder.MethodBuilder adapterMethod, ScopedBuilder body);
    }

    class AdapterBuilder {
        List<Method> methods = Lists.newArrayList();
        String sourceName;
        Class<?> clazz;
        ObjectBuilder target;
        TypeWidget sourceClass;
        List<Class<?>> signature;
        List<String> argumentNames;

        // SELECT
        // DELETE - match an index (just like SELECT)

        // UPDATE - match an INDEX
        // INSERT - accept a record, dispatch to an appropriate method if possible

        Map<IndexDescriptor, QueryMethod> selectMap = Maps.newHashMap();
        QueryMethod scanner;

        Map<IndexDescriptor, QueryMethod> deleteMap = Maps.newHashMap();
        QueryMethod deleteAll;

        Map<IndexDescriptor, UpdateMethod> updateMap = Maps.newHashMap();
        UpdateMethod updateAll;

        InsertMethod insert;
        int sym = 0;

        public AdapterBuilder(String sourceName, Class<? extends Source> clazz, BytecodeExpression providerConstant, List<Class<?>> signature) {
            this.clazz = clazz;
            this.sourceName = sourceName;
            // TODO: should accept the annotations for the free argument signatures, so they can be checked for @NotNullable or equiv.
            // TODO: should likely use generic types for type adapters
            this.sourceClass = gambitScope.adapt(clazz, false);
            this.target = createSourceAdapter(sourceName, clazz, sourceClass, gambitScope.adapt(TaskContext.class, false), providerConstant);
            this.argumentNames = Lists.newArrayList();
            this.signature = signature;
            for (Class<?> arg : signature) {
                String name = "$arg" + argumentNames.size();
                switch (arg.getCanonicalName()) {
                    case "java.lang.Byte":
                        target.addParameter(name, BaseTypeAdapter.INT8);
                        break;
                    case "java.lang.Short":
                        target.addParameter(name, BaseTypeAdapter.INT16);
                        break;
                    case "java.lang.Integer":
                        target.addParameter(name, BaseTypeAdapter.INT32);
                        break;
                    case "java.lang.Long":
                        target.addParameter(name, BaseTypeAdapter.INT64);
                        break;                       
                    case "java.lang.Double":
                        target.addParameter(name, BaseTypeAdapter.FLOAT64);
                        break;
                    case "java.lang.Boolean":
                        target.addParameter(name, BaseTypeAdapter.BOOLEAN);
                        break;
                    default:
                        target.addParameter(name, gambitScope.adapt(arg, true));
                        break;
                }
                argumentNames.add(name);
            }
        }

        private String gensym(String prefix) {
            return prefix + (++sym);
        }


        public SourceType create() {
            return new IndexedSourceAdapter(target.type(), sourceName, selectMap, scanner, deleteMap, deleteAll, updateMap, updateAll, insert);
        }

        private void reportMethodParameterException(String type, Method method, String message, Object... args) {
            message = String.format(message, args);
            throw new YQLTypeException(String.format("@%s method error: %s.%s: %s", type, clazz.getName(), method.getName(), message));
        }

        private void reportMethodException(Method method, String message, Object... args) {
            message = String.format(message, args);
            throw new YQLTypeException(String.format("method error: %s.%s: %s", clazz.getName(), method.getName(), message));
        }

        /**
         * Verifies that the given resultType has a property matching the given fieldName and fieldType.
         */
        private void verifyArgumentType(String methodTypeName, TypeWidget rowType, PropertyAdapter rowProperties, String propertyName, TypeWidget argumentType, String annotationName,
                                        Method method)
                throws ProgramCompileException {
            try {
                TypeWidget targetType = rowProperties.getPropertyType(propertyName);
                if (!targetType.isAssignableFrom(argumentType)) {
                    reportMethodParameterException(methodTypeName, method, "class %s property %s is %s while @%s('%s') type is %s: @%s('%s') " +
                                    "argument type %s cannot be coerced to property '%s' type %s in method %s.%s",
                            rowType.getTypeName(), propertyName, targetType.getTypeName(), annotationName,
                            propertyName, argumentType.getTypeName(),
                            annotationName, propertyName, argumentType.getTypeName(), propertyName,
                            targetType.getTypeName(), method.getDeclaringClass().getName(),
                            method.getName());
                }
            } catch (PropertyNotFoundException e) {
                reportMethodParameterException(methodTypeName, method, "Property @%s('%s') for method %s.%s does not exist on return type %s",
                        annotationName, propertyName, method.getDeclaringClass().getName(), method.getName(),
                        rowType.getTypeName());
            }
        }

        private ObjectBuilder.MethodBuilder addAdapterMethod(Method method, String operation, SourceMethodBuilder bodyBuilder) {
            ObjectBuilder.MethodBuilder adapterMethod = target.method(gensym(operation + "$invoke$"));
            TypeWidget contextType = gambitScope.adapt(TaskContext.class, false);
            adapterMethod.addArgument("$context", contextType);
            GambitCreator.ScopeBuilder block = adapterMethod.scope();

            TypeWidget outputType = gambitScope.adapt(method.getGenericReturnType(), true);
            AssignableValue resultValue = block.allocate(outputType);

            TypeWidget rowType = outputType;
            boolean singleton = true;
            boolean async = false;
            if (outputType.isPromise()) {
                rowType = outputType.getPromiseAdapter().getResultType();
                async = true;
            }
            if (rowType.isIterable()) {
                singleton = false;
                rowType = rowType.getIterableAdapter().getValue();
            }

            if (!rowType.hasProperties()) {
                throw new YQLTypeException("Source method " + method + " does not return a STRUCT type: " + rowType);
            }

            // construct for evaluate an expression in a context with or without timeout enforcement

            AssignableValue contextVar = block.local("$context");

            BytecodeExpression metricExpr = block.local("$metricDimension$" + operation);
            block.set(Location.NONE, contextVar,
                    block.invokeExact(Location.NONE, "start", TaskContext.class, contextVar.getType(),
                            contextVar,
                            metricWith(block, metricExpr, "method", method.getName())));
            GambitCreator.CatchBuilder catchBlock = block.tryCatchFinally();
            ScopedBuilder finallyBody = catchBlock.always();
            
            ScopedBuilder catchBody = catchBlock.body();
            // we do NOT want to fork a callable -- just call the function and return the result.
            GambitCreator.Invocable targetMethod = catchBody.findExactInvoker(method.getDeclaringClass(), method.getName(), outputType, method.getParameterTypes());


            Iterator<String> freeArguments = argumentNames.iterator();
            List<BytecodeExpression> invocationArguments = Lists.newArrayList();
            invocationArguments.add(catchBody.local("$source"));

            // lots of boolean parameters are not great
            bodyBuilder.begin(catchBody, rowType, singleton, async);

            visitMethodArguments(target, method, bodyBuilder, contextVar, catchBody, freeArguments, invocationArguments, block);

            
            bodyBuilder.complete(adapterMethod, catchBody);

            BytecodeExpression invocation = targetMethod.invoke(Location.NONE, invocationArguments);

            catchBody.set(Location.NONE, resultValue, invocation);

            block.exec(catchBlock.build());
            block.exec(finallyBody.invokeExact(Location.NONE, "end", TaskContext.class, BaseTypeAdapter.VOID, contextVar));
            for (BytecodeExpression argument:invocationArguments) {
                if (argument.getType().getJVMType().getClassName().equals(Tracer.class.getName())) {
                    block.exec(finallyBody.invokeExact(Location.NONE, "end", Tracer.class, BaseTypeAdapter.VOID, argument));                  
                }
            }
            adapterMethod.exit(block.complete(resultValue));
            return adapterMethod;
        }


        public void addSelectMethod(final Method method) {
            SelectMethodAdapterBuilder builder = new SelectMethodAdapterBuilder(method);
            ObjectBuilder.MethodBuilder methodBuilder = addAdapterMethod(method, "SELECT", builder);
            // ok, so we've define the adapter method which handles all of the injectable arguments
            // and we're left with a method that takes (context{, key-or-keys}) where
            // key-or-keys is:
            //    absent if it's a SCAN method
            //    a single Record instance of the keys to do a lookup for if it's a SINGLE
            //    a list of Record instances of the keys to do a lookup for if it's a BATCH

            if (builder.isScan()) {
                if (scanner != null) {
                    reportMethodException(method, "There can be only one @Query method for SCAN (no @Key/@CompoundKey arguments) (and one is already set)");
                }
                scanner = new QueryMethod(builder.rowType, target.type(), methodBuilder.invoker(), builder.singleton, builder.async);
            } else if (builder.batch) {
                IndexDescriptor descriptor = builder.indexBuilder.build();
                final QueryMethod qm = new QueryMethod(descriptor, QueryMethod.QueryType.BATCH, builder.rowType, target.type(), methodBuilder.invoker(), builder.singleton, builder.async);
                selectMap.put(descriptor, qm);
            } else {
                // a single key at a time
                IndexDescriptor descriptor = builder.indexBuilder.build();
                final QueryMethod qm = new QueryMethod(descriptor, QueryMethod.QueryType.SINGLE, builder.rowType, target.type(), methodBuilder.invoker(), builder.singleton, builder.async);
                selectMap.put(descriptor, qm);
            }
        }

        public void addInsertMethod(Method method) {
            if (insert != null) {
                reportMethodException(method, "There can be only one @Insert method (and one is already set)");
            }
            InsertMethodAdapterBuilder builder = new InsertMethodAdapterBuilder(method);
            ObjectBuilder.MethodBuilder methodBuilder = addAdapterMethod(method, "INSERT", builder);
            this.insert = new InsertMethod(method.getDeclaringClass() + ":" + method.getName(), builder.rowType, builder.insertType, target.type(), methodBuilder.invoker(), false, builder.singleton, builder.async);
        }

        public void addUpdateMethod(Method method) {
            UpdateMethodAdapterBuilder builder = new UpdateMethodAdapterBuilder(method);
            ObjectBuilder.MethodBuilder methodBuilder = addAdapterMethod(method, "UPDATE", builder);
            // ok, so we've define the adapter method which handles all of the injectable arguments
            // and we're left with a method that takes (context{, key-or-keys}) where
            // key-or-keys is:
            //    absent if it's a SCAN method
            //    a single Record instance of the keys to do a lookup for if it's a SINGLE
            //    a list of Record instances of the keys to do a lookup for if it's a BATCH
            if (builder.isScan()) {
                if (updateAll != null) {
                    reportMethodException(method, "There can be only one @Update all method (and one is already set)");
                }
                updateAll = new UpdateMethod(method.getDeclaringClass() + ":" + method.getName(), builder.rowType, builder.updateType, builder.updateRecord, target.type(), methodBuilder.invoker(), builder.singleton, builder.async);
            } else if (builder.batch) {
                IndexDescriptor descriptor = builder.indexBuilder.build();
                final UpdateMethod qm = new UpdateMethod(method.getDeclaringClass() + ":" + method.getName(), descriptor, QueryMethod.QueryType.BATCH, builder.rowType, builder.updateType, builder.updateRecord, target.type(), methodBuilder.invoker(), builder.singleton, builder.async);
                updateMap.put(descriptor, qm);
            } else {
                // a single key at a time
                IndexDescriptor descriptor = builder.indexBuilder.build();
                final UpdateMethod qm = new UpdateMethod(method.getDeclaringClass() + ":" + method.getName(), descriptor, QueryMethod.QueryType.SINGLE, builder.rowType, builder.updateType, builder.updateRecord, target.type(), methodBuilder.invoker(), builder.singleton, builder.async);
                updateMap.put(descriptor, qm);
            }
        }

        public void addDeleteMethod(Method method) {

            DeleteMethodAdapterBuilder builder = new DeleteMethodAdapterBuilder(method);
            ObjectBuilder.MethodBuilder methodBuilder = addAdapterMethod(method, "DELETE", builder);
            // ok, so we've define the adapter method which handles all of the injectable arguments
            // and we're left with a method that takes (context{, key-or-keys}) where
            // key-or-keys is:
            //    absent if it's a SCAN method
            //    a single Record instance of the keys to do a lookup for if it's a SINGLE
            //    a list of Record instances of the keys to do a lookup for if it's a BATCH

            if (builder.isScan()) {
                deleteAll = new QueryMethod(builder.rowType, target.type(), methodBuilder.invoker(), builder.singleton, builder.async);
            } else if (builder.batch) {
                IndexDescriptor descriptor = builder.indexBuilder.build();
                final QueryMethod qm = new QueryMethod(descriptor, QueryMethod.QueryType.BATCH, builder.rowType, target.type(), methodBuilder.invoker(), builder.singleton, builder.async);
                deleteMap.put(descriptor, qm);
            } else {
                // a single key at a time
                IndexDescriptor descriptor = builder.indexBuilder.build();
                final QueryMethod qm = new QueryMethod(descriptor, QueryMethod.QueryType.SINGLE, builder.rowType, target.type(), methodBuilder.invoker(), builder.singleton, builder.async);
                deleteMap.put(descriptor, qm);
            }
        }

        private abstract class IndexedMethodAdapterBuilder implements SourceMethodBuilder {
            private final String methodType;

            final IndexDescriptor.Builder indexBuilder;
            final Map<String, AssignableValue> keyArguments;
            final Method method;
            boolean singleton;
            boolean async;
            boolean batch;

            ScopedBuilder body;

            TypeWidget rowType;
            PropertyAdapter rowProperties;

            public IndexedMethodAdapterBuilder(String methodType, Method method) {
                this.methodType = methodType;
                this.method = method;
                indexBuilder = IndexDescriptor.builder();
                keyArguments = Maps.newLinkedHashMap();
                batch = false;
            }

            @Override
            public void begin(ScopedBuilder body, TypeWidget rowType, boolean singleton, boolean async) {
                this.body = body;
                this.rowType = rowType;
                if (!rowType.hasProperties()) {
                    reportMethodException(method, "method return type has no properties (is not a struct): %s", rowType.getTypeName());
                }
                rowProperties = rowType.getPropertyAdapter();
                this.async = async;
                this.singleton = singleton;
            }

            protected void addIndexKey(String keyName, TypeWidget keyType, boolean skipEmpty, boolean skipNull) {
                try {
                    indexBuilder.addColumn(keyName, keyType.getValueCoreType(), skipEmpty, skipNull);
                } catch (IllegalArgumentException e) {
                    reportMethodParameterException(methodType, method, "Key '%s' cannot be added to index: %s", keyName, e.getMessage());
                }
            }

            public boolean isScan() {
                return keyArguments.isEmpty();
            }

            @Override
            public BytecodeExpression visitSet(Set annotate, DefaultValue defaultValue, ScopedBuilder body, Class<?> parameterType, TypeWidget parameterWidget) {
                reportMethodParameterException(methodType, method, "@Set parameters are not permitted on @%s methods", methodType);
                // unreachable
                throw new IllegalArgumentException();
            }


            @Override
            public BytecodeExpression visitKeyArgument(Key key, ScopedBuilder body, Class<?> parameterType, TypeWidget parameterWidget) {
                String keyName = key.value().toLowerCase();
                boolean skipEmpty = key.skipEmptyOrZero();
                boolean skipNull = key.skipNull();
                if (keyArguments.containsKey(keyName)) {
                    reportMethodParameterException(methodType, method, "@Key column '%s' used multiple times", keyName);
                } else if (List.class.isAssignableFrom(parameterType)) {
                    if (!batch && !isScan()) {
                        reportMethodParameterException(methodType, method, "@Key column '%s' is a List (batch); a method must either be entirely-batch or entirely-not", keyName);
                    }
                    batch = true;
                    TypeWidget keyType = parameterWidget.getIterableAdapter().getValue();
                    verifyArgumentType(methodType, rowType, rowProperties, keyName, keyType, "Key", method);
                    addIndexKey(keyName, keyType, skipEmpty, skipNull);
                    addKeyParameter(body, parameterWidget, keyName);
                } else if (batch) {
                    reportMethodParameterException(methodType, method, "@Key column '%s' is a single value but other parameters are batch; a method must either be entirely-batch or entirely-not", keyName);
                } else {
                    verifyArgumentType(methodType, rowType, rowProperties, keyName, parameterWidget, "Key", method);
                    addIndexKey(keyName, parameterWidget, skipEmpty, skipNull);
                    addKeyParameter(body, parameterWidget, keyName);
                }
                return keyArguments.get(keyName);
            }

            private void addKeyParameter(ScopedBuilder body, TypeWidget parameterWidget, String keyName) {
                keyArguments.put(keyName, body.allocate(gensym(keyName), NotNullableTypeWidget.create(parameterWidget)));
            }

            @Override
            public void complete(ObjectBuilder.MethodBuilder adapterMethod, ScopedBuilder body) {
                if (isScan()) {
                    // we're done
                    return;
                }
                // if it's not a scan, we need to prepare all the key arguments
                // either be lists or not
                // we'll receive a (list of) structs
                
                // first build the struct type representing each compound key
                StructBuilder structBuilder = gambitScope.createStruct();
                for (Map.Entry<String, AssignableValue> keyEntry : keyArguments.entrySet()) {
                    TypeWidget type = keyEntry.getValue().getType();
                    structBuilder.add(keyEntry.getKey(), (batch ? type.getIterableAdapter().getValue() : type));
                }
                TypeWidget recordType = gambitScope.adapt(Record.class, false);
                if (batch) {
                    BytecodeExpression keys = adapterMethod.addArgument("$key", NotNullableTypeWidget.create(new ListTypeWidget(recordType)));
                    GambitCreator.ScopeBuilder convertScope = body.scope();
                    GambitCreator.IterateBuilder loop = body.iterate(keys);
                    for (Map.Entry<String, AssignableValue> keyEntry : keyArguments.entrySet()) {
                        String propertyName = keyEntry.getKey();
                        AssignableValue keyListValue = keyEntry.getValue();
                        // create the list (above the loop)
                        convertScope.set(Location.NONE, keyListValue, convertScope.constructor(keyListValue.getType()).invoke(Location.NONE));
                        // for each item, extract the property into the list
                        loop.exec(loop.invokeExact(Location.NONE, "add", Collection.class, BaseTypeAdapter.BOOLEAN,
                                keyListValue,
                                // explicit cast to Object so the invokeExact works
                                loop.cast(Location.NONE, AnyTypeWidget.getInstance(), loop.propertyValue(Location.NONE, loop.getItem(), propertyName))));
                    }
                    // run the loop, making and setting all the parallel lists
                    body.exec(convertScope.complete(loop.build()));
                } else {
                    BytecodeExpression key = adapterMethod.addArgument("$key", recordType);
                    // scatter the record in key into all the key values
                    for (Map.Entry<String, AssignableValue> keyEntry : keyArguments.entrySet()) {
                        String propertyName = keyEntry.getKey();
                        AssignableValue keyListValue = keyEntry.getValue();
                        // create the list (above the loop)
                        body.set(Location.NONE, keyListValue, body.cast(keyListValue.getType(), body.propertyValue(Location.NONE, key, propertyName)));
                    }
                }
            }
        }

        private class SelectMethodAdapterBuilder extends IndexedMethodAdapterBuilder {
            private SelectMethodAdapterBuilder(Method method) {
                super("Query", method);
            }


        }

        private class DeleteMethodAdapterBuilder extends IndexedMethodAdapterBuilder {
            private DeleteMethodAdapterBuilder(Method method) {
                super("Delete", method);
            }
        }

        private BytecodeExpression parseDefaultValue(ScopedBuilder body, Method method, String keyName, TypeWidget setType, String defaultValue) {
            if (setType.isIterable()) {
                TypeWidget target = setType.getIterableAdapter().getValue();
                List<BytecodeExpression> expr = Lists.newArrayList();
                StringTokenizer tokenizer = new StringTokenizer(defaultValue, ",", false);
                while (tokenizer.hasMoreElements()) {
                    expr.add(parseDefaultValue(body, method, keyName, target, tokenizer.nextToken().trim()));
                }
                return body.list(Location.NONE, expr);
            } else {
                try {
                    switch (setType.getValueCoreType()) {
                        case BOOLEAN:
                            return body.constant(Boolean.valueOf(defaultValue));
                        case INT8:
                            return body.constant(Byte.decode(defaultValue));
                        case INT16:
                            return body.constant(Short.valueOf(defaultValue));
                        case INT32:
                            return body.constant(Integer.valueOf(defaultValue));
                        case INT64:
                        case TIMESTAMP:
                            return body.constant(Long.valueOf(defaultValue));
                        case FLOAT32:
                            return body.constant(Float.valueOf(defaultValue));
                        case FLOAT64:
                            return body.constant(Double.valueOf(defaultValue));
                        case STRING:
                            return body.constant(defaultValue);
                        // TODO: what should this be? as-is? base64? some kind of encoding?
                        //                            case BYTES:
                        //                                return body.constant(defaultValue);
                        default:
                            reportMethodException(method, "Unable to match default value for @Set('%s') @DefaultValue('%s') to type %s", keyName, defaultValue, setType.getTypeName());
                            throw new IllegalArgumentException(); // reachability
                    }
                } catch (NumberFormatException e) {
                    reportMethodException(method, "Unable to parse default argument %s for @Set('%s'): %s", defaultValue, keyName, e.getMessage());
                    throw new IllegalArgumentException(); // reachability
                }
            }
        }

        private class UpdateMethodAdapterBuilder extends IndexedMethodAdapterBuilder {
            private UpdateMethodAdapterBuilder(Method method) {
                super("Update", method);
                dataValues = Maps.newLinkedHashMap();
                defaultValues = Maps.newLinkedHashMap();
                argumentMap = YQLStructType.builder();
            }

            final Map<String, AssignableValue> dataValues;
            final Map<String, BytecodeExpression> defaultValues;
            final YQLStructType.Builder argumentMap;
            TypeWidget updateRecord;
            YQLStructType updateType;

            @Override
            public BytecodeExpression visitSet(Set annotate, DefaultValue defaultValue, ScopedBuilder body, Class<?> parameterType, TypeWidget setType) {
                String keyName = annotate.value();
                if (dataValues.containsKey(keyName)) {
                    reportMethodParameterException("Update", method, "@Set('%s') used multiple times", keyName);
                    throw new IllegalArgumentException(); // unreachable, but satisfies javac reachability analyzer
                }
                dataValues.put(keyName, body.allocate(setType));
                verifyArgumentType("Update", rowType, rowProperties, keyName, setType, "Set", method);
                YQLType type = gambitScope.createYQLType(setType);
                argumentMap.addField(keyName, type, defaultValue != null);
                if (defaultValue != null) {
                    defaultValues.put(keyName, parseDefaultValue(body, method, keyName, setType, defaultValue.value()));
                }
                return dataValues.get(keyName);
            }


            @Override
            public void complete(ObjectBuilder.MethodBuilder adapterMethod, ScopedBuilder body) {
                super.complete(adapterMethod, body);
                // we need to surface optional/required arguments and a suitable struct type to populate the planner
                // so let's make a YQL schema for our argument map
                this.updateType = argumentMap.build();
                // derive a struct type from it
                // that is the type our adapter method takes as an argument
                // it then needs to explode that into the arguments to the method
                updateRecord = body.adapt(updateType);
                BytecodeExpression record = adapterMethod.addArgument("$record", updateRecord);
                // scatter the record in key into all the key values
                for (Map.Entry<String, AssignableValue> keyEntry : dataValues.entrySet()) {
                    String propertyName = keyEntry.getKey();
                    BytecodeExpression defaultValue = defaultValues.get(propertyName);
                    AssignableValue setValue = keyEntry.getValue();
                    if (defaultValue != null) {
                        body.set(Location.NONE, setValue, body.cast(setValue.getType(), body.coalesce(Location.NONE, body.propertyValue(Location.NONE, record, propertyName), defaultValue)));
                    } else {
                        body.set(Location.NONE, setValue, body.cast(setValue.getType(), body.propertyValue(Location.NONE, record, propertyName)));
                    }
                }
            }
        }

        private class InsertMethodAdapterBuilder implements SourceMethodBuilder {
            final Map<String, AssignableValue> dataValues;
            final Map<String, BytecodeExpression> defaultValues;
            final YQLStructType.Builder argumentMap;

            private final Method method;
            ScopedBuilder body;
            boolean singleton;
            boolean async;
            TypeWidget rowType;
            PropertyAdapter rowProperties;

            YQLStructType insertType;

            public InsertMethodAdapterBuilder(Method method) {
                this.method = method;
                dataValues = Maps.newLinkedHashMap();
                defaultValues = Maps.newLinkedHashMap();
                argumentMap = YQLStructType.builder();
            }

            @Override
            public void begin(ScopedBuilder body, TypeWidget rowType, boolean singleton, boolean async) {
                this.body = body;
                this.rowType = rowType;
                if (!rowType.hasProperties()) {
                    reportMethodException(method, "method return type has no properties (is not a struct): %s", rowType.getTypeName());
                }
                rowProperties = rowType.getPropertyAdapter();
                this.async = async;
                this.singleton = singleton;
            }

            @Override
            public BytecodeExpression visitKeyArgument(Key key, ScopedBuilder body, Class<?> parameterType, TypeWidget parameterWidget) {
                reportMethodParameterException("Insert", method, "@Key parameters are not permitted on @Insert methods");
                throw new IllegalArgumentException();
            }

            @Override
            public BytecodeExpression visitSet(Set annotate, DefaultValue defaultValue, ScopedBuilder body, Class<?> parameterType, TypeWidget setType) {
                String keyName = annotate.value();
                if (dataValues.containsKey(keyName)) {
                    reportMethodParameterException("Insert", method, "@Set('%s') used multiple times", keyName);
                    throw new IllegalArgumentException(); // unreachable, but satisfies javac reachability analyzer
                }
                dataValues.put(keyName, body.allocate(setType));
                verifyArgumentType("Insert", rowType, rowProperties, keyName, setType, "Set", method);
                YQLType type = gambitScope.createYQLType(setType);
                argumentMap.addField(keyName, type, defaultValue != null);
                if (defaultValue != null) {
                    defaultValues.put(keyName, parseDefaultValue(body, method, keyName, setType, defaultValue.value()));
                }
                return dataValues.get(keyName);
            }

            @Override
            public void complete(ObjectBuilder.MethodBuilder adapterMethod, ScopedBuilder body) {
                // we need to surface optional/required arguments and a suitable struct type to populate the planner
                // so let's make a YQL schema for our argument map
                this.insertType = argumentMap.build();
                // derive a struct type from it
                // that is the type our adapter method takes as an argument
                // it then needs to explode that into the arguments to the method
                TypeWidget insertRecord = AnyTypeWidget.getInstance();
                BytecodeExpression record = adapterMethod.addArgument("$record", insertRecord);
                // scatter the record in key into all the key values
                // TODO: check data types before blind coercion attempts!
                for (Map.Entry<String, AssignableValue> keyEntry : dataValues.entrySet()) {
                    String propertyName = keyEntry.getKey();
                    BytecodeExpression defaultValue = defaultValues.get(propertyName);
                    AssignableValue setValue = keyEntry.getValue();
                    if(defaultValue == null) {
                        defaultValue = new MissingRequiredFieldExpr(clazz.getName(), this.method.getName(), propertyName, setValue.getType());
                    }
                    body.set(Location.NONE, setValue, body.cast(setValue.getType(), body.coalesce(Location.NONE, body.propertyValue(Location.NONE, record, propertyName), defaultValue)));
                }
                // verify we didn't get any unrecognized fields
                if(insertType.isClosed()) {
                    PropertyAdapter adapter = insertRecord.getPropertyAdapter();
                    TreeSet<String> fieldNames = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
                    for(String name : Iterables.transform(insertType.getFields(), new Function<YQLNamePair, String>() {
                                @Nullable
                                @Override
                                public String apply(YQLNamePair input) {
                                    return input.getName();
                                }
                            })) {
                        fieldNames.add(name);
                    }
                    body.exec(adapter.mergeIntoFieldWriter(record, body.constant(body.adapt(FieldWriter.class, false), new VerifyNoExtraFieldsFieldWriter(fieldNames, clazz.getName(), this.method.getName()))));
                }

            }
        }
    }

    /**
     * Transform a Source provider into an implementation of IndexedTable and/or IndexedTableFunction.
     *
     * @param input
     * @return
     */
    public SourceType apply(List<String> path, Provider<? extends Source> input) {
        String sourceName = Joiner.on(".").join(path);
        Source source = input.get();
        final Class<? extends Source> clazz = source.getClass();
        // first step, extract the "contract" of the Source
        //   exported row type
        //   is it a function, a table, or both (does it have @Query methods with/without free arguments)
        //     if it's a function, each signature will produce a table
        //     each table will have zero or more indexes, and zero or one SCAN operation
        // we're going to compile all TableFunction @Query methods into methods with the signature
        Map<Integer, AdapterBuilder> generators = Maps.newHashMap();
        final BytecodeExpression providerConstant = gambitScope.constant(input);
        for (Method method : clazz.getMethods()) {
            Query select = method.getAnnotation(Query.class);
            Insert insert = method.getAnnotation(Insert.class);
            Update update = method.getAnnotation(Update.class);
            Delete delete = method.getAnnotation(Delete.class);
            AdapterBuilder builder;
            if (select != null || insert != null || update != null || delete != null) {
                List<Class<?>> argTypes = Lists.newArrayList();
                Class<?>[] argumentTypes = method.getParameterTypes();
                Annotation[][] annotations = method.getParameterAnnotations();
                for (int i = 0; i < argumentTypes.length; ++i) {
                    if (isFreeArgument(argumentTypes[i], annotations[i])) {
                        argTypes.add(argumentTypes[i]);
                    }
                }
                builder = generators.get(argTypes.size());
                if (builder == null) {
                    builder = new AdapterBuilder(sourceName, clazz, providerConstant, argTypes);
                    builder.methods.add(method);
                    generators.put(argTypes.size(), builder);
                } else if (!argTypes.equals(builder.signature)) {
                    throw new ProgramCompileException("@{Query|Insert|Update|Delete} methods may not be overloaded within the same source (method %s::%s argument signature differs from %s::%s by types only); they must be different in argument count if they differ in argument length",
                            method.getDeclaringClass().getName(), method.getName(),
                            builder.methods.get(0).getDeclaringClass().getName(), builder.methods.get(0).getName());
                } else {
                    builder.methods.add(method);
                }
            } else {
                // not a source method!
                continue;
            }
            if (select != null) {
                builder.addSelectMethod(method);
            } else if (insert != null) {
                builder.addInsertMethod(method);
            } else if (update != null) {
                builder.addUpdateMethod(method);
            } else {
                builder.addDeleteMethod(method);
            }
        }

        // generate the final source type, which may need to dispatch
        if (generators.size() == 1) {
            return generators.values().iterator().next().create();
        } else {
            Map<Integer, SourceType> types = Maps.newHashMap();
            for (Map.Entry<Integer, AdapterBuilder> e : generators.entrySet()) {
                types.put(e.getKey(), e.getValue().create());
            }
            return new DispatchSourceTypeAdapter(types);
        }

    }
}
