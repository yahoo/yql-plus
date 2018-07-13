package com.yahoo.yqlplus.engine;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.yahoo.yqlplus.api.Exports;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.engine.api.ViewRegistry;
import com.yahoo.yqlplus.engine.internal.plan.ast.ConditionalsBuiltinsModule;
import com.yahoo.yqlplus.engine.internal.plan.ast.RecordsBuiltinsModule;
import com.yahoo.yqlplus.engine.internal.plan.ast.SequenceBuiltinsModule;
import com.yahoo.yqlplus.engine.source.ExportModuleAdapter;
import com.yahoo.yqlplus.engine.source.SourceAdapter;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.Location;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class BindingNamespace implements ModuleNamespace, SourceNamespace, ViewRegistry {
    private final Map<String, SourceType> sources;
    private final Map<String, ModuleType> modules;
    private final Map<String, OperatorNode<SequenceOperator>> views;

    public BindingNamespace() {
        this.sources = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        this.modules = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        this.views = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        // default bindings
        bindModule("yql.sequences", new SequenceBuiltinsModule());
        bindModule("yql.conditionals", new ConditionalsBuiltinsModule());
        bindModule("yql.records", new RecordsBuiltinsModule());
    }

    public BindingNamespace(Object... kvPairs) {
        this();
        bind(kvPairs);
    }

    @SuppressWarnings("unchecked")
    public BindingNamespace bind(Object... kvPairs) {
        for (int i = 0; i < kvPairs.length; i += 2) {
            String key = (String) kvPairs[i];
            Object value = kvPairs[i + 1];
            if (value instanceof Source) {
                bindSource(key, (Source)value);
            } else if (value instanceof Exports) {
                bindModule(key, (Exports) value);
            } else if (value instanceof Supplier) {
                Object val = ((Supplier)value).get();
                if(val instanceof Exports) {
                    bindModule(key, (Class<? extends Exports>) val.getClass(), (Supplier<Exports>)value);
                } else if (val instanceof Source) {
                    bindSource(key, (Class<? extends Source>) val.getClass(), (Supplier<Source>) value);
                } else {
                    throw new IllegalArgumentException("Don't know how to bind supplier supplying non-Source non-Exports class " + val);
                }
            } else if (value instanceof ModuleType) {
                bindModule(key, (ModuleType) value);
            } else if (value instanceof SourceType) {
                bindSource(key, (SourceType) value);
            } else if (value instanceof Class) {
                Class<?> clazz = (Class<?>) value;
                if (Source.class.isAssignableFrom(clazz)) {
                    bindSource(key, (Class<? extends Source>) clazz);
                } else if (Exports.class.isAssignableFrom(clazz)) {
                    bindModule(key, (Class<? extends Exports>) clazz);
                } else {
                    throw new IllegalArgumentException("Don't know how to bind " + clazz);
                }
            } else {
                throw new IllegalArgumentException("Don't know how to bind " + value);
            }

        }
        return this;
    }

    public BindingNamespace bindSource(String name, SourceType sourceType) {
        this.sources.put(name, sourceType);
        return this;
    }

    public BindingNamespace bindSource(String name, Class<? extends Source> source) {
        return bindSource(name, new SourceAdapter(name, source));
    }

    public BindingNamespace bindSource(String name, Source source) {
        return bindSource(name, new SourceAdapter(name, source.getClass(), () -> source));
    }

    public BindingNamespace bindSource(String name, Supplier<Source> sourceSupplier) {
        return bindSource(name, new SourceAdapter(name, sourceSupplier.get().getClass(), sourceSupplier));
    }

    private BindingNamespace bindSource(String name, Class<? extends Source> clazz, Supplier<Source> sourceSupplier) {
        return bindSource(name, new SourceAdapter(name, clazz, sourceSupplier));
    }

    public BindingNamespace bindModule(String name, ModuleType sourceType) {
        this.modules.put(name, sourceType);
        return this;
    }

    public BindingNamespace bindModule(String name, Exports module) {
        return bindModule(name, new ExportModuleAdapter(name, module.getClass(), () -> module));
    }

    public BindingNamespace bindModule(String name, Class<? extends Exports> source) {
        return bindModule(name, new ExportModuleAdapter(name, source));
    }

    private BindingNamespace bindModule(String name, Class<? extends Exports> clazz, Supplier<Exports> moduleSupplier) {
        return bindModule(name, new ExportModuleAdapter(name, clazz, moduleSupplier));
    }


    public BindingNamespace bindModule(String name, Supplier<Exports> moduleSupplier) {
        return bindModule(name, new ExportModuleAdapter(name, moduleSupplier.get().getClass(), moduleSupplier));
    }

    public BindingNamespace bindView(String name, OperatorNode<SequenceOperator> view) {
        this.views.put(name, view);
        return this;
    }

    @Override
    public ModuleType findModule(Location location, List<String> modulePath) {
        return this.modules.get(Joiner.on('.').join(modulePath));
    }

    @Override
    public SourceType findSource(Location location, List<String> path) {
        return this.sources.get(Joiner.on('.').join(path));
    }

    @Override
    public OperatorNode<SequenceOperator> getView(List<String> name) {
        return views.get(Joiner.on('.').join(name));
    }
}
