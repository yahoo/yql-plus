/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.compiler;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yahoo.yqlplus.api.types.YQLTypeException;
import com.yahoo.yqlplus.engine.internal.plan.types.TypeWidget;
import org.objectweb.asm.Label;

import java.util.List;
import java.util.Map;

public final class LocalFrame {
    Label startFrame;
    Label endFrame;
    final LocalFrame parent;
    final Map<String, LocalValue> vars = Maps.newLinkedHashMap();
    final Map<String, LocalValue> aliases = Maps.newLinkedHashMap();
    int top;

    public LocalFrame() {
        startFrame = new Label();
        endFrame = new Label();
        top = 0;
        parent = null;
    }

    public LocalFrame(LocalFrame parent) {
        this.parent = parent;
        this.top = parent.top;
        startFrame = new Label();
        endFrame = new Label();
    }

    void alias(String name, String alias) {
        aliases.put(alias, get(name));
    }

    LocalValue allocate(String name, TypeWidget type) {
        Preconditions.checkState(!vars.containsKey(name), "variable name '%s' cannot already exist", name);
        int size = type.getJVMType().getSize();
        Preconditions.checkArgument(size > 0, "Do not allocate variables for void ('%s')", name);
        LocalValue result = new LocalValue(name, type, top);
        vars.put(name, result);
        top += size;
        return result;
    }

    LocalValue get(String name) {
        if (vars.containsKey(name)) {
            return vars.get(name);
        } else if (aliases.containsKey(name)) {
            return aliases.get(name);
        } else if (parent != null) {
            return parent.get(name);
        }
        throw new YQLTypeException("Variable not found '" + name + "'");
    }

    public void replaceFrom(LocalFrame locals) {
        top = parent.top;
        startFrame = locals.startFrame;
        endFrame = locals.endFrame;
        vars.clear();
        aliases.clear();
        for (Map.Entry<String, LocalValue> o : locals.vars.entrySet()) {
            vars.put(o.getKey(), o.getValue());
            o.getValue().start = top;
            top += o.getValue().getType().getJVMType().getSize();
        }
        aliases.putAll(locals.aliases);
    }

    public void startFrame(CodeEmitter codeEmitter) {
        codeEmitter.getMethodVisitor().visitLabel(startFrame);
    }

    public void endFrame(CodeEmitter codeEmitter) {
        codeEmitter.getMethodVisitor().visitLabel(endFrame);
        for (Map.Entry<String, LocalValue> o : vars.entrySet()) {
            codeEmitter.getMethodVisitor()
                    .visitLocalVariable(o.getKey(),
                            o.getValue().getType().getJVMType().getDescriptor(), null, startFrame, endFrame, o.getValue().start);
        }
    }

    void getNames(List<String> names) {
        names.addAll(vars.keySet());
    }

    void getTypes(List<TypeWidget> types) {
        for (LocalValue value : vars.values()) {
            types.add(value.getType());
        }
    }

    public List<String> getNames() {
        List<String> names = Lists.newArrayList();
        getNames(names);
        return names;
    }

    public List<TypeWidget> getTypes() {
        List<TypeWidget> types = Lists.newArrayList();
        getTypes(types);
        return types;
    }

    public void check(LocalValue localValue) {
        if (!vars.containsValue(localValue)) {
            if (parent != null) {
                parent.check(localValue);
            } else {
                throw new IllegalStateException("LocalValue is not valid for use within frame");
            }
        }
    }
}
