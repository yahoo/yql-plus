/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import com.google.common.collect.Maps;
import com.yahoo.yqlplus.engine.api.PropertyNotFoundException;
import com.yahoo.yqlplus.engine.compiler.runtime.FieldWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.Map;

public abstract class BasePropertyAdapter implements PropertyAdapter {
    protected TypeWidget type;

    public BasePropertyAdapter(TypeWidget type) {
        this.type = type;
    }

    @Override
    public final BytecodeExpression construct(final Map<String, BytecodeExpression> fields) {
        return new BaseTypeExpression(type) {
            @Override
            public void generate(CodeEmitter top) {
                CodeEmitter code = top.createScope();
                BytecodeExpression input = code.evaluateOnce(type.construct());
                for(Map.Entry<String, BytecodeExpression> e : fields.entrySet()) {
                    AssignableValue out = property(input, e.getKey());
                    BytecodeExpression value = e.getValue();
                    if(value.getType().isNullable()) {
                        Label nextItem = new Label();
                        value = code.evaluateOnce(value);
                        code.gotoIfNull(value, nextItem);
                        code.exec(out.write(new BytecodeCastExpression(NotNullableTypeWidget.create(out.getType()), value)));
                        code.getMethodVisitor().visitLabel(nextItem);
                    } else {
                        code.exec(out.write(e.getValue()));
                    }
                }
                code.exec(input);
                code.endScope();
            }
        };
    }

    @Override
    public final BytecodeSequence mergeIntoFieldWriter(BytecodeExpression target, final BytecodeExpression fieldWriter) {
        return visitProperties(target, new PropertyVisit() {
            @Override
            public void item(CodeEmitter code, BytecodeExpression propertyName, BytecodeExpression propertyValue, Label abortLoop, Label nextItem) {
                code.exec(fieldWriter);
                code.exec(propertyName);
                code.exec(propertyValue);
                code.box(propertyValue.getType());
                code.getMethodVisitor()
                        .visitMethodInsn(Opcodes.INVOKEINTERFACE,
                                Type.getInternalName(FieldWriter.class),
                                "put",
                                Type.getMethodDescriptor(Type.VOID_TYPE, Type.getType(String.class), Type.getType(Object.class)),
                                true);
            }
        });
    }

    @Override
    public BytecodeExpression createFrom(BytecodeExpression inputExpr) {
        if(type.isAssignableFrom(inputExpr.getType())) {
            return inputExpr;
        }
        if (isClosed()) {
            return new BaseTypeExpression(type) {
                @Override
                public void generate(CodeEmitter code) {
                    PropertyAdapter other = inputExpr.getType().getPropertyAdapter();
                    BytecodeExpression src = code.evaluateOnce(inputExpr);
                    Map<String, BytecodeExpression> fields = Maps.newLinkedHashMap();
                    for (Property property : getProperties()) {
                        try {
                            BytecodeExpression val = other.property(src, property.name);
                            if (!property.type.isAssignableFrom(val.getType())) {
                                continue;
                            }
                            fields.put(property.name, val);
                        } catch (PropertyNotFoundException ignored) {
                        }
                    }
                    code.exec(construct(fields));
                }
            };
        } else {
            return new BaseTypeExpression(type) {
                @Override
                public void generate(CodeEmitter parent) {
                    CodeEmitter code = parent.createScope();
                    BytecodeExpression val = code.evaluateOnce(type.construct());
                    PropertyAdapter p = inputExpr.getType().getPropertyAdapter();
                    code.exec(p.visitProperties(inputExpr, new PropertyVisit() {
                        @Override
                        public void item(CodeEmitter code, BytecodeExpression propertyName, BytecodeExpression propertyValue, Label abortLoop, Label nextItem) {
                            try {
                                code.exec(index(val, propertyName).write(propertyValue));
                            } catch (PropertyNotFoundException ignored) {
                                // skip unknown properties
                            }
                        }
                    }));
                    code.exec(val);
                    code.endScope();
                }
            };
        }
    }
}
