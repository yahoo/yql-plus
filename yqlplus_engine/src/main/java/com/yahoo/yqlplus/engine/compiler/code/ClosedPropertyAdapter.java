/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.compiler.code;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.yahoo.yqlplus.engine.api.PropertyNotFoundException;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.List;
import java.util.Map;

public abstract class ClosedPropertyAdapter extends BasePropertyAdapter {
    private final List<Property> properties;
    private final Map<String, Property> propertyMap;

    public ClosedPropertyAdapter(TypeWidget type, Iterable<Property> properties) {
        super(type);
        this.properties = ImmutableList.copyOf(properties);
        ImmutableMap.Builder<String,Property> propertyMapBuilder = ImmutableMap.builder();
        for(Property property : properties) {
            propertyMapBuilder.put(property.name.toLowerCase(), property);
        }
        this.propertyMap = propertyMapBuilder.build();
        // TODO: compute unified type for property values for use in index()
    }

    @Override
    public final AssignableValue property(BytecodeExpression target, String propertyName) {
        // normalize property name
        return getPropertyValue(target, getProperty(propertyName).name);
    }

    protected abstract AssignableValue getPropertyValue(BytecodeExpression target, String propertyName);

    @Override
    public final Iterable<Property> getProperties() {
        return properties;
    }

    @Override
    public final boolean isClosed() {
        return true;
    }

    protected final Property getProperty(String propertyName) {
        Property property = propertyMap.get(propertyName.toLowerCase());
        if(property == null) {
            throw new PropertyNotFoundException(String.format("Type '%s' does not have property '%s'", type.getTypeName(), propertyName));
        }
        return property;
    }

    @Override
    public final TypeWidget getPropertyType(String propertyName) throws PropertyNotFoundException {
        return getProperty(propertyName).type;
    }

    @Override
    public final BytecodeSequence visitProperties(final BytecodeExpression target, final PropertyVisit loop) {
        return new BytecodeSequence() {
            @Override
            public void generate(CodeEmitter code) {
                Label done = new Label();
                MethodVisitor mv = code.getMethodVisitor();
                for(Property property : getProperties()) {
                    Label nextProperty = new Label();
                    BytecodeExpression propertyValue = code.evaluateOnce(property(target, property.name));
                    boolean maybeNull = code.gotoIfNull(propertyValue, nextProperty);
                    if (maybeNull) {

                        propertyValue = new NullCheckedEvaluatedExpression(propertyValue);
                    }
                    loop.item(code, new StringConstantExpression(property.name), propertyValue, done, nextProperty);
                    if(maybeNull) {
                        mv.visitLabel(nextProperty);
                    }
                }
                mv.visitLabel(done);

            }
        };
    }

    protected interface DispatchProperty {
        BytecodeSequence visit(BytecodeExpression target, String propertyName);
    }

    protected BytecodeSequence dispatchProperty(final BytecodeExpression target, final BytecodeExpression propertyName, DispatchProperty visitor, final BytecodeSequence notFound) {
        StringSwitchSequence seq = new StringSwitchSequence(propertyName, true);
        for(Property property : getProperties()) {
            seq.put(property.name, visitor.visit(target, property.name));
        }
        seq.setDefaultSequence(notFound);
        return seq;
    }

    @Override
    public final AssignableValue index(final BytecodeExpression target, final BytecodeExpression propertyName) {
        return new AssignableValue() {
            @Override
            public TypeWidget getType() {
                return AnyTypeWidget.getInstance();
            }

            @Override
            public BytecodeExpression read() {
                return new BaseTypeExpression(getType()) {
                    @Override
                    public void generate(CodeEmitter code) {
                        code.exec(dispatchProperty(target, propertyName, new DispatchProperty() {
                                    @Override
                                    public BytecodeSequence visit(final BytecodeExpression target, final String propertyName) {
                                        return new BytecodeSequence() {
                                            @Override
                                            public void generate(CodeEmitter code) {
                                                BytecodeExpression propertyValue = property(target, propertyName).read();
                                                code.exec(propertyValue);
                                                code.cast(AnyTypeWidget.getInstance(), propertyValue.getType());
                                            }
                                        };
                                    }
                                },
                                new NullExpr(AnyTypeWidget.getInstance())));
                    }
                };
            }

            @Override
            public BytecodeSequence write(final BytecodeExpression value) {
                return dispatchProperty(target, propertyName, new DispatchProperty() {
                    @Override
                    public BytecodeSequence visit(BytecodeExpression target, String propertyName) {
                        return property(target, propertyName).write(value);
                    }
                }, new NotFoundSequence(target, propertyName));
            }

            @Override
            public BytecodeSequence write(final TypeWidget top) {
                return dispatchProperty(target, propertyName, new DispatchProperty() {
                    @Override
                    public BytecodeSequence visit(BytecodeExpression target, String propertyName) {
                        return property(target, propertyName).write(top);
                    }
                }, new NotFoundSequence(target, propertyName));
            }

            @Override
            public void generate(CodeEmitter code) {
                code.exec(read());
            }
        };
    }

    @Override
    public BytecodeExpression getPropertyNameIterable(final BytecodeExpression target) {
        return new BaseTypeExpression(new IterableTypeWidget(BaseTypeAdapter.STRING)) {
            @Override
            public void generate(CodeEmitter code) {
                ListTypeWidget listOfString = new ListTypeWidget(BaseTypeAdapter.STRING);
                final BytecodeExpression list = code.evaluateOnce(listOfString.construct());
                code.exec(visitProperties(target, new PropertyVisit() {
                    @Override
                    public void item(CodeEmitter code, BytecodeExpression propertyName, BytecodeExpression propertyValue, Label abortLoop, Label nextItem) {
                        code.exec(list);
                        code.exec(propertyName);
                        code.box(propertyName.getType());
                        code.getMethodVisitor()
                                .visitMethodInsn(Opcodes.INVOKEINTERFACE,
                                        Type.getInternalName(List.class),
                                        "add",
                                        Type.getMethodDescriptor(Type.BOOLEAN_TYPE, Type.getType(Object.class)),
                                        true);
                        code.pop(BaseTypeAdapter.BOOLEAN);
                    }
                }));
                code.exec(list);
            }
        };
    }

}
