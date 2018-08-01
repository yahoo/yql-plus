/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.api.types;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Take two YQL types and identify (or create) a unified type.
 * <p/>
 * <p>If any input type is ANY, then it just returns ANY.</p>
 * <p/>
 * <p>If any input type is Optional, then the output will be optional.</p>
 * <p/>
 * <p>For scalar types, unify into a UNION.</p>
 * <p/>
 * <p>For union types, unify into into a union of the member types of each union.</p>
 * <p/>
 * <p>For record types, blend the fields of both input types.</p>
 * <p/>
 * <p>For scalar/record types, make a union.</p>
 * <p/>
 * <p>For unions containing multiple record types, blend all records then union among scalar types.</p>
 * <p/>
 * <p>Blend map and array types (e.g. if both sides are an array, unify the value element type but keep it as an array).</p>
 */
public class YQLTypeUnifier {
    public YQLType unify(YQLType left, YQLType right) {
        TypeBuilder output = new TypeBuilder();
        output.add(left);
        output.add(right);
        return output.create();
    }

    public YQLType unify(Iterable<YQLType> types) {
        TypeBuilder output = new TypeBuilder();
        for (YQLType type : types) {
            output.add(type);
        }
        return output.create();
    }

    public YQLType canonical(YQLType type) {
        if (type.getCoreType() == YQLCoreType.OPTIONAL && ((YQLOptionalType) type).getValueType().getCoreType() == YQLCoreType.UNION) {
            return YQLOptionalType.create(canonical(((YQLOptionalType) type).getValueType()));
        } else if (type.getCoreType() == YQLCoreType.UNION) {
            return unify(((YQLUnionType) type).getChoices());
        }
        return type;
    }

    private static class TypeBuilder {
        boolean any = false;
        boolean optional = false;
        YQLBaseType integerType = null;
        YQLBaseType floatType = null;
        TypeBuilder mapKey = null;
        TypeBuilder mapValue = null;
        TypeBuilder arrayElement = null;
        Map<String, TypeBuilder> structFields = null;
        List<YQLBaseType> otherType = null;
        boolean closedStruct = true;

        void add(YQLType type) {
            if (type.getCoreType() == YQLCoreType.OPTIONAL) {
                optional = true;
                type = YQLOptionalType.deoptional(type);
            }
            if (any) {
                return;
            }
            switch (type.getCoreType()) {
                case ANY:
                    any = true;
                    break;
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                    integerType = maxType(type, integerType);
                    break;
                case FLOAT32:
                case FLOAT64:
                    floatType = maxType(type, floatType);
                    break;
                case STRING:
                case BYTES:
                case TIMESTAMP:
                case BOOLEAN: {
                    if (otherType == null) {
                        otherType = new ArrayList<>();
                    }
                    YQLBaseType base = (YQLBaseType) type;
                    if (!otherType.contains(base)) {
                        otherType.add(base);
                    }
                    break;
                }
                case MAP: {
                    if (mapKey == null || mapValue == null) {
                        mapKey = new TypeBuilder();
                        mapValue = new TypeBuilder();
                    }
                    YQLMapType mapType = (YQLMapType) type;
                    mapKey.add(mapType.getKeyType());
                    mapValue.add(mapType.getValueType());
                    break;
                }
                case ARRAY: {
                    if (arrayElement == null) {
                        arrayElement = new TypeBuilder();
                    }
                    YQLArrayType arrayType = (YQLArrayType) type;
                    arrayElement.add(arrayType.getValueType());
                    break;
                }
                case UNION: {
                    YQLUnionType union = (YQLUnionType) type;
                    for (YQLType choice : union.getChoices()) {
                        add(choice);
                    }
                    break;
                }
                case STRUCT: {
                    YQLStructType structType = (YQLStructType) type;
                    boolean first = structFields == null;
                    closedStruct = closedStruct && structType.isClosed();
                    if (structFields == null) {
                        structFields = new LinkedHashMap<String, TypeBuilder>();
                    }
                    for (YQLNamePair field : structType.getFields()) {
                        String name = field.getName();
                        YQLType fieldType = field.getValueType();
                        TypeBuilder builder = getField(name, first);
                        builder.add(fieldType);
                    }
                    for (Map.Entry<String, TypeBuilder> e : structFields.entrySet()) {
                        if (structType.get(e.getKey()) == null) {
                            e.getValue().optional = true;
                        }
                    }
                    break;
                }
                default:
                    throw new UnsupportedOperationException("Unexpected type: " + type);
            }
        }

        private TypeBuilder getField(String name, boolean first) {
            TypeBuilder fieldType = structFields.get(name);
            if (fieldType == null) {
                fieldType = new TypeBuilder();
                structFields.put(name, fieldType);
                if (!first) {
                    fieldType.optional = true;
                }
            }
            return fieldType;
        }

        private YQLBaseType maxType(YQLType left, YQLBaseType right) {
            if (right == null) {
                return (YQLBaseType) left;
            }
            int lr = left.getCoreType().ordinal();
            int rr = right.getCoreType().ordinal();
            if (lr == rr) {
                return right;
            }
            return lr > rr ? (YQLBaseType) left : right;
        }

        YQLType create() {
            YQLType outputType;
            if (any) {
                outputType = YQLBaseType.ANY;
            } else {
                List<YQLType> output = new ArrayList<YQLType>();
                add(output, integerType);
                add(output, floatType);
                if (otherType != null) {
                    Collections.sort(otherType);
                    output.addAll(otherType);
                }
                if (mapKey != null) {
                    output.add(new YQLMapType(mapKey.create(), mapValue.create()));
                }
                if (arrayElement != null) {
                    output.add(new YQLArrayType(arrayElement.create()));
                }
                if (structFields != null) {
                    YQLStructType.Builder structTypeBuilder = YQLStructType.builder();
                    structTypeBuilder.setClosed(closedStruct);
                    for (Map.Entry<String, TypeBuilder> e : structFields.entrySet()) {
                        structTypeBuilder.addField(e.getKey(), e.getValue().create());
                    }
                    output.add(structTypeBuilder.build());
                }
                if (output.size() == 1) {
                    outputType = output.get(0);
                } else {
                    outputType = YQLUnionType.create(output);
                }
            }
            return optional ? YQLOptionalType.create(outputType) : outputType;
        }

        private void add(List<YQLType> output, YQLType input) {
            if (input != null) {
                output.add(input);
            }
        }

    }

}
