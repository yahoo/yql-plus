package com.yahoo.yqlplus.engine.compiler.code;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Provider;

import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class DefaultTypeAdapter implements TypeAdaptingWidget {
    private final List<TypeAdaptingWidget> defaultAdapters;
    
    public DefaultTypeAdapter() {
        List<TypeAdaptingWidget> adapters = Lists.newArrayList();
        adapters.add(new TypeFieldAdaptingWidget());
        adapters.add(new TypeAdaptingWidget() {
            @Override
            public boolean supports(Class<?> clazzType) {
                return Enum.class.isAssignableFrom(clazzType) && !clazzType.isEnum();
            }

            @Override
            public TypeWidget adapt(EngineValueTypeAdapter typeAdapter, Type type) {
                return typeAdapter.adaptInternal(JVMTypes.getRawType(type).getSuperclass());
            }
        });
        adapters.add(new TypeAdaptingWidget() {
            @Override
            public boolean supports(Class<?> clazzType) {
                return clazzType.isEnum();
            }

            @Override
            public TypeWidget adapt(EngineValueTypeAdapter typeAdapter, Type type) {
                return new EnumTypeAdapter(JVMTypes.getRawType(type));
            }
        });
        adapters.add(new TypeAdaptingWidget() {
            @Override
            public boolean supports(Class<?> clazzType) {
                return Future.class.isAssignableFrom(clazzType);
            }

            @Override
            public TypeWidget adapt(EngineValueTypeAdapter typeAdapter, Type type) {
                TypeWidget valueType = typeAdapter.adaptInternal(JVMTypes.getTypeArgument(type, 0));
                Class<?> rawType = JVMTypes.getRawType(type);
                if(ListenableFuture.class.isAssignableFrom(rawType)) {
                    return new ListenableFutureResultType(valueType);
                } else if(CompletableFuture.class.isAssignableFrom(rawType)) {
                    return new CompletableFutureResultType(valueType);
                }
                return new FutureResultType(valueType);
            }
        });
        adapters.add(new RecordAdaptingWidget());
        adapters.add(new TypeAdaptingWidget() {
            @Override
            public boolean supports(Class<?> clazzType) {
                return Map.class.isAssignableFrom(clazzType);
            }

            @Override
            public TypeWidget adapt(EngineValueTypeAdapter typeAdapter, Type type) {
                return new MapTypeWidget(getPackageSafeRawType(type, Map.class), typeAdapter.adaptInternal(JVMTypes.getTypeArgument(type, 0)), typeAdapter.adaptInternal(JVMTypes.getTypeArgument(type, 1)));
            }
        });
        adapters.add(new TypeAdaptingWidget() {
            @Override
            public boolean supports(Class<?> clazzType) {
                return List.class.isAssignableFrom(clazzType);
            }

            @Override
            public TypeWidget adapt(EngineValueTypeAdapter typeAdapter, Type type) {
                return new ListTypeWidget(getPackageSafeRawType(type, List.class), typeAdapter.adaptInternal(JVMTypes.getTypeArgument(type, 0)));
            }
        });
        adapters.add(new TypeAdaptingWidget() {
            @Override
            public boolean supports(Class<?> clazzType) {
                return Provider.class.isAssignableFrom(clazzType);
            }

            @Override
            public TypeWidget adapt(EngineValueTypeAdapter typeAdapter, Type type) {
                return new ReflectiveJavaTypeWidget(typeAdapter, Provider.class);
            }
        });
        adapters.add(new ReflectiveTypeAdapter());
        this.defaultAdapters = adapters;
    }
    
    @Override
    public boolean supports(Class<?> clazzType) {
        for(TypeAdaptingWidget adapter : defaultAdapters) {
            if(adapter.supports(clazzType)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public TypeWidget adapt(EngineValueTypeAdapter typeAdapter, Type type) {
        for(TypeAdaptingWidget adapter : defaultAdapters) {
            if (adapter.supports(getRawType(type))) {
                TypeWidget result = adapter.adapt(typeAdapter, type);
                if (result != null) {
                    return result;
                }
            }
        }
        return null;
    }

    private org.objectweb.asm.Type getPackageSafeRawType(Type type, Class<?> acceptableType) {
        Class<?> clazz = JVMTypes.getRawType(type);
        if(!Modifier.isPublic(clazz.getModifiers())) {
            clazz = acceptableType;
        }
        return org.objectweb.asm.Type.getType(clazz);
    }


    private static Class<?> getRawType(Type type) {
        if (type instanceof Class) {
            return (Class)type;
        } else if (type instanceof ParameterizedType) {
            return (Class)((ParameterizedType)type).getRawType();
        } else if (type instanceof GenericArrayType) {
            Type componentType = ((GenericArrayType)type).getGenericComponentType();
            return Array.newInstance(getRawType(componentType), 0).getClass();
        } else if (!(type instanceof TypeVariable) && !(type instanceof WildcardType)) {
            throw new IllegalArgumentException("Expected a Class, ParameterizedType, or GenericArrayType, but <" + type + "> is of type " + type.getClass().getName());
        } else {
            return Object.class;
        }
    }
}
