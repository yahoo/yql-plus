package com.yahoo.yqlplus.engine.compiler.code;

import java.util.List;

public final class FunctionalInterfaceContract {
    public final TypeWidget interfaceType;
    public final String methodName;
    public final TypeWidget returnType;
    public final List<TypeWidget> methodArgumentTypes;

    public FunctionalInterfaceContract(TypeWidget interfaceType, String methodName, TypeWidget returnType, List<TypeWidget> methodArgumentTypes) {
        this.interfaceType = interfaceType;
        this.methodName = methodName;
        this.returnType = returnType;
        this.methodArgumentTypes = methodArgumentTypes;
    }

    public List<TypeWidget> deriveFactoryArguments(List<TypeWidget> argumentTypes) {
        int size = argumentTypes.size() - methodArgumentTypes.size();
        return argumentTypes.subList(0, size);
    }
}
