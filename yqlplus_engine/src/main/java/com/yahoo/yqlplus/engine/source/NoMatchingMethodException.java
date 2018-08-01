package com.yahoo.yqlplus.engine.source;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class NoMatchingMethodException extends ProgramCompileException {
    private static String formatQuery(OperatorNode<SequenceOperator> query) {
        return String.format("%s: %s", query.getLocation(), query.toString());
    }

    private static String formatCandidates(Collection<SourceAdapter.CandidateMethod> candidateMethods) {
        return Joiner.on("\n   ").join(candidateMethods.stream().map((v) -> String.format("%s::%s - %s", v.method.getDeclaringClass().getName(), v.method.getName(), v.getMessage())).collect(Collectors.toList()));
    }

    public enum Reason {
        EXTRA_FIELD("unknown field '%s' (no matching @Set parameter)"),
        FREE_ARGUMENTS("free argument count"),
        RECORD_SET_PARAMETER("@Set('$') parameter"),
        SET_PARAMETER("required @Set('%s') parameter"),
        KEY_PARAMETER("required @Key('%s') parameter");

        Reason(String format) {
            this.format = format;
        }

        ;

        private final String format;

        public String getFormat() {
            return format;
        }
    }

    public static class Candidate {
        private final Method method;
        private final Reason reason;
        private final String message;

        public Candidate(Method method, Reason reason, String message) {
            this.method = method;
            this.reason = reason;
            this.message = message;
        }

        public Method getMethod() {
            return method;
        }

        public Reason getReason() {
            return reason;
        }

        public String getMessage() {
            return message;
        }
    }

    private final String sourceName;
    private final Class<?> sourceClass;
    private final Class<? extends Annotation> annotation;
    private final List<Candidate> candidates;
    private final OperatorNode<SequenceOperator> query;

    NoMatchingMethodException(Class<? extends Annotation> annotationClass, String sourceName, Class<?> clazz, OperatorNode<SequenceOperator> query, Collection<SourceAdapter.CandidateMethod> candidates) {
        super(String.format("Source '%s' class '%s' has no matching public @%s method found for query %s - candidate methods: %s\n", sourceName, clazz.getName(), annotationClass.getSimpleName(), formatQuery(query), formatCandidates(candidates)));
        this.annotation = annotationClass;
        this.sourceName = sourceName;
        this.query = query;
        this.sourceClass = clazz;
        this.candidates = candidates
                .stream()
                .map((v) -> new Candidate(v.method, v.getReason(), v.getMessage()))
                .collect(ImmutableList.toImmutableList());
    }

    public String getSourceName() {
        return sourceName;
    }

    public Class<?> getSourceClass() {
        return sourceClass;
    }

    public Class<? extends Annotation> getAnnotation() {
        return annotation;
    }

    public List<Candidate> getCandidates() {
        return candidates;
    }

    public OperatorNode<SequenceOperator> getQuery() {
        return query;
    }
}
