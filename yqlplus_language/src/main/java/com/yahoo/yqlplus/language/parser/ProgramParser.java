/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.language.parser;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.yahoo.yqlplus.language.grammar.yqlplusLexer;
import com.yahoo.yqlplus.language.grammar.yqlplusParser;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.AnnotateExpressionContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.AnnotationContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.ArgumentContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.ArgumentsContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.ArrayLiteralContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.ArrayTypeContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Array_eq_argumentContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Call_sourceContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.ConstantArrayContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.ConstantExpressionContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.ConstantMapExpressionContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.ConstantPropertyNameAndValueContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Delete_statementContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.DereferencedExpressionContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.EqualityExpressionContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Execute_statementContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.ExpressionContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.FallbackContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Field_defContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Field_names_specContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Field_values_group_specContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Field_values_specContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.IdentContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Import_listContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Import_statementContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.InNotInTargetContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Insert_sourceContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Insert_statementContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Insert_valuesContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.JoinClauseContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.JoinExpressionContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Join_exprContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.LimitContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Literal_elementContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Literal_listContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.LogicalANDExpressionContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.LogicalORExpressionContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.MapExpressionContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.MapTypeContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Merge_componentContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Merge_statementContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.ModuleIdContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.ModuleNameContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.MultiplicativeExpressionContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Namespaced_nameContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Next_statementContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.OffsetContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.OrderbyContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Orderby_fieldContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Output_specContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Paged_clauseContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.ParamsContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Pipeline_stepContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Procedure_argumentContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.ProgramContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Program_arglistContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Project_specContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.PropertyNameAndValueContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Query_statementContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.RelationalExpressionContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.RelationalOpContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Returning_specContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Scalar_literalContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Select_sourceContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Select_source_joinContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Select_source_multiContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Select_statementContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Selectvar_statementContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Sequence_sourceContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Source_listContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Source_specContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Source_statementContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.StatementContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.TimeoutContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.TypenameContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.UnaryExpressionContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Update_statementContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.Update_valuesContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.ViewContext;
import com.yahoo.yqlplus.language.grammar.yqlplusParser.WhereContext;
import com.yahoo.yqlplus.language.internal.ast.StringUnescaper;
import com.yahoo.yqlplus.language.internal.logical.ProjectionBuilder;
import com.yahoo.yqlplus.language.internal.parser.LowerCaseStream;
import com.yahoo.yqlplus.language.logical.ExpressionOperator;
import com.yahoo.yqlplus.language.logical.SequenceOperator;
import com.yahoo.yqlplus.language.logical.SortOperator;
import com.yahoo.yqlplus.language.logical.StatementOperator;
import com.yahoo.yqlplus.language.logical.TypeOperator;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Translate the ANTLR grammar into the logical representation.
 */
public final class ProgramParser {
    public yqlplusParser prepareParser(String programName, InputStream input) throws IOException {
        return prepareParser(programName, new LowerCaseStream(CharStreams.fromStream(input)));
    }

    public yqlplusParser prepareParser(String programName, String input) {
        return prepareParser(programName, new LowerCaseStream(CharStreams.fromString(input)));
    }

    public yqlplusParser prepareParser(File file) throws IOException {
        return prepareParser(file.getAbsoluteFile().toString(), new LowerCaseStream(CharStreams.fromFileName(file.getAbsolutePath())));
    }


    private yqlplusParser prepareParser(final String programName, CharStream input) {
        yqlplusLexer lex = new yqlplusLexer(input);
        lex.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer,
                                    Object offendingSymbol,
                                    int line,
                                    int charPositionInLine,
                                    String msg,
                                    RecognitionException e) {
                throw new ProgramCompileException(new Location(programName, line, charPositionInLine), msg);
            }

        });
        TokenStream tokens = new CommonTokenStream(lex);
        yqlplusParser parser = new yqlplusParser(tokens);
        parser.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer,
                                    Object offendingSymbol,
                                    int line,
                                    int charPositionInLine,
                                    String msg,
                                    RecognitionException e) {
                throw new ProgramCompileException(new Location(programName, line, charPositionInLine), msg);
            }

        });
        parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
        return parser;
    }

    private ProgramContext parseProgram(yqlplusParser parser) throws RecognitionException {
        try {
            return parser.program();
        } catch (RecognitionException e) {
            //Retry parsing using full LL mode
            parser.reset();
            parser.getInterpreter().setPredictionMode(PredictionMode.LL);
            return parser.program();
        }
    }

    public OperatorNode<StatementOperator> parse(String programName, InputStream program) throws IOException, RecognitionException {
        yqlplusParser parser = prepareParser(programName, program);
        return convertProgram(parseProgram(parser), parser, programName);
    }

    public OperatorNode<StatementOperator> parse(String programName, String program) throws IOException, RecognitionException {
        yqlplusParser parser = prepareParser(programName, program);
        return convertProgram(parseProgram(parser), parser, programName);
    }

    public OperatorNode<StatementOperator> parse(File input) throws IOException, RecognitionException {
        yqlplusParser parser = prepareParser(input);
        return convertProgram(parseProgram(parser), parser, input.getAbsoluteFile().toString());
    }

    public OperatorNode<ExpressionOperator> parseExpression(String input) throws IOException, RecognitionException {
        return convertExpr(prepareParser("<expression>", input).expression(false).getRuleContext(), new Scope());
    }

    public OperatorNode<ExpressionOperator> parseExpression(String input, Set<String> visibleAliases) throws IOException, RecognitionException {
        Scope scope = new Scope();
        final Location loc = new Location("<expression>", -1, -1);
        for (String alias : visibleAliases) {
            scope.defineDataSource(loc, alias);
        }
        return convertExpr(prepareParser("<expression>", input).expression(false).getRuleContext(), scope);
    }

    public OperatorNode<ExpressionOperator> parseExpression(String input, Set<String> visibleAliases, Map<String, List<String>> imports) throws IOException, RecognitionException {
        Scope scope = new Scope();
        final Location loc = new Location("<expression>", -1, -1);
        for (String alias : visibleAliases) {
            scope.defineDataSource(loc, alias);
        }
        for (Map.Entry<String, List<String>> e : imports.entrySet()) {
            scope.bindModule(loc, e.getValue(), e.getKey());
        }
        return convertExpr(prepareParser("<expression>", input).expression(false).getRuleContext(), scope);
    }


    private Location toLocation(Scope scope, ParseTree node) {
        Token start;
        if (node instanceof ParserRuleContext) {
            start = ((ParserRuleContext) node).start;
        } else if (node instanceof TerminalNode) {
            start = ((TerminalNode) node).getSymbol();
        } else {
            throw new ProgramCompileException("Location is not available for type " + node.getClass());
        }
        Location location = new Location(scope != null ? scope.programName : "<string>", start.getLine(), start.getCharPositionInLine());
        return location;
    }

    private List<String> readName(Namespaced_nameContext node) {
        List<String> path = Lists.newArrayList();
        for (ParseTree elt : node.children) {
            if (!(getParseTreeIndex(elt) == yqlplusParser.DOT)) {
                path.add(elt.getText());
            }
        }
        return path;
    }

    public OperatorNode<SequenceOperator> parseQuery(String query) throws IOException {
        OperatorNode<StatementOperator> program = parse("program.yql", query + ";");
        assert program.getOperator() == StatementOperator.PROGRAM;
        List<OperatorNode<StatementOperator>> statements = program.getArgument(0);
        OperatorNode<StatementOperator> execute = statements.get(0);
        Preconditions.checkState(execute.getOperator() == StatementOperator.EXECUTE);
        return execute.getArgument(0);
    }

    static class Binding {
        private final List<String> binding;

        Binding(String moduleName, String exportName) {
            this.binding = ImmutableList.of(moduleName, exportName);
        }

        Binding(String moduleName) {
            this.binding = ImmutableList.of(moduleName);
        }

        Binding(List<String> binding) {
            this.binding = binding;
        }

        public List<String> toPath() {
            return binding;
        }

        public List<String> toPathWith(List<String> rest) {
            return ImmutableList.copyOf(Iterables.concat(toPath(), rest));
        }
    }

    static class Scope {
        final Scope root;
        final Scope parent;
        Map<String, Boolean> cursors = ImmutableMap.of();
        Set<String> variables = ImmutableSet.of();
        Set<String> arrayArguments = ImmutableSet.of();
        Set<String> views = Sets.newHashSet();
        Map<String, Binding> bindings = Maps.newHashMap();
        final yqlplusParser parser;
        final String programName;
        
        Scope() {
            this.parser = null;
            this.programName = null;
            this.root = this;
            this.parent = null;
        }

        Scope(yqlplusParser parser, String programName) {
            this.parser = parser;
            this.programName = programName;
            this.root = this;
            this.parent = null;
        }

        Scope(Scope root, Scope parent) {
            this.root = root;
            this.parent = parent;
            this.parser = parent.parser;
            this.programName = parent.programName;
        }

        public yqlplusParser getParser() {
            return parser;
        }

        public String getProgramName() {
            return programName;
        }

        Map<String, Boolean> getCursors() {
            return cursors;
        }

        boolean isBound(String name) {
            // bindings live only in the 'root' node
            return root.bindings.containsKey(name);
        }

        public Binding getBinding(String name) {
            return root.bindings.get(name);
        }

        public List<String> resolvePath(List<String> path) {
            if (path.size() < 1 || !isBound(path.get(0))) {
                return path;
            } else {
                return getBinding(path.get(0)).toPathWith(path.subList(1, path.size()));
            }
        }

        boolean isCursor(String name) {
            return cursors.containsKey(name) || (parent != null && parent.isCursor(name));
        }

        boolean isVariable(String name) {
            return variables.contains(name) || (parent != null && parent.isVariable(name));
        }

        public void bindModule(Location loc, List<String> binding, String symbolName) {
            if (isBound(symbolName)) {
                throw new ProgramCompileException(loc, "Name '%s' is already used.", symbolName);
            }
            root.bindings.put(symbolName, new Binding(binding));
        }

        public void bindModuleSymbol(Location loc, List<String> moduleName, String exportName, String symbolName) {
            ImmutableList.Builder<String> builder = ImmutableList.builder();
            builder.addAll(moduleName);
            builder.add(exportName);
            bindModule(loc, builder.build(), symbolName);
        }

        void defineDataSource(Location loc, String name) {
            defineDataSource(loc, name, false);
        }

        void defineDataSource(Location loc, String name, boolean hasAliasDefinition) {
            if (isCursor(name)) {
                throw new ProgramCompileException(loc, "Alias '%s' is already used.", name);
            }
            if (cursors.isEmpty()) {
                cursors = Maps.newHashMap();
            }
            cursors.put(name, Boolean.valueOf(hasAliasDefinition));
        }

        public void defineVariable(Location loc, String name) {
            if (isVariable(name)) {
                throw new ProgramCompileException(loc, "Variable/argument '%s' is already used.", name);
            }
            if (variables.isEmpty()) {
                variables = Sets.newHashSet();
            }
            variables.add(name);
        }

        public void arrayArgument(String name) {
            if(root.arrayArguments.isEmpty()) {
                root.arrayArguments = Sets.newHashSet();
            }
            root.arrayArguments.add(name);
        }

        public boolean isArrayArgument(String name) {
            return root.arrayArguments.contains(name);
        }

        public void defineView(Location loc, String text) {
            if (this != root) {
                throw new IllegalStateException("Views MUST be defined in 'root' scope only");
            }
            if (views.contains(text)) {
                throw new ProgramCompileException(loc, "View '%s' already defined", text);
            }
            views.add(text);
        }

        Scope child() {
            return new Scope(root, this);
        }

        Scope getRoot() {
            return root;
        }
    }

    private OperatorNode<SequenceOperator> convertSelectOrInsertOrUpdateOrDelete(ParseTree node, Scope scopeParent) {

        Preconditions.checkArgument(node instanceof Select_statementContext || node instanceof Insert_statementContext ||
                node instanceof Update_statementContext || node instanceof Delete_statementContext);

        //  SELECT^ select_field_spec select_source where? orderby? limit? offset? timeout? fallback?
        // select is the only place to define where/orderby/limit/offset and joins
        Scope scope = scopeParent.child();
        ProjectionBuilder proj = null;
        OperatorNode<SequenceOperator> source = null;
        OperatorNode<ExpressionOperator> filter = null;
        List<OperatorNode<SortOperator>> orderby = null;
        OperatorNode<ExpressionOperator> offset = null;
        OperatorNode<ExpressionOperator> limit = null;
        OperatorNode<ExpressionOperator> timeout = null;
        OperatorNode<SequenceOperator> fallback = null;
        OperatorNode<SequenceOperator> insertValues = null;
        OperatorNode<ExpressionOperator> updateValues = null;

        ParseTree sourceNode;

        if (node instanceof Select_statementContext) {
            sourceNode = node.getChild(2) instanceof Select_sourceContext ? node.getChild(2).getChild(0) : null;
        } else {
            sourceNode = node.getChild(1);
        }

        if (sourceNode != null) {
            switch (getParseTreeIndex(sourceNode)) {
                // ALL_SOURCE and MULTI_SOURCE are how FROM SOURCES
                // *|source_name,... are parsed
                // They can't be used directly with the JOIN syntax at this time
                case yqlplusParser.RULE_select_source_all: {
                    Location location = toLocation(scope, sourceNode.getChild(2));
                    source = OperatorNode.create(location, SequenceOperator.ALL);
                    source.putAnnotation("alias", "row");
                    scope.defineDataSource(location, "row");
                }
                    break;
                case yqlplusParser.RULE_select_source_multi:
                    Source_listContext multiSourceContext = ((Select_source_multiContext) sourceNode).source_list();
                    source = readMultiSource(scope, multiSourceContext);
                    source.putAnnotation("alias", "row");
                    scope.defineDataSource(toLocation(scope, multiSourceContext), "row");
                    break;
                case yqlplusParser.RULE_select_source_join:
                    source = convertSource((ParserRuleContext) sourceNode.getChild(1), scope);
                    List<Join_exprContext> joinContexts = ((Select_source_joinContext) sourceNode).join_expr();
                    for (Join_exprContext joinContext : joinContexts) {
                        source = convertJoin(joinContext, source, scope);
                    }
                    break;
                case yqlplusParser.RULE_insert_source:
                    Insert_sourceContext insertSourceContext = (Insert_sourceContext) sourceNode;
                    source = convertSource((ParserRuleContext) insertSourceContext.getChild(1), scope);
                    break;
                case yqlplusParser.RULE_delete_source:
                    source = convertSource((ParserRuleContext) sourceNode.getChild(1), scope);
                    break;
                case yqlplusParser.RULE_update_source:
                    source = convertSource((ParserRuleContext) sourceNode.getChild(0), scope);
                    break;
            }
        } else {
            source = OperatorNode.create(SequenceOperator.EMPTY);
            }

        for (int i = 1; i < node.getChildCount(); ++i) {
            ParseTree child = node.getChild(i);
            switch (getParseTreeIndex(child)) {
                case yqlplusParser.RULE_select_field_spec:
                    if (getParseTreeIndex(child.getChild(0)) == yqlplusParser.RULE_project_spec) {
                        proj = readProjection(((Project_specContext) child.getChild(0)).field_def(), scope);
                    }
                    break;
                case yqlplusParser.RULE_returning_spec:
                    proj = readProjection(((Returning_specContext) child).select_field_spec().project_spec().field_def(), scope);
                    break;
                case yqlplusParser.RULE_where:
                    filter = convertExpr(((WhereContext) child).expression(), scope);
                    break;
                case yqlplusParser.RULE_orderby:
                    // OrderbyContext orderby()
                    List<Orderby_fieldContext> orderFieds = ((OrderbyContext) child)
                            .orderby_fields().orderby_field();
                    orderby = Lists.newArrayListWithExpectedSize(orderFieds.size());
                    for (int j = 0; j < orderFieds.size(); ++j) {
                        orderby.add(convertSortKey(orderFieds.get(j), scope));
                    }
                    break;
                case yqlplusParser.RULE_limit:
                    limit = convertExpr(((LimitContext) child).expression(), scope);
                    break;
                case yqlplusParser.RULE_offset:
                    offset = convertExpr(((OffsetContext) child).expression(), scope);
                    break;
                case yqlplusParser.RULE_timeout:
                    timeout = convertExpr(((TimeoutContext) child).fixed_or_parameter(), scope);
                    break;
                case yqlplusParser.RULE_fallback:
                    fallback = convertQuery(((FallbackContext) child).select_statement(), scope);
                    break;
                case yqlplusParser.RULE_insert_values:
                    if (child.getChild(0) instanceof yqlplusParser.Query_statementContext) {
                        insertValues = convertQuery(child.getChild(0).getChild(0), scope);
                    } else {
                        insertValues = readBatchValues(((Insert_valuesContext) child).field_names_spec(), ((Insert_valuesContext) child).field_values_group_spec(), scope);
                    }
                    break;
                case yqlplusParser.RULE_update_values:
                    if (getParseTreeIndex(child.getChild(0)) == yqlplusParser.RULE_field_def) {
                        updateValues = readValues(((Update_valuesContext) child).field_def(), scope);
                    } else {
                        updateValues = readValues((Field_names_specContext) child.getChild(0), (Field_values_specContext) child.getChild(2), scope);
                    }
                    break;
            }
            }
        // now assemble the logical plan
        OperatorNode<SequenceOperator> result = source;
        // filter
        if (filter != null) {
            result = OperatorNode.create(SequenceOperator.FILTER, result, filter);
        }
        // insert values
        if (insertValues != null) {
            result = OperatorNode.create(SequenceOperator.INSERT, result, insertValues);
        }
        // update
        if (updateValues != null) {
            if (filter != null) {
                result = OperatorNode.create(SequenceOperator.UPDATE, source, updateValues, filter);
            } else {
                result = OperatorNode.create(SequenceOperator.UPDATE_ALL, source, updateValues);
            }
        }
        // delete
        if (getParseTreeIndex(node) == yqlplusParser.RULE_delete_statement) {
            if (filter != null) {
                result = OperatorNode.create(SequenceOperator.DELETE, source, filter);
            } else {
                result = OperatorNode.create(SequenceOperator.DELETE_ALL, source);
            }
        }
        // then sort (or project and sort)
        boolean projectBeforeSort = false;
        if (orderby != null) {
            if (proj != null) {
                for (OperatorNode<SortOperator> sortKey : orderby) {
                    OperatorNode<ExpressionOperator> sortExpression = sortKey.getArgument(0);
                    List<OperatorNode<ExpressionOperator>> sortReadFields = getReadFieldExpressions(sortExpression);
                    for (OperatorNode<ExpressionOperator> sortReadField : sortReadFields) {
                        String sortKeyField = sortReadField.getArgument(1);
                        if (proj.isAlias(sortKeyField)) {
                            // TODO: Add support for "mixed" case
                            projectBeforeSort = true;
                            break;
                        }
                    }
                }
            }
            if (projectBeforeSort) {
                result = OperatorNode.create(SequenceOperator.SORT, proj.make(result), orderby);
            } else {
                result = OperatorNode.create(SequenceOperator.SORT, result, orderby);
            }
        }
        // then offset/limit (must be done after sorting!)
        if (offset != null && limit != null) {
            result = OperatorNode.create(SequenceOperator.SLICE, result, offset, limit);
        } else if (offset != null) {
            result = OperatorNode.create(SequenceOperator.OFFSET, result, offset);
        } else if (limit != null) {
            result = OperatorNode.create(SequenceOperator.LIMIT, result, limit);
        }
        // finally, project (if not already)
        if (proj != null && !projectBeforeSort) {
            result = proj.make(result);
        }
        if (timeout != null) {
            result = OperatorNode.create(SequenceOperator.TIMEOUT, result, timeout);
        }
        // if there's a fallback, emit a fallback node
        if (fallback != null) {
            result = OperatorNode.create(SequenceOperator.FALLBACK, result, fallback);
        }
        return result;
    }

    private OperatorNode<ExpressionOperator> readValues(List<Field_defContext> fieldDefs, Scope scope) {
        List<String> fieldNames;
        List<OperatorNode<ExpressionOperator>> fieldValues;
        int numPairs = fieldDefs.size();
        fieldNames = Lists.newArrayListWithExpectedSize(numPairs);
        fieldValues = Lists.newArrayListWithExpectedSize(numPairs);
        for (int j = 0; j < numPairs; j++) {
            ParseTree startNode = fieldDefs.get(j);
            while (startNode.getChildCount() < 3) {
                startNode = startNode.getChild(0);
            }
            fieldNames.add((String) convertExpr(startNode.getChild(0), scope).getArgument(1));
            fieldValues.add(convertExpr(startNode.getChild(2), scope));
        }
        return OperatorNode.create(ExpressionOperator.MAP, fieldNames, fieldValues);
    }

    private OperatorNode<SequenceOperator> readMultiSource(Scope scope, Source_listContext multiSource) {
        List<List<String>> sourceNameList = Lists.newArrayList();
        List<Namespaced_nameContext> nameSpaces = multiSource.namespaced_name();
        for (Namespaced_nameContext node : nameSpaces) {
            List<String> name = readName(node);
            sourceNameList.add(name);
        }
        return OperatorNode.create(toLocation(scope, multiSource), SequenceOperator.MULTISOURCE, sourceNameList);
    }

    //    pipeline_step
//    : namespaced_name arguments[false]?
//    ;
    private OperatorNode<SequenceOperator> convertPipe(Query_statementContext queryStatementContext, List<Pipeline_stepContext> nodes, Scope scope) {
        OperatorNode<SequenceOperator> result = convertQuery(queryStatementContext.getChild(0), scope.getRoot());
        for (Pipeline_stepContext step : nodes) {
            List<String> name = readName(step.namespaced_name());
            List<OperatorNode<ExpressionOperator>> args = ImmutableList.of();
            //LPAREN (argument[$in_select] (COMMA argument[$in_select])*) RPAREN
            if (step.getChildCount() > 1) {
                ArgumentsContext arguments = step.arguments();
                if (arguments.getChildCount() > 2) {
                    List<ArgumentContext> argumentContextList = arguments.argument();
                    args = Lists.newArrayListWithExpectedSize(argumentContextList.size());
                    for (ArgumentContext argumentContext : argumentContextList) {
                        args.add(convertExpr(argumentContext.expression(), scope.getRoot()));

                    }
                }
            }
            result = OperatorNode.create(SequenceOperator.PIPE, result, scope.resolvePath(name), args);
        }
        return result;
    }

    private OperatorNode<SequenceOperator> convertMerge(List<Merge_componentContext> mergeComponentList, Scope scope) {
        Preconditions.checkArgument(mergeComponentList != null);
        List<OperatorNode<SequenceOperator>> sources = Lists.newArrayListWithExpectedSize(mergeComponentList.size());
        for (Merge_componentContext mergeComponent : mergeComponentList) {
            Select_statementContext selectContext = mergeComponent.select_statement();
            Source_statementContext sourceContext = mergeComponent.source_statement();
            if (selectContext != null) {
                sources.add(convertQuery(selectContext, scope.getRoot()));
            } else {
                sources.add(convertQuery(sourceContext, scope.getRoot()));
            }
        }
        return OperatorNode.create(SequenceOperator.MERGE, sources);
    }

    private OperatorNode<SequenceOperator> convertQuery(ParseTree node, Scope scope) {
        if (node instanceof Select_statementContext
                || node instanceof Insert_statementContext
                || node instanceof Update_statementContext
                || node instanceof Delete_statementContext) {
            return convertSelectOrInsertOrUpdateOrDelete(node, scope.getRoot());
        } else if (node instanceof Source_statementContext) { //for pipe
            Source_statementContext sourceStatementContext = (Source_statementContext) node;
            return convertPipe(sourceStatementContext.query_statement(), sourceStatementContext.pipeline_step(), scope);
        } else if (node instanceof Merge_statementContext) {
            return convertMerge(((Merge_statementContext) node).merge_component(), scope);
        } else if (node instanceof Execute_statementContext) {
            return convertExecute((Execute_statementContext) node, scope);
        } else {
            throw new IllegalArgumentException("Unexpected argument type to convertQueryStatement: " + node.toStringTree());
        }

    }

    private OperatorNode<SequenceOperator> convertExecute(Execute_statementContext node, Scope scope) {
        OperatorNode<ExpressionOperator> expr = convertExpr(node.expression(), scope);
        return OperatorNode.create(toLocation(scope, node), SequenceOperator.EVALUATE, expr);
    }

    private OperatorNode<SequenceOperator> convertJoin(Join_exprContext node, OperatorNode<SequenceOperator> left, Scope scope) {
        Source_specContext sourceSpec = node.source_spec();
        OperatorNode<SequenceOperator> right = convertSource(sourceSpec, scope);
        JoinExpressionContext joinContext = node.joinExpression();
        List<OperatorNode<ExpressionOperator>> clauses = Lists.newArrayList();
        List<JoinClauseContext> joinClauses = joinContext.joinClause();
        for(JoinClauseContext clause : joinClauses) {
            OperatorNode<ExpressionOperator> joinExpression = readBinOp(ExpressionOperator.valueOf("EQ"), clause.getChild(0), clause.getChild(2), scope);
            clauses.add(joinExpression);
        }
        OperatorNode<ExpressionOperator> joinExpression = clauses.size() == 1 ? clauses.get(0) : OperatorNode.create(ExpressionOperator.AND, clauses);
        return OperatorNode.create(toLocation(scope, sourceSpec), node.join_spec().LEFT() != null ? SequenceOperator.LEFT_JOIN : SequenceOperator.JOIN, left, right, joinExpression);
    }

    private String assignAlias(String alias, ParserRuleContext node, Scope scope) {
        if (alias == null) {
            alias = "source";
        }

        if (node != null && node instanceof yqlplusParser.Alias_defContext) {
            //alias_def :   (AS? ID);
            ParseTree idChild = node;
            if (node.getChildCount() > 1) {
                idChild = node.getChild(1);
            }
            alias = idChild.getText();
            if (scope.isCursor(alias)) {
                throw new ProgramCompileException(toLocation(scope, idChild), "Source alias '%s' is already used", alias);
            }
            scope.defineDataSource(toLocation(scope, idChild), alias, true);
            return alias;
        } else {
            String candidate = alias;
            int c = 0;
            while (scope.isCursor(candidate)) {
                candidate = alias + (++c);
            }
            scope.defineDataSource(null, candidate);
            return alias;
        }
    }

    private OperatorNode<SequenceOperator> convertSource(ParserRuleContext sourceSpecNode, Scope scope) {
        
        // DataSources
        String alias;
        OperatorNode<SequenceOperator> result;
        ParserRuleContext dataSourceNode = sourceSpecNode;
        ParserRuleContext aliasContext = null;
      //data_source
        //:   call_source
        //|   LPAREN source_statement RPAREN
        //|   sequence_source
        //;
        if (sourceSpecNode instanceof Source_specContext) {
            dataSourceNode = (ParserRuleContext)sourceSpecNode.getChild(0);
            if (sourceSpecNode.getChildCount() == 2) {
                aliasContext = (ParserRuleContext)sourceSpecNode.getChild(1);
            }
            if (dataSourceNode.getChild(0) instanceof Call_sourceContext ||
                dataSourceNode.getChild(0) instanceof  Sequence_sourceContext) {
                dataSourceNode = (ParserRuleContext)dataSourceNode.getChild(0);              
            } 
//TODO double check whether comment out this is correct            
//            else { //source_statement
//                dataSourceNode = (ParserRuleContext)dataSourceNode.getChild(1); 
//            }
        }
        switch (getParseTreeIndex(dataSourceNode)) {
            case yqlplusParser.RULE_write_data_source:
            case yqlplusParser.RULE_call_source: {
                List<String> names = readName((Namespaced_nameContext) dataSourceNode.getChild(Namespaced_nameContext.class, 0));
                alias = assignAlias(names.get(names.size() - 1), aliasContext, scope);
                List<OperatorNode<ExpressionOperator>> arguments = ImmutableList.of();
                ArgumentsContext argumentsContext = dataSourceNode.getRuleContext(ArgumentsContext.class, 0);
                if (argumentsContext != null) {
                    List<ArgumentContext> argumentContexts = argumentsContext.argument();
                    arguments = Lists.newArrayListWithExpectedSize(argumentContexts.size());
                    for (ArgumentContext argumentContext : argumentContexts) {
                        arguments.add(convertExpr(argumentContext, scope));
                    }
                }
                if (names.size() == 1 && scope.isVariable(names.get(0))) {
                    String ident = names.get(0);
                    if (arguments.size() > 0) {
                        throw new ProgramCompileException(toLocation(scope, argumentsContext), "Invalid call-with-arguments on local source '%s'", ident);
                    }
                    result = OperatorNode.create(toLocation(scope, dataSourceNode), SequenceOperator.EVALUATE, OperatorNode.create(toLocation(scope, dataSourceNode), ExpressionOperator.VARREF, ident));
                } else {
                    result = OperatorNode.create(toLocation(scope, dataSourceNode), SequenceOperator.SCAN, scope.resolvePath(names), arguments);
                }
                break;
            }
            case yqlplusParser.RULE_sequence_source: {
                IdentContext identContext = dataSourceNode.getRuleContext(IdentContext.class,0);
                String ident = identContext.getText();
                if (!scope.isVariable(ident)) {
                    throw new ProgramCompileException(toLocation(scope, identContext), "Unknown variable reference '%s'", ident);
                }
                alias = assignAlias(ident, aliasContext, scope);
                result = OperatorNode.create(toLocation(scope, dataSourceNode), SequenceOperator.EVALUATE, OperatorNode.create(toLocation(scope, dataSourceNode), ExpressionOperator.VARREF, ident));
                break;
            }
            case yqlplusParser.RULE_source_statement: {
                alias = assignAlias(null, dataSourceNode, scope);
                result = convertQuery(dataSourceNode, scope);
                break;
            }
            case yqlplusParser.RULE_data_source: {
                alias = assignAlias("source", aliasContext, scope);
                result = convertQuery(dataSourceNode.getChild(1), scope);
                break;
            }
            default:
                throw new IllegalArgumentException("Unexpected argument type to convertSource: " + dataSourceNode.getText());
        }
        result.putAnnotation("alias", alias);
        return result;
    }

    private OperatorNode<TypeOperator> decodeType(Scope scope, TypenameContext type) {

        TypeOperator op;
        ParseTree typeNode = type.getChild(0);
        switch (getParseTreeIndex(typeNode)) {
            case yqlplusParser.TYPE_BOOLEAN:
                op = TypeOperator.BOOLEAN;
                break;
            case yqlplusParser.TYPE_BYTE:
                op = TypeOperator.BYTE;
                break;
            case yqlplusParser.TYPE_DOUBLE:
                op = TypeOperator.DOUBLE;
                break;
            case yqlplusParser.TYPE_INT16:
                op = TypeOperator.INT16;
                break;
            case yqlplusParser.TYPE_INT32:
                op = TypeOperator.INT32;
                break;
            case yqlplusParser.TYPE_INT64:
                op = TypeOperator.INT64;
                break;
            case yqlplusParser.TYPE_STRING:
                op = TypeOperator.STRING;
                break;
            case yqlplusParser.TYPE_TIMESTAMP:
                op = TypeOperator.TIMESTAMP;
                break;
            case yqlplusParser.RULE_arrayType:
                return OperatorNode.create(toLocation(scope, typeNode), TypeOperator.ARRAY, decodeType(scope, ((ArrayTypeContext) typeNode).getChild(TypenameContext.class, 0)));
            case yqlplusParser.RULE_mapType:
                return OperatorNode.create(toLocation(scope, typeNode), TypeOperator.MAP, decodeType(scope, ((MapTypeContext) typeNode).getChild(TypenameContext.class, 0)));
            default:
                throw new ProgramCompileException("Unknown type " + typeNode.getText());
        }
        return OperatorNode.create(toLocation(scope, typeNode), op);
    }

    private List<String> createBindingName(ParseTree node) {
        if (node instanceof ModuleNameContext) {
            if (((ModuleNameContext) node).namespaced_name() != null) {
                return readName(((ModuleNameContext) node).namespaced_name());
            } else if (((ModuleNameContext) node).literalString() != null) {
                return ImmutableList.of(((ModuleNameContext) node).literalString().STRING().getText());
            }
        } else if (node instanceof ModuleIdContext) {
            return ImmutableList.of(node.getText());
        }
        throw new ProgramCompileException("Wrong context");
    }

    private OperatorNode<StatementOperator> convertProgram(
            ParserRuleContext program, yqlplusParser parser, String programName) {
        Scope scope = new Scope(parser, programName);
        List<OperatorNode<StatementOperator>> stmts = Lists.newArrayList();
        int output = 0;
        for (ParseTree node : program.children) {
            if (!(node instanceof ParserRuleContext)) {
                continue;
            }
            ParserRuleContext ruleContext = (ParserRuleContext) node;
            switch (ruleContext.getRuleIndex()) {
                case yqlplusParser.RULE_params: {
                    // ^(ARGUMENT ident typeref expression?)
                    ParamsContext paramsContext = (ParamsContext) ruleContext;
                    Program_arglistContext program_arglistContext = paramsContext.program_arglist();
                    if (program_arglistContext != null) {
                        List<Procedure_argumentContext> argList = program_arglistContext.procedure_argument();
                        for (Procedure_argumentContext procedureArgumentContext : argList) {
                            if(procedureArgumentContext.array_eq_argument() != null) {
                                Array_eq_argumentContext arrayArgument = procedureArgumentContext.array_eq_argument();
                                String name = arrayArgument.ident().getText();
                                OperatorNode<TypeOperator> type = OperatorNode.create(TypeOperator.ARRAY,  decodeType(scope, arrayArgument.getChild(TypenameContext.class, 0)));
                                OperatorNode<ExpressionOperator> defaultValue = OperatorNode.create(ExpressionOperator.NULL);
                                if (arrayArgument.expression() != null) {
                                    defaultValue = convertExpr(arrayArgument.expression(), scope);
                                }
                                scope.defineVariable(toLocation(scope, arrayArgument), name);
                                scope.arrayArgument(name);
                                stmts.add(OperatorNode.create(StatementOperator.ARGUMENT, name, type, defaultValue));
                            } else {
                                String name = procedureArgumentContext.ident().getText();
                                OperatorNode<TypeOperator> type = decodeType(scope, procedureArgumentContext.getChild(TypenameContext.class, 0));
                                OperatorNode<ExpressionOperator> defaultValue = OperatorNode.create(ExpressionOperator.NULL);
                                if (procedureArgumentContext.expression() != null) {
                                    defaultValue = convertExpr(procedureArgumentContext.expression(), scope);
                                }
                                scope.defineVariable(toLocation(scope, procedureArgumentContext), name);
                                if(type.getOperator() == TypeOperator.ARRAY) {
                                    scope.arrayArgument(name);
                                }
                                stmts.add(OperatorNode.create(StatementOperator.ARGUMENT, name, type, defaultValue));
                            }
                    }
                }
                    break;
                }
                case yqlplusParser.RULE_import_statement: {
                    Import_statementContext importContext = (Import_statementContext) ruleContext;
                    if (null == importContext.import_list()) {
                        List<String> name = createBindingName(node.getChild(1));
                    String target;
                        Location location = toLocation(scope, node.getChild(1));
                        if (node.getChildCount() == 2) {
                        target = name.get(0);
                        } else if (node.getChildCount() == 4) {
                            target = node.getChild(3).getText();
                    } else {
                        throw new ProgramCompileException("Unknown node count for IMPORT: " + node.toStringTree());
                    }
                    scope.bindModule(location, name, target);
                    } else {
                        // | FROM moduleName IMPORT import_list -> ^(IMPORT_FROM
                        // moduleName import_list+)
                        Import_listContext importListContext = importContext.import_list();
                        List<String> name = createBindingName(importContext.moduleName());
                        Location location = toLocation(scope, importContext.moduleName());
                        List<ModuleIdContext> moduleIds = importListContext.moduleId();
                        List<String> symbols = Lists.newArrayListWithExpectedSize(moduleIds.size());
                        for (ModuleIdContext cnode : moduleIds) {
                            symbols.add(cnode.ID().getText());
                    }
                    for (String sym : symbols) {
                        scope.bindModuleSymbol(location, name, sym, sym);
                    }
                    }
                    break;
                }

                // DDL
                case yqlplusParser.RULE_ddl:
                    ruleContext = (ParserRuleContext) ruleContext.getChild(0);
                case yqlplusParser.RULE_view: {
                    // view and projection expansion now has to be done by the
                    // execution engine
                    // since views/projections, in order to be useful, have to
                    // support being used from outside the same program
                    ViewContext viewContext = (ViewContext) ruleContext;
                    Location loc = toLocation(scope, viewContext);
                    scope.getRoot().defineView(loc, viewContext.ID().getText());
                    stmts.add(OperatorNode.create(loc, StatementOperator.DEFINE_VIEW, viewContext.ID().getText(), convertQuery(viewContext.source_statement(), scope.getRoot())));
                    break;
                }
                case yqlplusParser.RULE_statement: {
                    // ^(STATEMENT_QUERY source_statement paged_clause?
                    // output_spec?)
                    StatementContext statementContext = (StatementContext) ruleContext;
                    switch (getParseTreeIndex(ruleContext.getChild(0))) {
                        case yqlplusParser.RULE_selectvar_statement: {
                            // ^(STATEMENT_SELECTVAR ident source_statement)
                            Selectvar_statementContext selectVarContext = (Selectvar_statementContext) ruleContext.getChild(0);
                            String variable = selectVarContext.ident().getText();
                            OperatorNode<SequenceOperator> query = convertQuery(selectVarContext.source_statement(), scope);
                            Location location = toLocation(scope, selectVarContext.ident());
                            scope.defineVariable(location, variable);
                            stmts.add(OperatorNode.create(location, StatementOperator.EXECUTE, query, variable));
                    break;
                }
                        case yqlplusParser.RULE_next_statement: {
                            // NEXT^ literalString OUTPUT! AS! ident
                            Next_statementContext nextStateContext = (Next_statementContext) ruleContext.getChild(0);
                            String continuationValue = StringUnescaper.unquote(nextStateContext.literalString().getText());
                            String variable = nextStateContext.ident().getText();
                            Location location = toLocation(scope, node);
                            OperatorNode<SequenceOperator> next = OperatorNode.create(location, SequenceOperator.NEXT, continuationValue);
                            stmts.add(OperatorNode.create(location, StatementOperator.EXECUTE, next, variable));
                            stmts.add(OperatorNode.create(location, StatementOperator.OUTPUT, variable));
                            scope.defineVariable(location, variable);
                    break;
                }
                        case yqlplusParser.RULE_output_statement:
                            Source_statementContext source_statement = statementContext.output_statement().source_statement();
                            OperatorNode<SequenceOperator> query;
                            if (source_statement.getChildCount() == 1) {
                                query = convertQuery(source_statement.query_statement().getChild(0), scope);
                            } else {
                                query = convertQuery(source_statement, scope);
                            }
                    String variable = "result" + (++output);
                    boolean isCountVariable = false;
                    OperatorNode<ExpressionOperator> pageSize = null;
                            ParseTree outputStatement = node.getChild(0);
                            Location location = toLocation(scope, outputStatement);
                            for (int i = 1; i < outputStatement.getChildCount(); ++i) {
                                ParseTree child = outputStatement.getChild(i);
                                switch (getParseTreeIndex(child)) {
                                    case yqlplusParser.RULE_paged_clause:
                                        Paged_clauseContext pagedContext = (Paged_clauseContext) child;
                                        pageSize = convertExpr(pagedContext.fixed_or_parameter(), scope);
                                        break;
                                    case yqlplusParser.RULE_output_spec:
                                        Output_specContext outputSpecContext = (Output_specContext) child;
                                        variable = outputSpecContext.ident().getText();
                                        if (outputSpecContext.COUNT() != null) {
                                            isCountVariable = true;
                                        }
                                        break;
                                    default:
                                        throw new ProgramCompileException("Unknown statement attribute: " + child.toStringTree());
                        }
                    }
                    scope.defineVariable(location, variable);
                    if (pageSize != null) {
                        query = OperatorNode.create(SequenceOperator.PAGE, query, pageSize);
                    }
                    stmts.add(OperatorNode.create(location, StatementOperator.EXECUTE, query, variable));
                            stmts.add(OperatorNode.create(location, isCountVariable ? StatementOperator.COUNT : StatementOperator.OUTPUT, variable));
                }
                    break;
            }
                default:
                    throw new ProgramCompileException("Unknown program element: " + node.getText());
            }
        }
        // traverse the tree, find all of the namespaced calls not covered by
        // imports so we can
        // define "implicit" import statements for them (to make engine
        // implementation easier)
        return OperatorNode.create(new Location(programName, 0, 0), StatementOperator.PROGRAM, stmts);
    }

    private OperatorNode<SortOperator> convertSortKey(Orderby_fieldContext node, Scope scope) {
        TerminalNode descDef = node.DESC();
        OperatorNode<ExpressionOperator> exprNode = convertExpr(node.expression(), scope);
        if (descDef != null) {
            return OperatorNode.create(toLocation(scope, descDef), SortOperator.DESC, exprNode);
        } else {
            return OperatorNode.create(toLocation(scope, node), SortOperator.ASC, exprNode);
        } 
    }

    private ProjectionBuilder readProjection(List<Field_defContext> fieldDefs, Scope scope) {
        if (null == fieldDefs)
            throw new ProgramCompileException("Null fieldDefs");
        ProjectionBuilder proj = new ProjectionBuilder();
        for (Field_defContext rulenode : fieldDefs) {
            // FIELD
                // expression alias_def?
            OperatorNode<ExpressionOperator> expr = convertExpr((ExpressionContext) rulenode.getChild(0), scope);

                String aliasName = null;
                if (rulenode.getChildCount() > 1) {
                    // ^(ALIAS ID)
                    aliasName = rulenode.alias_def().ID().getText();
                }
                proj.addField(aliasName, expr);
            // no grammar for the other rule types at this time
        }
        return proj;
    }

    public static int getParseTreeIndex(ParseTree parseTree) {
        if (parseTree instanceof TerminalNode) {
            return ((TerminalNode) parseTree).getSymbol().getType();
        } else {
            return ((RuleNode) parseTree).getRuleContext().getRuleIndex();
        }
    }

    public OperatorNode<ExpressionOperator> convertExpr(ParseTree parseTree,
                                                        Scope scope) {
        switch (getParseTreeIndex(parseTree)) {
            case yqlplusParser.RULE_nullOperator:
                return OperatorNode.create(ExpressionOperator.NULL);
            case yqlplusParser.RULE_argument:
                return convertExpr(parseTree.getChild(0), scope);
            case yqlplusParser.RULE_fixed_or_parameter: {
                ParseTree firstChild = parseTree.getChild(0);
                if (getParseTreeIndex(firstChild) == yqlplusParser.INT) {
                    return OperatorNode.create(toLocation(scope, firstChild), ExpressionOperator.LITERAL, firstChild.getText());
                } else {
                    return convertExpr(firstChild, scope);
                }
            }
            case yqlplusParser.RULE_constantMapExpression: {
                List<ConstantPropertyNameAndValueContext> propertyList = ((ConstantMapExpressionContext) parseTree).constantPropertyNameAndValue();
                List<String> names = Lists.newArrayListWithExpectedSize(propertyList.size());
                List<OperatorNode<ExpressionOperator>> exprs = Lists.newArrayListWithExpectedSize(propertyList.size());
                for (ConstantPropertyNameAndValueContext child : propertyList) {
                    // : propertyName ':' expression[$expression::namespace] ->
                    // ^(PROPERTY propertyName expression)
                    names.add(StringUnescaper.unquote(child.getChild(0).getText()));
                    exprs.add(convertExpr(child.getChild(2), scope));
                }
                return OperatorNode.create(toLocation(scope, parseTree), ExpressionOperator.MAP, names, exprs);
            }
            case yqlplusParser.RULE_mapExpression: {
                List<PropertyNameAndValueContext> propertyList = ((MapExpressionContext) parseTree).propertyNameAndValue();
                List<String> names = Lists.newArrayListWithExpectedSize(propertyList.size());
                List<OperatorNode<ExpressionOperator>> exprs = Lists.newArrayListWithCapacity(propertyList.size());
                for (PropertyNameAndValueContext child : propertyList) {
                    // : propertyName ':' expression[$expression::namespace] ->
                    // ^(PROPERTY propertyName expression)
                    names.add(StringUnescaper.unquote(child.getChild(0).getText()));
                    exprs.add(convertExpr(child.getChild(2), scope));
                }
                return OperatorNode.create(toLocation(scope, parseTree), ExpressionOperator.MAP, names, exprs);
            }
            case yqlplusParser.RULE_constantArray: {
                List<ConstantExpressionContext> expressionList = ((ConstantArrayContext)parseTree).constantExpression();
                List<OperatorNode<ExpressionOperator>> values = Lists.newArrayListWithExpectedSize(expressionList.size());
                for (ConstantExpressionContext expr : expressionList) {
                    values.add(convertExpr(expr, scope));
                }
                return OperatorNode.create(toLocation(scope, expressionList.isEmpty()? parseTree:expressionList.get(0)), ExpressionOperator.ARRAY, values);
            }
            case yqlplusParser.RULE_arrayLiteral: {
                List<ExpressionContext> expressionList = ((ArrayLiteralContext) parseTree).expression();
                List<OperatorNode<ExpressionOperator>> values = Lists.newArrayListWithExpectedSize(expressionList.size());
                for (ExpressionContext expr : expressionList) {
                    values.add(convertExpr(expr, scope));
                }
                return OperatorNode.create(toLocation(scope, expressionList.isEmpty() ? parseTree : expressionList.get(0)), ExpressionOperator.ARRAY, values);
            }
            //dereferencedExpression: primaryExpression(indexref[in_select]| propertyref)*
            case yqlplusParser.RULE_dereferencedExpression: {
                DereferencedExpressionContext dereferencedExpression = (DereferencedExpressionContext) parseTree;
                Iterator<ParseTree> it = dereferencedExpression.children.iterator();
                OperatorNode<ExpressionOperator> result = convertExpr(it.next(), scope);
                while (it.hasNext()) {
                    ParseTree defTree = it.next();
                    if (getParseTreeIndex(defTree) == yqlplusParser.RULE_propertyref) {
                        //DOT nm=ID
                        result = OperatorNode.create(toLocation(scope, parseTree), ExpressionOperator.PROPREF, result, defTree.getChild(1).getText());
                    } else {
                        //indexref
                        result = OperatorNode.create(toLocation(scope, parseTree), ExpressionOperator.INDEX, result, convertExpr(defTree.getChild(1), scope));
                    }
                }
                return result;
            }
            case yqlplusParser.RULE_primaryExpression: {
                // ^(CALL namespaced_name arguments)
                ParseTree firstChild = parseTree.getChild(0);
                switch (getParseTreeIndex(firstChild)) {
                    case yqlplusParser.RULE_fieldref: {
                        return convertExpr(firstChild, scope);
                    }
                    case yqlplusParser.RULE_callExpresion: {
                        List<ArgumentContext> args = ((ArgumentsContext) firstChild.getChild(1)).argument();
                        List<OperatorNode<ExpressionOperator>> arguments = Lists.newArrayListWithExpectedSize(args.size());
                        for (ArgumentContext argContext : args) {
                            arguments.add(convertExpr(argContext.expression(), scope));
                        }
                        return OperatorNode.create(toLocation(scope, parseTree), ExpressionOperator.CALL, scope.resolvePath(readName((Namespaced_nameContext) firstChild.getChild(0))), arguments);
                    }
                    // TODO add processing this is not implemented in V3
                    // case yqlplusParser.APPLY:

                    case yqlplusParser.RULE_parameter:
                        // external variable reference
                        return OperatorNode.create(toLocation(scope, firstChild), ExpressionOperator.VARREF, firstChild.getChild(1).getText());
                    case yqlplusParser.RULE_scalar_literal:
                    case yqlplusParser.RULE_arrayLiteral:
                    case yqlplusParser.RULE_mapExpression:
                        return convertExpr(firstChild, scope);
                    case yqlplusParser.LPAREN:
                        return convertExpr(parseTree.getChild(1), scope);
                }
            }
            // TODO: Temporarily disable CAST - think through how types are named
            // case yqlplusParser.CAST: {
            //
            // return new Cast()
            // }
            // return new CastExpression(payload);
            case yqlplusParser.RULE_parameter: {
                // external variable reference
                ParserRuleContext parameterContext = (ParserRuleContext) parseTree;
                IdentContext identContext = parameterContext.getRuleContext(IdentContext.class, 0);
                return OperatorNode.create(toLocation(scope, identContext), ExpressionOperator.VARREF, identContext.getText());
            }
            case yqlplusParser.RULE_annotateExpression: {
                //annotation logicalORExpression
                AnnotationContext annotateExpressionContext = ((AnnotateExpressionContext) parseTree).annotation();
                OperatorNode<ExpressionOperator> annotation = convertExpr(annotateExpressionContext.constantMapExpression(), scope);
                OperatorNode<ExpressionOperator> expr = convertExpr(parseTree.getChild(1), scope);
                List<String> names = (List<String>) annotation.getArgument(0);
                List<OperatorNode<ExpressionOperator>> annotates = (List<OperatorNode<ExpressionOperator>>) annotation.getArgument(1);
                for (int i = 0; i < names.size(); ++i) {
                    expr.putAnnotation(names.get(i), readConstantExpression(annotates.get(i)));
                }
                return expr;
            }
            case yqlplusParser.RULE_expression: {
                return convertExpr(parseTree.getChild(0), scope);
            }
            case yqlplusParser.RULE_logicalANDExpression:
                LogicalANDExpressionContext andExpressionContext = (LogicalANDExpressionContext) parseTree;
                return readConjOp(ExpressionOperator.AND, andExpressionContext.equalityExpression(), scope);
            case yqlplusParser.RULE_logicalORExpression: {
                int childCount = parseTree.getChildCount();
                LogicalORExpressionContext logicalORExpressionContext = (LogicalORExpressionContext) parseTree;
                if (childCount > 1) {
                    return readConjOrOp(ExpressionOperator.OR, logicalORExpressionContext, scope);
                } else {
                    List<EqualityExpressionContext> equalityExpressionList = ((LogicalANDExpressionContext) parseTree.getChild(0)).equalityExpression();
                    if (equalityExpressionList.size() > 1) {
                        return readConjOp(ExpressionOperator.AND, equalityExpressionList, scope);
                    } else {
                        return convertExpr(equalityExpressionList.get(0), scope);
                    }
                }
            }
            case yqlplusParser.RULE_equalityExpression: {
                EqualityExpressionContext equalityExpression = (EqualityExpressionContext) parseTree;
                RelationalExpressionContext relationalExpressionContext = equalityExpression.relationalExpression(0);
                OperatorNode<ExpressionOperator> expr = convertExpr(relationalExpressionContext, scope);
                InNotInTargetContext inNotInTarget = equalityExpression.inNotInTarget();
                int childCount = equalityExpression.getChildCount();
                if (childCount == 1) {
                    return expr;
                }
                if (inNotInTarget != null) {
                    Literal_listContext literalListContext = inNotInTarget.literal_list();
                    boolean isIN = equalityExpression.IN() != null;
                    if (literalListContext == null) {
                        Select_statementContext selectStatementContext = inNotInTarget.select_statement();
                        OperatorNode<SequenceOperator> query = convertQuery(selectStatementContext, scope);
                        return OperatorNode.create(expr.getLocation(), isIN ? ExpressionOperator.IN_QUERY : ExpressionOperator.NOT_IN_QUERY, expr, query);
                    } else {
                        // we need to identify the type of the target; if it's a
                        // scalar we need to wrap it in a CREATE_ARRAY
                        // if it's already a CREATE ARRAY then it's fine, otherwise
                        // we need to know the variable type
                        // return readBinOp(node.getType() == yqlplusParser.IN ?
                        // ExpressionOperator.IN : ExpressionOperator.NOT_IN, node,
                        // scope);
                        return readBinOp(isIN ? ExpressionOperator.IN : ExpressionOperator.NOT_IN, equalityExpression.getChild(0), literalListContext, scope);
                    }

                } else {
                    ParseTree firstChild = equalityExpression.getChild(1);
                    if (equalityExpression.getChildCount() == 2) {
                        switch (getParseTreeIndex(firstChild)) {
                            case yqlplusParser.IS_NULL:
                                return readUnOp(ExpressionOperator.IS_NULL, relationalExpressionContext, scope);
                            case yqlplusParser.IS_NOT_NULL:
                                return readUnOp(ExpressionOperator.IS_NOT_NULL, relationalExpressionContext, scope);
                        }
                    } else {
                        switch (getParseTreeIndex(firstChild.getChild(0))) {
                            case yqlplusParser.EQ:
                                return readBinOp(ExpressionOperator.EQ, equalityExpression.getChild(0), equalityExpression.getChild(2), scope);
                            case yqlplusParser.NEQ:
                                return readBinOp(ExpressionOperator.NEQ, equalityExpression.getChild(0), equalityExpression.getChild(2), scope);
                            case yqlplusParser.LIKE:
                                return readBinOp(ExpressionOperator.LIKE, equalityExpression.getChild(0), equalityExpression.getChild(2), scope);
                            case yqlplusParser.NOTLIKE:
                                return readBinOp(ExpressionOperator.NOT_LIKE, equalityExpression.getChild(0), equalityExpression.getChild(2), scope);
                            case yqlplusParser.MATCHES:
                                return readBinOp(ExpressionOperator.MATCHES, equalityExpression.getChild(0), equalityExpression.getChild(2), scope);
                            case yqlplusParser.NOTMATCHES:
                                return readBinOp(ExpressionOperator.NOT_MATCHES, equalityExpression.getChild(0), equalityExpression.getChild(2), scope);
                            case yqlplusParser.CONTAINS:
                                return readBinOp(ExpressionOperator.CONTAINS, equalityExpression.getChild(0), equalityExpression.getChild(2), scope);
                        }
                    }

                }
                break;
            }
            case yqlplusParser.RULE_relationalExpression: {
                RelationalExpressionContext relationalExpressionContext = (RelationalExpressionContext) parseTree;
                RelationalOpContext opContext = relationalExpressionContext.relationalOp();
                if (opContext != null) {
                    switch (getParseTreeIndex(relationalExpressionContext.relationalOp().getChild(0))) {
                        case yqlplusParser.LT:
                            return readBinOp(ExpressionOperator.LT, parseTree, scope);
                        case yqlplusParser.LTEQ:
                            return readBinOp(ExpressionOperator.LTEQ, parseTree, scope);
                        case yqlplusParser.GT:
                            return readBinOp(ExpressionOperator.GT, parseTree, scope);
                        case yqlplusParser.GTEQ:
                            return readBinOp(ExpressionOperator.GTEQ, parseTree, scope);
                    }
                } else {
                    return convertExpr(relationalExpressionContext.additiveExpression(0), scope);
                }
            }
            break;
            case yqlplusParser.RULE_additiveExpression:
            case yqlplusParser.RULE_multiplicativeExpression: {
                if (parseTree.getChildCount() > 1) {
                    String opStr = parseTree.getChild(1).getText();
                    switch (opStr) {
                        case "+":
                            return readBinOp(ExpressionOperator.ADD, parseTree, scope);
                        case "-":
                            return readBinOp(ExpressionOperator.SUB, parseTree, scope);
                        case "/":
                            return readBinOp(ExpressionOperator.DIV, parseTree, scope);
                        case "*":
                            return readBinOp(ExpressionOperator.MULT, parseTree, scope);
                        case "%":
                            return readBinOp(ExpressionOperator.MOD, parseTree, scope);
                        default:
                            if (parseTree.getChild(0) instanceof UnaryExpressionContext) {
                                return convertExpr(parseTree.getChild(0), scope);
                            } else {
                                throw new ProgramCompileException(toLocation(scope, parseTree), "Unknown expression type: " + parseTree.toStringTree());
                            }
                    }
                } else {
                    if (parseTree.getChild(0) instanceof UnaryExpressionContext) {
                        return convertExpr(parseTree.getChild(0), scope);
                    } else if (parseTree.getChild(0) instanceof MultiplicativeExpressionContext) {
                        return convertExpr(parseTree.getChild(0), scope);
                    } else {
                        throw new ProgramCompileException(toLocation(scope, parseTree), "Unknown expression type: " + parseTree.getText());
                    }
                }
            }
            case yqlplusParser.RULE_unaryExpression: {
                if (1 == parseTree.getChildCount()) {
                    return convertExpr(parseTree.getChild(0), scope);
                } else if (2 == parseTree.getChildCount()) {
                    if ("-".equals(parseTree.getChild(0).getText())) {
                        return readUnOp(ExpressionOperator.NEGATE, parseTree, scope);
                    } else if ("!".equals(parseTree.getChild(0).getText())) {
                        return readUnOp(ExpressionOperator.NOT, parseTree, scope);
                    }
                    throw new ProgramCompileException(toLocation(scope, parseTree), "Unknown unary operator " + parseTree.getText());
                } else {
                    throw new ProgramCompileException(toLocation(scope, parseTree), "Unknown child count " + parseTree.getChildCount() + " of " + parseTree.getText());
                }
            }
            case yqlplusParser.RULE_fieldref:
            case yqlplusParser.RULE_joinDereferencedExpression: {
                // all in-scope data sources should be defined in scope
                // the 'first' field in a namespaced reference must be:
                // - a field name if (and only if) there is exactly one data source
                // in scope OR
                // - an alias name, which will be followed by a field name
                // ^(FIELDREF<FieldReference>[$expression::namespace]
                // namespaced_name)
                List<String> path = readName((Namespaced_nameContext) parseTree.getChild(0));
                Location loc = toLocation(scope, parseTree.getChild(0));
                String alias = path.get(0);
                OperatorNode<ExpressionOperator> result;
                int start;
                if (scope.isCursor(alias)) {
                    if (path.size() > 1) {
                        result = OperatorNode.create(loc, ExpressionOperator.READ_FIELD, alias, path.get(1));
                        start = 2;
                    } else {
                        if (scope.getCursors().size() > 1 || Boolean.TRUE.equals(scope.getCursors().get(alias))) {
                            result = OperatorNode.create(loc, ExpressionOperator.READ_RECORD, alias);
                        } else {
                            result = OperatorNode.create(loc, ExpressionOperator.READ_FIELD, alias, alias);
                        }
                        start = 1;
                    }
                } else if (scope.isBound(alias)) {
                    return OperatorNode.create(loc, ExpressionOperator.READ_MODULE, scope.getBinding(alias).toPathWith(path.subList(1, path.size())));
                } else if (scope.getCursors().size() == 1) {
                    alias = scope.getCursors().keySet().iterator().next();
                    result = OperatorNode.create(loc, ExpressionOperator.READ_FIELD, alias, path.get(0));
                    start = 1;
                } else {
                    // ah ha, we can't end up with a 'loose' UDF call because it
                    // won't be a module or known alias
                    // so we need not support implicit imports for constants used in
                    // UDFs
                    throw new ProgramCompileException(loc, "Unknown field or alias '%s'", alias);
                }
                for (int idx = start; idx < path.size(); ++idx) {
                    result = OperatorNode.create(loc, ExpressionOperator.PROPREF, result, path.get(idx));
                }
                return result;
            }
            case yqlplusParser.RULE_scalar_literal:
                return OperatorNode.create(toLocation(scope, parseTree), ExpressionOperator.LITERAL, convertLiteral((Scalar_literalContext) parseTree));
            case yqlplusParser.RULE_insert_values:
                return readValues((Insert_valuesContext) parseTree, scope);
            case yqlplusParser.RULE_constantExpression:
                return convertExpr(parseTree.getChild(0), scope);
            case yqlplusParser.RULE_literal_list:
                List<Literal_elementContext> elements = ((Literal_listContext) parseTree).literal_element();
                if (elements.size() == 1) {
                    Literal_elementContext child = elements.get(0);
                    OperatorNode<ExpressionOperator> expr = convertExpr(child.getChild(0), scope);
                    if(expr.getOperator() == ExpressionOperator.VARREF && scope.isArrayArgument((String)expr.getArgument(0))) {
                        return expr;
                    }
                    return OperatorNode.create(toLocation(scope, elements.get(0)), ExpressionOperator.ARRAY, ImmutableList.of(expr));
                }
                List<OperatorNode<ExpressionOperator>> values = Lists.newArrayListWithExpectedSize(elements.size());
                for (Literal_elementContext child : elements) {
                    values.add(convertExpr(child.getChild(0), scope));
                }
                return OperatorNode.create(toLocation(scope, elements.get(0)), ExpressionOperator.ARRAY, values);
        }
        throw new ProgramCompileException(toLocation(scope, parseTree),
                "Unknown expression type: " + parseTree.getText());
    }

    public Object convertLiteral(Scalar_literalContext literal) {
        int parseTreeIndex = getParseTreeIndex(literal.getChild(0));
        String text = literal.getChild(0).getText();
        switch (parseTreeIndex) {
            case yqlplusParser.INT:
                return Integer.valueOf(text);
            case yqlplusParser.FLOAT:
                return Double.valueOf(text);
            case yqlplusParser.STRING:
                return StringUnescaper.unquote(text);
            case yqlplusParser.TRUE:
            case yqlplusParser.FALSE:
                return Boolean.valueOf(text);
            case yqlplusParser.LONG_INT:
                return Long.parseLong(text.substring(0, text.length() - 1));
            default:
                throw new ProgramCompileException("Unknow literal type " + text);
        }        
    }

    private Object readConstantExpression(OperatorNode<ExpressionOperator> node) {
        switch (node.getOperator()) {
            case LITERAL:
                return node.getArgument(0);
            case MAP: {
                ImmutableMap.Builder<String, Object> map = ImmutableMap.builder();
                List<String> names = (List<String>) node.getArgument(0);
                List<OperatorNode<ExpressionOperator>> exprs = (List<OperatorNode<ExpressionOperator>>) node.getArgument(1);
                for (int i = 0; i < names.size(); ++i) {
                    map.put(names.get(i), readConstantExpression(exprs.get(i)));
                }
                return map.build();
            }
            case ARRAY: {
                List<OperatorNode<ExpressionOperator>> exprs = (List<OperatorNode<ExpressionOperator>>) node.getArgument(0);
                ImmutableList.Builder<Object> lst = ImmutableList.builder();
                for (OperatorNode<ExpressionOperator> expr : exprs) {
                    lst.add(readConstantExpression(expr));
                }
                return lst.build();
            }
            default:
                throw new ProgramCompileException(node.getLocation(), "Internal error: Unknown constant expression type: " + node.getOperator());
        }
    }

    private OperatorNode<ExpressionOperator> readBinOp(ExpressionOperator op, ParseTree node, Scope scope) {
        assert node.getChildCount() == 3;
        return OperatorNode.create(op, convertExpr(node.getChild(0), scope), convertExpr(node.getChild(2), scope));
    }

    private OperatorNode<ExpressionOperator> readBinOp(ExpressionOperator op, ParseTree operand1, ParseTree operand2, Scope scope) {
        return OperatorNode.create(op, convertExpr(operand1, scope), convertExpr(operand2, scope));
    }

    private OperatorNode<ExpressionOperator> readConjOp(ExpressionOperator op, List<EqualityExpressionContext> nodes, Scope scope) {
        List<OperatorNode<ExpressionOperator>> arguments = Lists.newArrayListWithExpectedSize(nodes.size());
        for (ParseTree child : nodes) {
            arguments.add(convertExpr(child, scope));
        }
        return OperatorNode.create(op, arguments);
    }

    private OperatorNode<ExpressionOperator> readConjOrOp(ExpressionOperator op, LogicalORExpressionContext node, Scope scope) {
        List<LogicalANDExpressionContext> andExpressionList = node.logicalANDExpression();
        List<OperatorNode<ExpressionOperator>> arguments = Lists.newArrayListWithExpectedSize(andExpressionList.size());
        for (LogicalANDExpressionContext child : andExpressionList) {
            List<EqualityExpressionContext> equalities = child.equalityExpression();
            if (equalities.size() == 1) {
                arguments.add(convertExpr(equalities.get(0), scope));
            } else {
                List<OperatorNode<ExpressionOperator>> andArguments = Lists.newArrayListWithExpectedSize(equalities.size());
                for (EqualityExpressionContext subTreeChild : equalities) {
                    andArguments.add(convertExpr(subTreeChild, scope));
                }
                arguments.add(OperatorNode.create(ExpressionOperator.AND, andArguments));
            }
        }
        return OperatorNode.create(op, arguments);
    }

    // (IS_NULL | IS_NOT_NULL)
    // unaryExpression
    private OperatorNode<ExpressionOperator> readUnOp(ExpressionOperator op, ParseTree node, Scope scope) {
        assert (node instanceof TerminalNode) || (node.getChildCount() == 1) || (node instanceof UnaryExpressionContext);
        if (node instanceof TerminalNode) {
            return OperatorNode.create(op, convertExpr(node, scope));
        } else if (node.getChildCount() == 1) {
            return OperatorNode.create(op, convertExpr(node.getChild(0), scope));
        } else {
            return OperatorNode.create(op, convertExpr(node.getChild(1), scope));
        }
    }

    private OperatorNode<ExpressionOperator> readValues(Field_names_specContext nameDefs, Field_values_specContext values, Scope scope) {
        List<Field_defContext> fieldDefs = nameDefs.field_def();
        List<ExpressionContext> valueDefs = values.expression();
        assert fieldDefs.size() == valueDefs.size();
        List<String> fieldNames;
        List<OperatorNode<ExpressionOperator>> fieldValues;
        int numPairs = fieldDefs.size();
        fieldNames = Lists.newArrayListWithExpectedSize(numPairs);
        fieldValues = Lists.newArrayListWithExpectedSize(numPairs);
        for (int i = 0; i < numPairs; i++) {
            fieldNames.add((String) convertExpr(fieldDefs.get(i).expression(), scope).getArgument(1));
            fieldValues.add(convertExpr(valueDefs.get(i), scope));
        }
        return OperatorNode.create(ExpressionOperator.MAP, fieldNames, fieldValues);
    }

    private OperatorNode<ExpressionOperator> readValues(ParserRuleContext node, Scope scope) {
        List<String> fieldNames;
        List<OperatorNode<ExpressionOperator>> fieldValues;
        if (node.getRuleIndex() == yqlplusParser.RULE_field_def) {
            Field_defContext fieldDefContext = (Field_defContext) node;
            //TODO double check
            fieldNames = Lists.newArrayListWithExpectedSize(node.getChildCount());
            fieldValues = Lists.newArrayListWithExpectedSize(node.getChildCount());
            for (int i = 0; i < node.getChildCount(); i++) {
                fieldNames.add((String) convertExpr(node.getChild(i).getChild(0).getChild(0), scope).getArgument(1));
                fieldValues.add(convertExpr(node.getChild(i).getChild(0).getChild(1), scope));
            }
        } else {
            assert node.getChildCount() % 2 == 0;
            int numPairs = node.getChildCount() / 2;
            fieldNames = Lists.newArrayListWithExpectedSize(numPairs);
            fieldValues = Lists.newArrayListWithExpectedSize(numPairs);
            for (int i = 0; i < numPairs; i++) {
                fieldNames.add((String) convertExpr(node.getChild(i).getChild(0), scope).getArgument(1));
                fieldValues.add(convertExpr(node.getChild(numPairs + i), scope));
            }
        }
        return OperatorNode.create(ExpressionOperator.MAP, fieldNames, fieldValues);
    }

    /*
     * Converts node list
     *
     *   a_name, b_name, c_name, a_value_1, b_value_1, c_value_1, a_value_2, b_value_2, c_value2, a_value_3, b_value_3, c_value_3
     *
     * into corresponding constant sequence:
     *
     *   [ { a_name : a_value_1, b_name : b_value_1, c_name : c_value_1 }, ... ]
     *
     */
    private OperatorNode<SequenceOperator> readBatchValues(Field_names_specContext nameDefs, List<Field_values_group_specContext> valueGroups, Scope scope) {
        List<Field_defContext> nameContexts = nameDefs.field_def();
        List<String> fieldNames = Lists.newArrayList();
        for (Field_defContext nameContext : nameContexts) {
            fieldNames.add((String) convertExpr(nameContext.getChild(0), scope).getArgument(1));
        }
        List<OperatorNode> records = Lists.newArrayList();
        for (Field_values_group_specContext valueGorup : valueGroups) {
            List<ExpressionContext> expressionList = valueGorup.expression();
            List<OperatorNode<ExpressionOperator>> fieldValues = Lists.newArrayListWithExpectedSize(expressionList.size());
            for (ExpressionContext expressionContext : expressionList) {
                fieldValues.add(convertExpr(expressionContext, scope));
            }
            records.add(OperatorNode.create(ExpressionOperator.MAP, fieldNames, fieldValues));
        }
        // Return constant sequence of records with the given name/values
        return OperatorNode.create(SequenceOperator.EVALUATE, OperatorNode.create(ExpressionOperator.ARRAY, records));
    }

    /*
     * Scans the given node for READ_FIELD expressions.
     *
     * TODO: Search recursively and consider additional operators
     *
     * @param in the node to scan
     * @return list of READ_FIELD expressions
     */
    private List<OperatorNode<ExpressionOperator>> getReadFieldExpressions(OperatorNode<ExpressionOperator> in) {
        List<OperatorNode<ExpressionOperator>> readFieldList = Lists.newArrayList();
        switch (in.getOperator()) {
            case READ_FIELD:
                readFieldList.add(in);
                break;
            case CALL:
                List<OperatorNode<ExpressionOperator>> callArgs = in.getArgument(1);
                for (OperatorNode<ExpressionOperator> callArg : callArgs) {
                    if (callArg.getOperator() == ExpressionOperator.READ_FIELD) {
                        readFieldList.add(callArg);
                    }
                }
                break;
        }
        return readFieldList;
    }
}
