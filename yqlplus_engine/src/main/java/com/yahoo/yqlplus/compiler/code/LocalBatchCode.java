/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.compiler.code;

import com.google.common.base.Preconditions;
import com.yahoo.yqlplus.compiler.generate.PopSequence;
import org.objectweb.asm.Label;

import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalBatchCode implements LocalCodeChunk {
    private static abstract class Command {
        final LocalFrame frame;
        Command next;

        protected Command(LocalFrame frame) {
            this.frame = frame;
        }

        protected CodeEmitter before(CodeEmitter code) {
            return code;
        }

        protected void during(CodeEmitter scope) {

        }

        protected void after(CodeEmitter scope) {

        }

        public Command add(Command next) {
            Preconditions.checkArgument(next.next == null, "Unexpected new node has next already set?");
            if (this.next != null) {
                next.next = this.next;
            }
            this.next = next;
            return next;
        }

    }

    private static class FrameCommand extends Command {
        FrameCommand(LocalFrame frame) {
            super(frame);
        }

        @Override
        protected CodeEmitter before(CodeEmitter code) {
            return code.createScope(frame);
        }

        @Override
        protected void after(CodeEmitter scope) {
            scope.endScope();
        }
    }

    private static class SequenceCommand extends Command {
        private final BytecodeSequence seq;

        private SequenceCommand(LocalFrame frame, BytecodeSequence seq) {
            super(frame);
            this.seq = seq;
        }

        @Override
        protected void during(CodeEmitter scope) {
            seq.generate(scope);
        }
    }

    private final AtomicInteger symbols;
    private final Command head;
    private Command tail;

    public String gensym(String prefix) {
        return prefix + symbols.getAndIncrement();
    }

    public LocalBatchCode(LocalFrame parent) {
        this.symbols = new AtomicInteger(0);
        this.head = new FrameCommand(new LocalFrame(parent));
        this.tail = head;
    }

    private LocalBatchCode(AtomicInteger symbols, Command head) {
        this.symbols = symbols;
        this.head = head;
        this.tail = head;
    }

    @Override
    public AssignableValue allocate(TypeWidget type) {
        return allocate(gensym("$local"), type);
    }

    @Override
    public void alias(String name, String alias) {
        tail.frame.alias(name, alias);
    }

    @Override
    public AssignableValue allocate(String name, TypeWidget type) {
        return tail.frame.allocate(name, type);
    }


    @Override
    public void execute(BytecodeSequence code) {
        add(code);
        if (code instanceof BytecodeExpression) {
            add(new PopSequence(((BytecodeExpression) code).getType()));
        }
    }

    @Override
    public AssignableValue evaluate(BytecodeExpression expr) {
        AssignableValue local = allocate(expr.getType());
        add(local.write(expr));
        return local;
    }

    @Override
    public AssignableValue evaluate(String name, BytecodeExpression expr) {
        AssignableValue local = allocate(name, expr.getType());
        add(local.write(expr));
        return local;
    }


    @Override
    public AssignableValue getLocal(String name) {
        return tail.frame.get(name);
    }

    @Override
    public LocalCodeChunk block() {
        LocalCodeChunk child = child();
        add(child);
        return child;
    }

    @Override
    public LocalCodeChunk child() {
        return new LocalBatchCode(symbols, new FrameCommand(new LocalFrame(tail.frame)));
    }

    @Override
    public void generate(CodeEmitter code) {
        Command current = head;
        Stack<CodeEmitter> stack = new Stack<>();
        Stack<Command> commands = new Stack<>();
        while (current != null) {
            CodeEmitter newCode = current.before(code);
            current.during(newCode);
            commands.push(current);
            stack.push(newCode);
            current = current.next;
            code = newCode;
        }
        while (!commands.isEmpty()) {
            code = stack.pop();
            current = commands.pop();
            current.after(code);
        }
    }

    @Override
    public Label getStart() {
        return head.frame.startFrame;
    }

    @Override
    public Label getEnd() {
        return head.frame.endFrame;
    }

    @Override
    public LocalCodeChunk point() {
        FrameCommand frameCommand = new FrameCommand(new LocalFrame(tail.frame));
        tail = tail.add(frameCommand);
        LocalBatchCode lc = new LocalBatchCode(symbols, tail);
        tail = tail.add(new SequenceCommand(tail.frame, BytecodeSequence.NOOP));
        return lc;
    }

    @Override
    public void add(BytecodeSequence code) {
        tail = tail.add(new SequenceCommand(tail.frame, code));
    }
}
