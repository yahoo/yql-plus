/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.code;

public interface CodeBlock extends CodePrinter {
    String gensym();

    /**
     * Render the chunk of code rooted at this point into the stringbuilder.
     *
     * @param output
     */
    void render(StringBuilder output);

    /**
     * Indent 3 spaces.
     */
    void indent();

    /**
     * Dedent 3 spaces.
     */
    void dedent();


    /**
     * Insert a block at the current point. Code added to the block will appear before lines added to this object after this point.
     *
     * @return
     */
    CodeBlock block();

    /**
     * Insert a dynamic chunk code at the current point. The Renderable will be invoked during render (for each render).
     *
     * @param block
     */
    CodeBlock add(CodeRenderable block);
}
