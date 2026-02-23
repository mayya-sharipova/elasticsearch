/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.onnx;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Token filter that runs ONNX BERT-style inference using native FFI (Panama FFM API).
 *
 * <p>This filter uses direct native calls to ONNX Runtime C API via Java's
 * Foreign Function &amp; Memory API, avoiding the Java wrapper and its shutdown
 * hook that conflicts with Elasticsearch entitlements.
 *
 * @see OnnxNativeInferenceEngine
 * @see OnnxTokenFilter for the Java wrapper-based implementation
 */
public final class OnnxNativeTokenFilter extends TokenFilter {

    private final OnnxNativeInferenceEngine engine;
    private final CharTermAttribute termAttr = addAttribute(CharTermAttribute.class);
    private final PositionIncrementAttribute posIncAttr = addAttribute(PositionIncrementAttribute.class);

    private List<String> outputTokens = new ArrayList<>();
    private int outputIndex = 0;
    private boolean inputExhausted = false;

    public OnnxNativeTokenFilter(TokenStream input, OnnxNativeInferenceEngine engine) {
        super(input);
        this.engine = engine;
    }

    @Override
    public boolean incrementToken() throws IOException {
        // EMIT: If we have buffered output tokens, emit them one-by-one
        if (outputIndex < outputTokens.size()) {
            clearAttributes();
            termAttr.setEmpty().append(outputTokens.get(outputIndex));
            posIncAttr.setPositionIncrement(1);
            outputIndex++;
            return true;
        }

        // If input already exhausted, we're done
        if (inputExhausted) {
            return false;
        }

        // COLLECT: Consume all input tokens (BERT models need full sentence context)
        List<String> inputTokens = new ArrayList<>();
        while (input.incrementToken()) {
            inputTokens.add(termAttr.toString());
        }
        inputExhausted = true;

        if (inputTokens.isEmpty()) {
            return false;
        }

        // PROCESS: Run native inference on entire batch
        outputTokens = engine.process(inputTokens);
        outputIndex = 0;

        // Emit first token
        if (!outputTokens.isEmpty()) {
            clearAttributes();
            termAttr.setEmpty().append(outputTokens.get(outputIndex));
            posIncAttr.setPositionIncrement(1);
            outputIndex++;
            return true;
        }

        return false;
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        outputTokens.clear();
        outputIndex = 0;
        inputExhausted = false;
    }
}
