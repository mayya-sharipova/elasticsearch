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
 * A token filter that runs ONNX BERT-style inference on the token stream.
 *
 * <p>Unlike typical token filters that process one token at a time, this filter:
 * <ol>
 *   <li>Collects ALL tokens first - BERT models need full sentence context</li>
 *   <li>Adds special tokens automatically - [CLS] at start, [SEP] at end</li>
 *   <li>Runs single inference - on the complete token sequence</li>
 *   <li>Emits results one-by-one - back to the token stream</li>
 * </ol>
 *
 * <p><b>Limitation:</b> BERT models typically have a maximum sequence length of 512 tokens.
 * Input exceeding this limit will be truncated, losing tokens beyond position 512.
 * For long documents, consider splitting into smaller fields or sentences.
 */
public class OnnxTokenFilter extends TokenFilter {

    private final OnnxInferenceEngine engine;
    private final CharTermAttribute termAttr = addAttribute(CharTermAttribute.class);
    private final PositionIncrementAttribute posIncAttr = addAttribute(PositionIncrementAttribute.class);

    private List<String> outputTokens = new ArrayList<>();
    private int outputIndex = 0;
    private boolean inputExhausted = false;

    public OnnxTokenFilter(TokenStream input, OnnxInferenceEngine engine) {
        super(input);
        this.engine = engine;
    }

    /**
     * Standard Lucene "collect-then-emit" pattern for filters that buffer output.
     *
     * Code structure: EMIT check comes before COLLECT because:
     * - First call: no buffered output → falls through to collect phase
     * - Subsequent calls: emits buffered tokens one by one
     * - Final: buffer empty + input exhausted → returns false
     */
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

        // PROCESS: Run inference on entire batch
        // engine.process() internally:
        // 1. Encodes tokens to WordPiece IDs
        // 2. Adds [CLS] at start and [SEP] at end
        // 3. Runs ONNX inference
        // 4. Decodes output (skipping [CLS]/[SEP])
        outputTokens = engine.process(inputTokens);
        outputIndex = 0;

        // Emit first token
        if (outputTokens.isEmpty() == false) {
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
