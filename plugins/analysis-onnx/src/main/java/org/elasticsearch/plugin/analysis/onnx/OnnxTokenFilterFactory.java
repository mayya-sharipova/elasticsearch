/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.onnx;

import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;

import java.nio.file.Path;

/**
 * Factory for creating ONNX BERT token filters.
 *
 * Configuration:
 * - model: path to ONNX model file (relative to ES config directory)
 * - tokenizer: path to tokenizer.json file in HuggingFace Tokenizers format.
 *              Can be downloaded from a HuggingFace model page.
 * - cls_token: (optional) start token override - auto-detected from tokenizer.json if not specified
 * - sep_token: (optional) end token override - auto-detected from tokenizer.json if not specified
 *
 * Example:
 * <pre>
 * {
 *   "type": "onnx_bert",
 *   "model": "dictabert-lex.onnx",
 *   "tokenizer": "tokenizer.json"
 * }
 * </pre>
 */
public class OnnxTokenFilterFactory extends AbstractTokenFilterFactory {

    private final OnnxInferenceEngine engine;

    public OnnxTokenFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(name);

        String modelPath = settings.get("model");
        String tokenizerPath = settings.get("tokenizer");
        String clsToken = settings.get("cls_token");  // null if not specified
        String sepToken = settings.get("sep_token");  // null if not specified

        // Defer validation - allows ES to probe the filter type without errors
        // Actual validation happens when the filter is used
        if (modelPath == null || tokenizerPath == null) {
            this.engine = null;
            return;
        }

        Path modelFile = env.configDir().resolve(modelPath);
        Path tokenizerFile = env.configDir().resolve(tokenizerPath);

        this.engine = new OnnxInferenceEngine(modelFile, tokenizerFile, clsToken, sepToken);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        if (engine == null) {
            throw new IllegalArgumentException("onnx_bert token filter requires 'model' and 'tokenizer' settings");
        }
        return new OnnxTokenFilter(tokenStream, engine);
    }
}
