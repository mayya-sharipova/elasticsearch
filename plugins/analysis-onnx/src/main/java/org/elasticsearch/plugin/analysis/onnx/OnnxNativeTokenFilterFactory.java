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

import java.io.IOException;
import java.nio.file.Path;

/**
 * Factory for creating {@link OnnxNativeTokenFilter} instances.
 *
 * <p>This factory creates token filters that use native FFI (Panama FFM API)
 * to call ONNX Runtime directly, bypassing the Java wrapper.
 *
 * <p>Configuration settings:
 * <ul>
 *   <li>{@code model_path} - Path to ONNX model file (relative to config/analysis-onnx)</li>
 *   <li>{@code tokenizer_config} - Path to tokenizer.json (relative to config/analysis-onnx)</li>
 *   <li>{@code library_path} - (optional) Path to ONNX Runtime native library. If not specified,
 *       loads from standard system paths (LD_LIBRARY_PATH on Linux, DYLD_LIBRARY_PATH on macOS)</li>
 *   <li>{@code cls_token} - CLS token override (optional)</li>
 *   <li>{@code sep_token} - SEP token override (optional)</li>
 * </ul>
 *
 * <p>Example configuration (ONNX Runtime installed system-wide):
 * <pre>
 * {
 *   "settings": {
 *     "analysis": {
 *       "filter": {
 *         "my_lemmatizer": {
 *           "type": "onnx_bert_native",
 *           "model_path": "hebrew-lemmatizer/model.onnx",
 *           "tokenizer_config": "hebrew-lemmatizer/tokenizer.json"
 *         }
 *       }
 *     }
 *   }
 * }
 * </pre>
 */
public class OnnxNativeTokenFilterFactory extends AbstractTokenFilterFactory {

    private final OnnxNativeInferenceEngine engine;

    public OnnxNativeTokenFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(name);

        Path configDir = env.configDir().resolve("analysis-onnx");

        // Get model path
        String modelPathStr = settings.get("model_path");
        if (modelPathStr == null) {
            throw new IllegalArgumentException("[onnx_bert_native] filter requires 'model_path' setting");
        }
        Path modelPath = configDir.resolve(modelPathStr);

        // Get tokenizer config path
        String tokenizerConfigStr = settings.get("tokenizer_config");
        if (tokenizerConfigStr == null) {
            throw new IllegalArgumentException("[onnx_bert_native] filter requires 'tokenizer_config' setting");
        }
        Path tokenizerConfigPath = configDir.resolve(tokenizerConfigStr);

        // Get library path (optional - if not specified, loads from system paths)
        String libraryPathStr = settings.get("library_path");
        Path libraryPath = libraryPathStr != null ? configDir.resolve(libraryPathStr) : null;

        // Optional special token overrides
        String clsToken = settings.get("cls_token");
        String sepToken = settings.get("sep_token");

        try {
            this.engine = new OnnxNativeInferenceEngine(libraryPath, modelPath, tokenizerConfigPath, clsToken, sepToken);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to initialize native ONNX engine: " + e.getMessage(), e);
        }
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new OnnxNativeTokenFilter(tokenStream, engine);
    }
}
