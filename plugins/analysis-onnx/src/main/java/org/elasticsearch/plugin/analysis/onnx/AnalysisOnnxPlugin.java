/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.onnx;

import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Map;

/**
 * Plugin that provides ONNX token filters for BERT-style model inference in analysis chains.
 *
 * <p>Registers two token filter types:
 * <ul>
 *   <li>{@code onnx_bert} - Uses ONNX Runtime Java wrapper (patched to remove shutdown hook)</li>
 *   <li>{@code onnx_bert_native} - Uses native FFI via Panama Foreign Function &amp; Memory API</li>
 * </ul>
 *
 * <p>The native implementation ({@code onnx_bert_native}) bypasses the Java wrapper entirely,
 * calling ONNX Runtime C API directly. This avoids shutdown hook issues but requires the
 * ONNX Runtime native library to be installed on the system.
 */
public class AnalysisOnnxPlugin extends Plugin implements AnalysisPlugin {

    @Override
    public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        return Map.of("onnx_bert", OnnxTokenFilterFactory::new, "onnx_bert_native", OnnxNativeTokenFilterFactory::new);
    }
}
