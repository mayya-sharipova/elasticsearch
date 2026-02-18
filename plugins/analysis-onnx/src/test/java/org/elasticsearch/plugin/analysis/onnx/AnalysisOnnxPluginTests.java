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
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.hasKey;

public class AnalysisOnnxPluginTests extends ESTestCase {

    public void testPluginRegistersOnnxBertFilter() {
        AnalysisOnnxPlugin plugin = new AnalysisOnnxPlugin();
        Map<String, AnalysisProvider<TokenFilterFactory>> tokenFilters = plugin.getTokenFilters();
        assertThat(tokenFilters, hasKey("onnx_bert"));
    }
}
