/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.onnx.tokenizers;

import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * Factory that auto-detects and creates the appropriate tokenizer based on tokenizer.json config.
 */
public class TokenizerFactory {

    /**
     * Create a tokenizer from a HuggingFace tokenizer.json configuration file.
     * Auto-detects the tokenizer type from the "model.type" field.
     *
     * @param tokenizerJson path to the tokenizer.json file
     * @return the appropriate AutoTokenizer implementation
     * @throws IOException if the file cannot be read
     * @throws IllegalArgumentException if the tokenizer type is not supported
     */
    @SuppressWarnings("unchecked")
    public static AutoTokenizer fromConfig(Path tokenizerJson) throws IOException {
        Map<String, Object> config;
        try (InputStream in = Files.newInputStream(tokenizerJson)) {
            try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, in)) {
                config = parser.map();
            }
        }

        Map<String, Object> model = (Map<String, Object>) config.get("model");
        if (model == null) {
            throw new IllegalArgumentException("tokenizer.json missing 'model' section");
        }

        String type = (String) model.get("type");

        return switch (type) {
            case "WordPiece" -> new WordPieceTokenizer(config);
            case "BPE" -> throw new UnsupportedOperationException(
                "BPE tokenizer is not yet supported. Only WordPiece tokenizers are currently supported."
            );
            case "Unigram" -> throw new UnsupportedOperationException(
                "Unigram/SentencePiece tokenizer is not yet supported. Only WordPiece tokenizers are currently supported."
            );
            default -> throw new IllegalArgumentException("Unsupported tokenizer type: " + type);
        };
    }
}
