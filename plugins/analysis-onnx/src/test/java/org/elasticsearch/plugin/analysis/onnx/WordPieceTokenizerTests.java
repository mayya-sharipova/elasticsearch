/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.onnx;

import org.elasticsearch.plugin.analysis.onnx.tokenizers.WordPieceTokenizer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

public class WordPieceTokenizerTests extends ESTestCase {

    private WordPieceTokenizer createTokenizer(String vocabJson) throws IOException {
        String configJson = "{\"model\": {\"type\": \"WordPiece\", \"unk_token\": \"[UNK]\", "
            + "\"continuing_subword_prefix\": \"##\", \"max_input_chars_per_word\": 100, "
            + "\"vocab\": "
            + vocabJson
            + "}}";
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, configJson)) {
            Map<String, Object> config = parser.map();
            return new WordPieceTokenizer(config);
        }
    }

    public void testEncodeSimpleWord() throws IOException {
        WordPieceTokenizer tokenizer = createTokenizer("""
            {"hello": 0, "world": 1, "[UNK]": 2, "[CLS]": 3, "[SEP]": 4}
            """);

        int[] ids = tokenizer.encode("hello");
        assertEquals(1, ids.length);
        assertEquals(0, ids[0]);
    }

    public void testEncodeWithSubwords() throws IOException {
        WordPieceTokenizer tokenizer = createTokenizer("""
            {"un": 0, "##known": 1, "unknown": 2, "[UNK]": 3, "[CLS]": 4, "[SEP]": 5}
            """);

        // Should prefer full word match
        int[] ids = tokenizer.encode("unknown");
        assertEquals(1, ids.length);
        assertEquals(2, ids[0]);
    }

    public void testEncodeSubwordSplit() throws IOException {
        WordPieceTokenizer tokenizer = createTokenizer("""
            {"un": 0, "##know": 1, "##n": 2, "[UNK]": 3, "[CLS]": 4, "[SEP]": 5}
            """);

        // Should split into subwords: "un" + "##know" + "##n"
        int[] ids = tokenizer.encode("unknown");
        assertEquals(3, ids.length);
        assertEquals(0, ids[0]);  // "un"
        assertEquals(1, ids[1]);  // "##know"
        assertEquals(2, ids[2]);  // "##n"
    }

    public void testEncodeUnknownWord() throws IOException {
        WordPieceTokenizer tokenizer = createTokenizer("""
            {"hello": 0, "[UNK]": 1, "[CLS]": 2, "[SEP]": 3}
            """);

        int[] ids = tokenizer.encode("xyz");
        assertEquals(1, ids.length);
        assertEquals(1, ids[0]);  // [UNK]
    }

    public void testDecode() throws IOException {
        WordPieceTokenizer tokenizer = createTokenizer("""
            {"hello": 0, "world": 1, "[UNK]": 2, "[CLS]": 3, "[SEP]": 4}
            """);

        assertEquals("hello", tokenizer.decode(0));
        assertEquals("world", tokenizer.decode(1));
        assertEquals("[CLS]", tokenizer.decode(3));
    }

    public void testGetTokenId() throws IOException {
        WordPieceTokenizer tokenizer = createTokenizer("""
            {"hello": 0, "[CLS]": 1, "[SEP]": 2, "[UNK]": 3}
            """);

        assertEquals(1, tokenizer.getTokenId("[CLS]"));
        assertEquals(2, tokenizer.getTokenId("[SEP]"));
        assertEquals(-1, tokenizer.getTokenId("[MASK]"));  // Not in vocab
    }

    public void testGetVocab() throws IOException {
        WordPieceTokenizer tokenizer = createTokenizer("""
            {"a": 0, "b": 1, "c": 2}
            """);

        String[] vocab = tokenizer.getVocab();
        assertEquals(3, vocab.length);
        assertEquals("a", vocab[0]);
        assertEquals("b", vocab[1]);
        assertEquals("c", vocab[2]);
    }
}
