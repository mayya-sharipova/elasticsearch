/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.onnx.tokenizers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * WordPiece tokenizer implementation that reads configuration from HuggingFace tokenizer.json.
 *
 * WordPiece algorithm:
 * 1. Try to match the entire word in vocabulary
 * 2. If not found, try to find the longest matching prefix
 * 3. Continue with remaining characters, prefixed with "##"
 * 4. If no match found at all, use [UNK] token
 */
public class WordPieceTokenizer implements AutoTokenizer {

    private final Map<String, Integer> vocab;
    private final String[] vocabArray;
    private final String unkToken;
    private final String continuingSubwordPrefix;
    private final int maxInputCharsPerWord;
    private final int configuredClsTokenId;
    private final int configuredSepTokenId;

    @SuppressWarnings("unchecked")
    public WordPieceTokenizer(Map<String, Object> config) {
        this.vocab = new HashMap<>();
        Map<String, Object> model = (Map<String, Object>) config.get("model");

        // Load vocabulary
        Map<String, Object> vocabObj = (Map<String, Object>) model.get("vocab");
        int maxId = 0;
        for (Map.Entry<String, Object> entry : vocabObj.entrySet()) {
            int id = ((Number) entry.getValue()).intValue();
            vocab.put(entry.getKey(), id);
            maxId = Math.max(maxId, id);
        }

        // Build reverse lookup array
        this.vocabArray = new String[maxId + 1];
        for (Map.Entry<String, Integer> entry : vocab.entrySet()) {
            vocabArray[entry.getValue()] = entry.getKey();
        }

        // Get WordPiece-specific settings
        this.unkToken = model.containsKey("unk_token") ? (String) model.get("unk_token") : "[UNK]";
        this.continuingSubwordPrefix = model.containsKey("continuing_subword_prefix")
            ? (String) model.get("continuing_subword_prefix")
            : "##";
        this.maxInputCharsPerWord = model.containsKey("max_input_chars_per_word")
            ? ((Number) model.get("max_input_chars_per_word")).intValue()
            : 100;

        // Parse special tokens from post_processor section
        int[] specialTokenIds = parseSpecialTokens(config);
        this.configuredClsTokenId = specialTokenIds[0];
        this.configuredSepTokenId = specialTokenIds[1];
    }

    /**
     * Parse CLS and SEP token IDs from post_processor.special_tokens section.
     * Returns [-1, -1] if not found.
     */
    @SuppressWarnings("unchecked")
    private int[] parseSpecialTokens(Map<String, Object> config) {
        int clsId = -1;
        int sepId = -1;

        Map<String, Object> postProcessor = (Map<String, Object>) config.get("post_processor");
        if (postProcessor == null) {
            return new int[] { clsId, sepId };
        }

        // Get the template to find which tokens are CLS and SEP
        List<Map<String, Object>> single = (List<Map<String, Object>>) postProcessor.get("single");
        Map<String, Object> specialTokens = (Map<String, Object>) postProcessor.get("special_tokens");

        if (single == null || specialTokens == null) {
            return new int[] { clsId, sepId };
        }

        // First SpecialToken in template is CLS, last is SEP
        String clsTokenName = null;
        String sepTokenName = null;

        for (Map<String, Object> item : single) {
            Map<String, Object> specialToken = (Map<String, Object>) item.get("SpecialToken");
            if (specialToken != null) {
                String tokenId = (String) specialToken.get("id");
                if (clsTokenName == null) {
                    clsTokenName = tokenId;
                }
                sepTokenName = tokenId; // Last one becomes SEP
            }
        }

        // Look up IDs in special_tokens section
        if (clsTokenName != null && specialTokens.containsKey(clsTokenName)) {
            Map<String, Object> tokenInfo = (Map<String, Object>) specialTokens.get(clsTokenName);
            List<Number> ids = (List<Number>) tokenInfo.get("ids");
            if (ids != null && ids.isEmpty() == false) {
                clsId = ids.get(0).intValue();
            }
        }

        if (sepTokenName != null && specialTokens.containsKey(sepTokenName)) {
            Map<String, Object> tokenInfo = (Map<String, Object>) specialTokens.get(sepTokenName);
            List<Number> ids = (List<Number>) tokenInfo.get("ids");
            if (ids != null && ids.isEmpty() == false) {
                sepId = ids.get(0).intValue();
            }
        }

        return new int[] { clsId, sepId };
    }

    @Override
    public int[] encode(String word) {
        if (word.length() > maxInputCharsPerWord) {
            // Word too long, return [UNK]
            Integer unkId = vocab.get(unkToken);
            return unkId != null ? new int[] { unkId } : new int[0];
        }

        List<Integer> tokens = new ArrayList<>();
        int start = 0;

        while (start < word.length()) {
            int end = word.length();
            String curSubstr = null;
            Integer curId = null;

            while (start < end) {
                String substr = word.substring(start, end);
                if (start > 0) {
                    substr = continuingSubwordPrefix + substr;
                }

                Integer id = vocab.get(substr);
                if (id != null) {
                    curSubstr = substr;
                    curId = id;
                    break;
                }
                end--;
            }

            if (curSubstr == null) {
                // No match found, use [UNK] for entire word
                Integer unkId = vocab.get(unkToken);
                return unkId != null ? new int[] { unkId } : new int[0];
            }

            tokens.add(curId);
            start = end;
        }

        return tokens.stream().mapToInt(Integer::intValue).toArray();
    }

    @Override
    public String decode(int tokenId) {
        if (tokenId >= 0 && tokenId < vocabArray.length) {
            return vocabArray[tokenId];
        }
        return unkToken;
    }

    @Override
    public int getTokenId(String token) {
        Integer id = vocab.get(token);
        return id != null ? id : -1;
    }

    @Override
    public String[] getVocab() {
        return vocabArray;
    }

    @Override
    public int getConfiguredClsTokenId() {
        return configuredClsTokenId;
    }

    @Override
    public int getConfiguredSepTokenId() {
        return configuredSepTokenId;
    }
}
