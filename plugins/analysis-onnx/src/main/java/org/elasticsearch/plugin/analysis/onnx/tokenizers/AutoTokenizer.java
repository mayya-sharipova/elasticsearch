/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.onnx.tokenizers;

/**
 * Interface for tokenizers that encode text into token IDs for ONNX models.
 */
public interface AutoTokenizer {

    /**
     * Encode a word into token IDs (e.g., WordPiece subword IDs).
     *
     * @param word the word to encode
     * @return array of token IDs
     */
    int[] encode(String word);

    /**
     * Decode a token ID back to its string representation.
     *
     * @param tokenId the token ID to decode
     * @return the string representation of the token
     */
    String decode(int tokenId);

    /**
     * Get the token ID for a specific token string.
     *
     * @param token the token string (e.g., "[CLS]", "[SEP]")
     * @return the token ID, or -1 if not found
     */
    int getTokenId(String token);

    /**
     * Get the vocabulary as an array where index = token ID.
     *
     * @return the vocabulary array
     */
    String[] getVocab();

    /**
     * Get the CLS (start) token ID from tokenizer config's post_processor.special_tokens.
     *
     * @return the CLS token ID, or -1 if not configured
     */
    int getConfiguredClsTokenId();

    /**
     * Get the SEP (end) token ID from tokenizer config's post_processor.special_tokens.
     *
     * @return the SEP token ID, or -1 if not configured
     */
    int getConfiguredSepTokenId();
}
