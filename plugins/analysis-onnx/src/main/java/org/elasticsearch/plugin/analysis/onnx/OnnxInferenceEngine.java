/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.onnx;

import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;

import org.elasticsearch.plugin.analysis.onnx.tokenizers.AutoTokenizer;
import org.elasticsearch.plugin.analysis.onnx.tokenizers.TokenizerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * ONNX Runtime inference engine for BERT-style models.
 *
 * Handles:
 * - Loading ONNX model and tokenizer configuration
 * - Encoding tokens to WordPiece IDs
 * - Adding start/end special tokens (auto-detected from tokenizer.json or configurable)
 * - Running inference
 * - Decoding output logits back to tokens
 */
public class OnnxInferenceEngine implements AutoCloseable {

    private static final String DEFAULT_CLS_TOKEN = "[CLS]";
    private static final String DEFAULT_SEP_TOKEN = "[SEP]";

    private final OrtEnvironment env;
    private final OrtSession session;
    private final AutoTokenizer tokenizer;
    private final String[] vocab;
    private final int clsTokenId;
    private final int sepTokenId;

    /**
     * Create an inference engine with optional user-specified special tokens.
     * Fallback chain: user settings → tokenizer.json config → defaults ([CLS]/[SEP])
     *
     * @param modelPath path to ONNX model file
     * @param tokenizerConfigPath path to tokenizer.json file
     * @param userClsToken user-specified CLS token, or null to auto-detect
     * @param userSepToken user-specified SEP token, or null to auto-detect
     */
    public OnnxInferenceEngine(Path modelPath, Path tokenizerConfigPath, String userClsToken, String userSepToken) {
        try {
            this.env = OrtEnvironment.getEnvironment();

            OrtSession.SessionOptions opts = new OrtSession.SessionOptions();
            opts.setOptimizationLevel(OrtSession.SessionOptions.OptLevel.ALL_OPT);
            opts.setInterOpNumThreads(1);
            opts.setIntraOpNumThreads(1);

            this.session = env.createSession(modelPath.toString(), opts);
            this.tokenizer = TokenizerFactory.fromConfig(tokenizerConfigPath);
            this.vocab = tokenizer.getVocab();

            // Resolve CLS token ID with fallback chain:
            // 1. User-provided setting
            // 2. Configured in tokenizer.json post_processor.special_tokens
            // 3. Default [CLS]
            this.clsTokenId = resolveSpecialTokenId(userClsToken, tokenizer.getConfiguredClsTokenId(), DEFAULT_CLS_TOKEN, "cls_token");
            this.sepTokenId = resolveSpecialTokenId(userSepToken, tokenizer.getConfiguredSepTokenId(), DEFAULT_SEP_TOKEN, "sep_token");

        } catch (OrtException e) {
            throw new RuntimeException("Failed to initialize ONNX session: " + e.getMessage(), e);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load tokenizer config: " + e.getMessage(), e);
        }
    }

    private int resolveSpecialTokenId(String userToken, int configuredId, String defaultToken, String tokenName) {
        // 1. User-provided setting
        if (userToken != null) {
            int id = tokenizer.getTokenId(userToken);
            if (id == -1) {
                throw new IllegalArgumentException("Tokenizer vocabulary missing " + tokenName + ": " + userToken);
            }
            return id;
        }

        // 2. Configured in tokenizer.json
        if (configuredId != -1) {
            return configuredId;
        }

        // 3. Default
        int id = tokenizer.getTokenId(defaultToken);
        if (id == -1) {
            throw new IllegalArgumentException(
                "Could not determine " + tokenName + ": not in user settings, tokenizer.json, or default (" + defaultToken + ")"
            );
        }
        return id;
    }

    /**
     * Process a list of tokens through the ONNX model.
     *
     * @param tokens input tokens (words from the analyzer chain)
     * @return processed tokens (e.g., lemmatized forms)
     */
    public List<String> process(List<String> tokens) {
        try {
            // Step 1: Encode each word token to WordPiece IDs
            List<int[]> wordPieceIds = new ArrayList<>();
            for (String token : tokens) {
                wordPieceIds.add(tokenizer.encode(token));
            }

            // Step 2: Build input sequence with special tokens: [CLS] + word_pieces + [SEP]
            List<Integer> allIds = new ArrayList<>();
            allIds.add(clsTokenId);
            for (int[] ids : wordPieceIds) {
                for (int id : ids) {
                    allIds.add(id);
                }
            }
            allIds.add(sepTokenId);

            // Step 3: Create input tensors
            int seqLen = allIds.size();
            long[][] inputIds = new long[1][seqLen];
            long[][] attentionMask = new long[1][seqLen];
            long[][] tokenTypeIds = new long[1][seqLen];

            for (int i = 0; i < seqLen; i++) {
                inputIds[0][i] = allIds.get(i);
                attentionMask[0][i] = 1;  // All tokens attended
                tokenTypeIds[0][i] = 0;   // Single segment
            }

            // Step 4: Run ONNX inference
            float[][] logits = runInference(inputIds, attentionMask, tokenTypeIds);

            // Step 5: Decode output - map logits back to tokens, skip [CLS]/[SEP] positions
            return decodeOutput(tokens, wordPieceIds, logits);

        } catch (OrtException e) {
            throw new RuntimeException("ONNX inference failed: " + e.getMessage(), e);
        }
    }

    private float[][] runInference(long[][] inputIds, long[][] attentionMask, long[][] tokenTypeIds) throws OrtException {
        try (
            OnnxTensor idsTensor = OnnxTensor.createTensor(env, inputIds);
            OnnxTensor maskTensor = OnnxTensor.createTensor(env, attentionMask);
            OnnxTensor typeTensor = OnnxTensor.createTensor(env, tokenTypeIds)
        ) {
            Map<String, OnnxTensor> inputs = Map.of("input_ids", idsTensor, "attention_mask", maskTensor, "token_type_ids", typeTensor);

            try (OrtSession.Result results = session.run(inputs)) {
                OnnxTensor output = (OnnxTensor) results.get(0);
                float[][][] data = (float[][][]) output.getValue();
                return data[0];  // Return [seq_len, vocab_size] logits
            }
        }
    }

    private List<String> decodeOutput(List<String> originalTokens, List<int[]> wordPieceIds, float[][] logits) {
        List<String> results = new ArrayList<>();
        int logitIdx = 1;  // Skip [CLS] at position 0

        for (int i = 0; i < originalTokens.size(); i++) {
            int[] wpIds = wordPieceIds.get(i);

            // Get top prediction for first wordpiece of each word
            int predId = argmax(logits[logitIdx]);
            String lemma = vocab[predId];

            // Skip special tokens in output, use original token as fallback
            if (lemma.startsWith("[") == false && lemma.startsWith("##") == false) {
                results.add(lemma);
            } else {
                results.add(originalTokens.get(i));  // Fallback to original
            }

            logitIdx += wpIds.length;  // Move past all wordpieces of this word
        }

        return results;
    }

    private int argmax(float[] arr) {
        int maxIdx = 0;
        for (int i = 1; i < arr.length; i++) {
            if (arr[i] > arr[maxIdx]) {
                maxIdx = i;
            }
        }
        return maxIdx;
    }

    @Override
    public void close() throws Exception {
        if (session != null) {
            session.close();
        }
    }
}
