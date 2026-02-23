/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.onnx;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.plugin.analysis.onnx.tokenizers.AutoTokenizer;
import org.elasticsearch.plugin.analysis.onnx.tokenizers.TokenizerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * ONNX inference engine using native FFI (Panama Foreign Function &amp; Memory API).
 *
 * <p>This implementation directly calls the ONNX Runtime C API without using
 * the Java wrapper, avoiding shutdown hook issues with ES entitlements.
 *
 * <p><b>Key differences from Java wrapper approach:</b>
 * <ul>
 *   <li>No shutdown hook registration (main benefit)</li>
 *   <li>Direct memory management with Arena scopes</li>
 *   <li>Requires explicit function pointer handling for OrtApi struct</li>
 * </ul>
 */
public final class OnnxNativeInferenceEngine implements Closeable {

    private static final Logger logger = LogManager.getLogger(OnnxNativeInferenceEngine.class);

    private static final Linker LINKER = Linker.nativeLinker();
    private static final String DEFAULT_CLS_TOKEN = "[CLS]";
    private static final String DEFAULT_SEP_TOKEN = "[SEP]";

    // OrtApi function pointer offsets (ORT_API_VERSION 18)
    // These offsets are derived from the OrtApi struct in onnxruntime_c_api.h
    private static final long CREATE_MEMORY_INFO_OFFSET = 8 * 60;
    private static final long CREATE_TENSOR_WITH_DATA_OFFSET = 8 * 63;
    private static final long RUN_OFFSET = 8 * 22;
    private static final long GET_TENSOR_DATA_OFFSET = 8 * 104;
    private static final long RELEASE_VALUE_OFFSET = 8 * 243;
    private static final long RELEASE_MEMORY_INFO_OFFSET = 8 * 245;
    private static final long RELEASE_SESSION_OFFSET = 8 * 240;
    private static final long RELEASE_SESSION_OPTIONS_OFFSET = 8 * 241;
    private static final long RELEASE_ENV_OFFSET = 8 * 239;

    private final Arena arena;
    private final MemorySegment env;
    private final MemorySegment session;
    private final MemorySegment sessionOptions;
    private final AutoTokenizer tokenizer;
    private final String[] vocab;
    private final int clsTokenId;
    private final int sepTokenId;
    private volatile boolean closed = false;

    /**
     * Creates a native ONNX inference engine.
     *
     * @param libraryPath Path to ONNX Runtime native library
     * @param modelPath Path to ONNX model file
     * @param tokenizerConfigPath Path to tokenizer.json file
     * @param userClsToken User-specified CLS token (null for auto-detect)
     * @param userSepToken User-specified SEP token (null for auto-detect)
     */
    public OnnxNativeInferenceEngine(Path libraryPath, Path modelPath, Path tokenizerConfigPath, String userClsToken, String userSepToken)
        throws IOException {
        this.arena = Arena.ofShared();

        // Load tokenizer
        this.tokenizer = TokenizerFactory.fromConfig(tokenizerConfigPath);
        this.vocab = tokenizer.getVocab();

        // Resolve special token IDs
        this.clsTokenId = resolveSpecialTokenId(userClsToken, tokenizer.getConfiguredClsTokenId(), DEFAULT_CLS_TOKEN, "cls_token");
        this.sepTokenId = resolveSpecialTokenId(userSepToken, tokenizer.getConfiguredSepTokenId(), DEFAULT_SEP_TOKEN, "sep_token");

        // Initialize native library
        OnnxNativeLibrary.initialize(libraryPath);

        // Create ONNX Runtime objects
        this.env = OnnxNativeLibrary.createEnv(arena, "es-onnx-native");
        this.sessionOptions = OnnxNativeLibrary.createSessionOptions(arena);
        this.session = OnnxNativeLibrary.createSession(arena, env, modelPath, sessionOptions);
    }

    private int resolveSpecialTokenId(String userToken, int configuredId, String defaultToken, String tokenName) {
        if (userToken != null) {
            int id = tokenizer.getTokenId(userToken);
            if (id == -1) {
                throw new IllegalArgumentException("Tokenizer vocabulary missing " + tokenName + ": " + userToken);
            }
            return id;
        }
        if (configuredId != -1) {
            return configuredId;
        }
        int id = tokenizer.getTokenId(defaultToken);
        if (id == -1) {
            throw new IllegalArgumentException(
                "Could not determine " + tokenName + ": not in user settings, tokenizer.json, or default (" + defaultToken + ")"
            );
        }
        return id;
    }

    /**
     * Processes a list of tokens through the ONNX model.
     *
     * @param tokens Input tokens (words from the analyzer chain)
     * @return Processed tokens (e.g., lemmatized forms)
     */
    public List<String> process(List<String> tokens) {
        if (closed) {
            throw new IllegalStateException("Engine is closed");
        }

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

        // Step 3: Run native inference
        float[][] logits = runNativeInference(allIds);

        // Step 4: Decode output
        return decodeOutput(tokens, wordPieceIds, logits);
    }

    /**
     * Runs inference using native ONNX Runtime C API.
     */
    private float[][] runNativeInference(List<Integer> inputIds) {
        try (Arena inferenceArena = Arena.ofConfined()) {
            MemorySegment ortApi = OnnxNativeLibrary.getOrtApi();
            int seqLen = inputIds.size();

            // Create memory info for CPU
            MemorySegment memoryInfo = createCpuMemoryInfo(inferenceArena, ortApi);

            // Create input tensors
            MemorySegment inputIdsTensor = createInt64Tensor(inferenceArena, ortApi, memoryInfo, inputIds, seqLen);
            MemorySegment attentionMaskTensor = createAttentionMaskTensor(inferenceArena, ortApi, memoryInfo, seqLen);
            MemorySegment tokenTypeIdsTensor = createTokenTypeIdsTensor(inferenceArena, ortApi, memoryInfo, seqLen);

            // Run inference
            MemorySegment outputTensor = runSession(inferenceArena, ortApi, inputIdsTensor, attentionMaskTensor, tokenTypeIdsTensor);

            // Extract output data
            float[][] logits = extractLogits(inferenceArena, ortApi, outputTensor, seqLen);

            // Release tensors
            releaseValue(ortApi, inputIdsTensor);
            releaseValue(ortApi, attentionMaskTensor);
            releaseValue(ortApi, tokenTypeIdsTensor);
            releaseValue(ortApi, outputTensor);
            releaseMemoryInfo(ortApi, memoryInfo);

            return logits;

        } catch (Throwable t) {
            throw new RuntimeException("Native inference failed", t);
        }
    }

    private MemorySegment createCpuMemoryInfo(Arena arena, MemorySegment ortApi) throws Throwable {
        MemorySegment fnPtr = ortApi.get(ValueLayout.ADDRESS, CREATE_MEMORY_INFO_OFFSET);
        // OrtStatus* CreateCpuMemoryInfo(OrtAllocatorType type, OrtMemType mem_type, OrtMemoryInfo** out)
        FunctionDescriptor desc = FunctionDescriptor.of(
            ValueLayout.ADDRESS,
            ValueLayout.JAVA_INT,  // allocator type (0 = OrtDeviceAllocator)
            ValueLayout.JAVA_INT,  // mem type (0 = OrtMemTypeCPU)
            ValueLayout.ADDRESS    // out
        );
        MethodHandle mh = LINKER.downcallHandle(fnPtr, desc);
        MemorySegment out = arena.allocate(ValueLayout.ADDRESS);
        MemorySegment status = (MemorySegment) mh.invokeExact(0, 0, out);
        checkStatus(status);
        return out.get(ValueLayout.ADDRESS, 0);
    }

    private MemorySegment createInt64Tensor(Arena arena, MemorySegment ortApi, MemorySegment memoryInfo, List<Integer> values, int seqLen)
        throws Throwable {
        // Allocate data buffer - use allocate(byteSize) for Java 21 compatibility
        long byteSize = (long) seqLen * Long.BYTES;
        MemorySegment data = arena.allocate(byteSize);
        for (int i = 0; i < seqLen; i++) {
            data.setAtIndex(ValueLayout.JAVA_LONG, i, values.get(i).longValue());
        }

        // Shape: [1, seqLen]
        MemorySegment shape = arena.allocate(2L * Long.BYTES);
        shape.setAtIndex(ValueLayout.JAVA_LONG, 0, 1L);
        shape.setAtIndex(ValueLayout.JAVA_LONG, 1, (long) seqLen);

        return createTensorWithData(arena, ortApi, memoryInfo, data, byteSize, shape, 2, 7); // 7 = ONNX_TENSOR_ELEMENT_DATA_TYPE_INT64
    }

    private MemorySegment createAttentionMaskTensor(Arena arena, MemorySegment ortApi, MemorySegment memoryInfo, int seqLen)
        throws Throwable {
        long byteSize = (long) seqLen * Long.BYTES;
        MemorySegment data = arena.allocate(byteSize);
        for (int i = 0; i < seqLen; i++) {
            data.setAtIndex(ValueLayout.JAVA_LONG, i, 1L); // All ones
        }

        MemorySegment shape = arena.allocate(2L * Long.BYTES);
        shape.setAtIndex(ValueLayout.JAVA_LONG, 0, 1L);
        shape.setAtIndex(ValueLayout.JAVA_LONG, 1, (long) seqLen);

        return createTensorWithData(arena, ortApi, memoryInfo, data, byteSize, shape, 2, 7);
    }

    private MemorySegment createTokenTypeIdsTensor(Arena arena, MemorySegment ortApi, MemorySegment memoryInfo, int seqLen)
        throws Throwable {
        long byteSize = (long) seqLen * Long.BYTES;
        MemorySegment data = arena.allocate(byteSize);
        // All zeros for single segment (already zero-initialized)

        MemorySegment shape = arena.allocate(2L * Long.BYTES);
        shape.setAtIndex(ValueLayout.JAVA_LONG, 0, 1L);
        shape.setAtIndex(ValueLayout.JAVA_LONG, 1, (long) seqLen);

        return createTensorWithData(arena, ortApi, memoryInfo, data, byteSize, shape, 2, 7);
    }

    private MemorySegment createTensorWithData(
        Arena arena,
        MemorySegment ortApi,
        MemorySegment memoryInfo,
        MemorySegment data,
        long dataSize,
        MemorySegment shape,
        int shapeDims,
        int elementType
    ) throws Throwable {
        MemorySegment fnPtr = ortApi.get(ValueLayout.ADDRESS, CREATE_TENSOR_WITH_DATA_OFFSET);
        // OrtStatus* CreateTensorWithDataAsOrtValue(OrtMemoryInfo*, void* data, size_t size,
        // int64_t* shape, size_t shape_len, ONNXTensorElementDataType type, OrtValue** out)
        FunctionDescriptor desc = FunctionDescriptor.of(
            ValueLayout.ADDRESS,
            ValueLayout.ADDRESS,   // memoryInfo
            ValueLayout.ADDRESS,   // data
            ValueLayout.JAVA_LONG, // size
            ValueLayout.ADDRESS,   // shape
            ValueLayout.JAVA_LONG, // shape_len
            ValueLayout.JAVA_INT,  // element type
            ValueLayout.ADDRESS    // out
        );
        MethodHandle mh = LINKER.downcallHandle(fnPtr, desc);
        MemorySegment out = arena.allocate(ValueLayout.ADDRESS);
        MemorySegment status = (MemorySegment) mh.invokeExact(memoryInfo, data, dataSize, shape, (long) shapeDims, elementType, out);
        checkStatus(status);
        return out.get(ValueLayout.ADDRESS, 0);
    }

    private MemorySegment runSession(
        Arena arena,
        MemorySegment ortApi,
        MemorySegment inputIdsTensor,
        MemorySegment attentionMaskTensor,
        MemorySegment tokenTypeIdsTensor
    ) throws Throwable {
        MemorySegment runFnPtr = ortApi.get(ValueLayout.ADDRESS, RUN_OFFSET);

        // Prepare input names array (3 pointers)
        long ptrSize = ValueLayout.ADDRESS.byteSize();
        MemorySegment inputNames = arena.allocate(3 * ptrSize);
        inputNames.set(ValueLayout.ADDRESS, 0, allocateString(arena, "input_ids"));
        inputNames.set(ValueLayout.ADDRESS, ptrSize, allocateString(arena, "attention_mask"));
        inputNames.set(ValueLayout.ADDRESS, 2 * ptrSize, allocateString(arena, "token_type_ids"));

        // Prepare input tensors array
        MemorySegment inputs = arena.allocate(3 * ptrSize);
        inputs.set(ValueLayout.ADDRESS, 0, inputIdsTensor);
        inputs.set(ValueLayout.ADDRESS, ptrSize, attentionMaskTensor);
        inputs.set(ValueLayout.ADDRESS, 2 * ptrSize, tokenTypeIdsTensor);

        // Prepare output names array
        MemorySegment outputNames = arena.allocate(ptrSize);
        outputNames.set(ValueLayout.ADDRESS, 0, allocateString(arena, "logits"));

        // Prepare output tensor pointer
        MemorySegment outputs = arena.allocate(ptrSize);

        // OrtStatus* Run(OrtSession*, OrtRunOptions*, const char* const* input_names,
        // const OrtValue* const* inputs, size_t input_len,
        // const char* const* output_names, size_t output_len, OrtValue** outputs)
        FunctionDescriptor desc = FunctionDescriptor.of(
            ValueLayout.ADDRESS,
            ValueLayout.ADDRESS,   // session
            ValueLayout.ADDRESS,   // run_options (NULL)
            ValueLayout.ADDRESS,   // input_names
            ValueLayout.ADDRESS,   // inputs
            ValueLayout.JAVA_LONG, // input_len
            ValueLayout.ADDRESS,   // output_names
            ValueLayout.JAVA_LONG, // output_len
            ValueLayout.ADDRESS    // outputs
        );

        MethodHandle mh = LINKER.downcallHandle(runFnPtr, desc);
        MemorySegment status = (MemorySegment) mh.invokeExact(
            session,
            MemorySegment.NULL,
            inputNames,
            inputs,
            3L,
            outputNames,
            1L,
            outputs
        );
        checkStatus(status);

        return outputs.get(ValueLayout.ADDRESS, 0);
    }

    /**
     * Allocates a null-terminated UTF-8 string (Java 21 compatible).
     */
    private MemorySegment allocateString(Arena arena, String s) {
        // Java 21: allocateUtf8String; Java 22+: allocateFrom
        return arena.allocateUtf8String(s);
    }

    private float[][] extractLogits(Arena arena, MemorySegment ortApi, MemorySegment outputTensor, int seqLen) throws Throwable {
        // Get tensor data pointer
        MemorySegment getDataFnPtr = ortApi.get(ValueLayout.ADDRESS, GET_TENSOR_DATA_OFFSET);
        FunctionDescriptor getDataDesc = FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS);
        MethodHandle getDataMh = LINKER.downcallHandle(getDataFnPtr, getDataDesc);

        MemorySegment dataOut = arena.allocate(ValueLayout.ADDRESS);
        MemorySegment status = (MemorySegment) getDataMh.invokeExact(outputTensor, dataOut);
        checkStatus(status);

        MemorySegment dataPtr = dataOut.get(ValueLayout.ADDRESS, 0);

        // Output shape is [1, seqLen, vocabSize]
        int vocabSize = vocab.length;
        dataPtr = dataPtr.reinterpret((long) seqLen * vocabSize * Float.BYTES);

        float[][] logits = new float[seqLen][vocabSize];
        for (int i = 0; i < seqLen; i++) {
            for (int j = 0; j < vocabSize; j++) {
                logits[i][j] = dataPtr.getAtIndex(ValueLayout.JAVA_FLOAT, (long) i * vocabSize + j);
            }
        }

        return logits;
    }

    private void releaseValue(MemorySegment ortApi, MemorySegment value) throws Throwable {
        if (value == null || value.equals(MemorySegment.NULL)) return;
        MemorySegment fnPtr = ortApi.get(ValueLayout.ADDRESS, RELEASE_VALUE_OFFSET);
        FunctionDescriptor desc = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);
        MethodHandle mh = LINKER.downcallHandle(fnPtr, desc);
        mh.invokeExact(value);
    }

    private void releaseMemoryInfo(MemorySegment ortApi, MemorySegment memInfo) throws Throwable {
        if (memInfo == null || memInfo.equals(MemorySegment.NULL)) return;
        MemorySegment fnPtr = ortApi.get(ValueLayout.ADDRESS, RELEASE_MEMORY_INFO_OFFSET);
        FunctionDescriptor desc = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);
        MethodHandle mh = LINKER.downcallHandle(fnPtr, desc);
        mh.invokeExact(memInfo);
    }

    private List<String> decodeOutput(List<String> originalTokens, List<int[]> wordPieceIds, float[][] logits) {
        List<String> results = new ArrayList<>();
        int logitIdx = 1; // Skip [CLS] at position 0

        for (int i = 0; i < originalTokens.size(); i++) {
            int[] wpIds = wordPieceIds.get(i);

            // Get top prediction for first wordpiece of each word
            int predId = argmax(logits[logitIdx]);
            String lemma = vocab[predId];

            // Skip special tokens in output, use original token as fallback
            if (!lemma.startsWith("[") && !lemma.startsWith("##")) {
                results.add(lemma);
            } else {
                results.add(originalTokens.get(i)); // Fallback to original
            }

            logitIdx += wpIds.length; // Move past all wordpieces of this word
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

    private void checkStatus(MemorySegment status) {
        if (!status.equals(MemorySegment.NULL)) {
            throw new RuntimeException("ONNX Runtime operation failed");
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;

        try {
            releaseOnnxResources();
        } finally {
            arena.close();
        }
    }

    private void releaseOnnxResources() {
        MemorySegment ortApi = OnnxNativeLibrary.getOrtApi();

        try {
            if (session != null && !session.equals(MemorySegment.NULL)) {
                MemorySegment fn = ortApi.get(ValueLayout.ADDRESS, RELEASE_SESSION_OFFSET);
                FunctionDescriptor desc = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);
                MethodHandle mh = LINKER.downcallHandle(fn, desc);
                mh.invokeExact(session);
            }

            if (sessionOptions != null && !sessionOptions.equals(MemorySegment.NULL)) {
                MemorySegment fn = ortApi.get(ValueLayout.ADDRESS, RELEASE_SESSION_OPTIONS_OFFSET);
                FunctionDescriptor desc = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);
                MethodHandle mh = LINKER.downcallHandle(fn, desc);
                mh.invokeExact(sessionOptions);
            }

            if (env != null && !env.equals(MemorySegment.NULL)) {
                MemorySegment fn = ortApi.get(ValueLayout.ADDRESS, RELEASE_ENV_OFFSET);
                FunctionDescriptor desc = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);
                MethodHandle mh = LINKER.downcallHandle(fn, desc);
                mh.invokeExact(env);
            }
        } catch (Throwable t) {
            logger.warn("Failed to release ONNX resources", t);
        }
    }
}
