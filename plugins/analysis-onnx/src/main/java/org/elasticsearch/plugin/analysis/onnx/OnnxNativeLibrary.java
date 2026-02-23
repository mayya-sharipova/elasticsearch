/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.onnx;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Native FFI bindings for ONNX Runtime C API using Panama Foreign Function &amp; Memory API.
 *
 * <p>This class provides direct access to the ONNX Runtime C API without going through
 * the Java wrapper, avoiding shutdown hook registration that conflicts with ES entitlements.
 *
 * <p>ONNX Runtime C API structure:
 * <pre>
 * // Get the API base (exported function)
 * const OrtApiBase* api_base = OrtGetApiBase();
 *
 * // Get versioned API struct (struct of function pointers)
 * const OrtApi* api = api_base-&gt;GetApi(ORT_API_VERSION);
 *
 * // Call through function pointers
 * api-&gt;CreateEnv(ORT_LOGGING_LEVEL_WARNING, "es", &amp;env);
 * api-&gt;CreateSession(env, model_path, options, &amp;session);
 * api-&gt;Run(session, ...);
 * </pre>
 */
public final class OnnxNativeLibrary {

    private static final Linker LINKER = Linker.nativeLinker();
    private static final int ORT_API_VERSION = 18;  // ONNX Runtime 1.17.x

    // Native library symbol lookup
    private static volatile SymbolLookup libraryLookup;

    // Cached method handles
    private static volatile MethodHandle ortGetApiBase$mh;
    private static volatile MemorySegment ortApi;

    // OrtApi function pointer offsets (from ONNX Runtime C API headers)
    // These offsets are for ORT_API_VERSION 18
    private static final long CREATE_ENV_OFFSET = 8 * 1;       // CreateEnv is 2nd function
    private static final long CREATE_SESSION_OPTIONS_OFFSET = 8 * 11;
    private static final long CREATE_SESSION_OFFSET = 8 * 14;
    private static final long RUN_OFFSET = 8 * 22;
    private static final long RELEASE_ENV_OFFSET = 8 * 239;
    private static final long RELEASE_SESSION_OFFSET = 8 * 240;

    private OnnxNativeLibrary() {}

    /**
     * Initializes the ONNX Runtime native library from standard system paths.
     * Uses LD_LIBRARY_PATH on Linux, DYLD_LIBRARY_PATH on macOS, or PATH on Windows.
     *
     * @throws UnsatisfiedLinkError if the library cannot be loaded
     */
    public static synchronized void initialize() {
        initialize(null);
    }

    /**
     * Initializes the ONNX Runtime native library.
     *
     * @param libraryPath Optional path to the ONNX Runtime native library.
     *                    If null, loads from standard system paths (LD_LIBRARY_PATH, etc.)
     * @throws UnsatisfiedLinkError if the library cannot be loaded
     */
    public static synchronized void initialize(Path libraryPath) {
        if (libraryLookup != null) {
            return;  // Already initialized
        }

        if (libraryPath != null) {
            // Explicit path provided - load from that location
            if (Files.notExists(libraryPath)) {
                throw new UnsatisfiedLinkError("ONNX Runtime library not found: " + libraryPath);
            }
            libraryLookup = SymbolLookup.libraryLookup(libraryPath, Arena.global());
        } else {
            // Load from standard system paths (LD_LIBRARY_PATH, DYLD_LIBRARY_PATH, etc.)
            try {
                System.loadLibrary("onnxruntime");
                // Use loader lookup to find symbols in libraries loaded via System.loadLibrary
                libraryLookup = SymbolLookup.loaderLookup();
            } catch (UnsatisfiedLinkError e) {
                throw new UnsatisfiedLinkError(
                    "ONNX Runtime library not found in system paths. "
                        + "Install ONNX Runtime or set LD_LIBRARY_PATH (Linux), "
                        + "DYLD_LIBRARY_PATH (macOS), or specify 'library_path' setting. "
                        + "Original error: "
                        + e.getMessage()
                );
            }
        }

        // Get OrtGetApiBase function
        Optional<MemorySegment> ortGetApiBaseOpt = libraryLookup.find("OrtGetApiBase");
        if (ortGetApiBaseOpt.isEmpty()) {
            throw new UnsatisfiedLinkError("OrtGetApiBase not found in ONNX Runtime library");
        }

        // OrtGetApiBase() returns OrtApiBase*
        FunctionDescriptor getApiBaseDesc = FunctionDescriptor.of(ValueLayout.ADDRESS);
        ortGetApiBase$mh = LINKER.downcallHandle(ortGetApiBaseOpt.get(), getApiBaseDesc);

        try {
            // Call OrtGetApiBase() to get OrtApiBase*
            MemorySegment apiBase = (MemorySegment) ortGetApiBase$mh.invokeExact();

            // OrtApiBase is a struct with GetApi as first function pointer:
            // struct OrtApiBase {
            // const OrtApi*(ORT_API_CALL* GetApi)(uint32_t version);
            // const char*(ORT_API_CALL* GetVersionString)(void);
            // };
            // Read GetApi function pointer (first 8 bytes)
            MemorySegment getApiFnPtr = apiBase.reinterpret(16).get(ValueLayout.ADDRESS, 0);

            // Call GetApi(ORT_API_VERSION) to get OrtApi*
            FunctionDescriptor getApiDesc = FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_INT);
            MethodHandle getApi$mh = LINKER.downcallHandle(getApiFnPtr, getApiDesc);
            ortApi = (MemorySegment) getApi$mh.invokeExact(ORT_API_VERSION);

            // Reinterpret to access the full OrtApi struct
            // OrtApi has hundreds of function pointers, we need access to at least ~250 * 8 bytes
            ortApi = ortApi.reinterpret(8 * 300);

        } catch (Throwable t) {
            throw new RuntimeException("Failed to initialize ONNX Runtime native API", t);
        }
    }

    /**
     * Creates an ONNX Runtime environment.
     *
     * @param arena The memory arena for allocations
     * @param name The environment name
     * @return Pointer to OrtEnv
     */
    public static MemorySegment createEnv(Arena arena, String name) {
        checkInitialized();

        try {
            // Get CreateEnv function pointer from OrtApi
            MemorySegment createEnvFnPtr = ortApi.get(ValueLayout.ADDRESS, CREATE_ENV_OFFSET);

            // OrtStatus* CreateEnv(OrtLoggingLevel log_level, const char* logid, OrtEnv** out)
            FunctionDescriptor createEnvDesc = FunctionDescriptor.of(
                ValueLayout.ADDRESS,  // OrtStatus* return
                ValueLayout.JAVA_INT, // log_level
                ValueLayout.ADDRESS,  // logid (const char*)
                ValueLayout.ADDRESS   // out (OrtEnv**)
            );

            MethodHandle createEnv$mh = LINKER.downcallHandle(createEnvFnPtr, createEnvDesc);

            // Allocate output pointer
            MemorySegment envOut = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment nameStr = arena.allocateUtf8String(name);

            // Call CreateEnv with log level WARNING (2)
            MemorySegment status = (MemorySegment) createEnv$mh.invokeExact(2, nameStr, envOut);

            checkStatus(status);
            return envOut.get(ValueLayout.ADDRESS, 0);

        } catch (Throwable t) {
            throw new RuntimeException("Failed to create ONNX environment", t);
        }
    }

    /**
     * Creates session options.
     *
     * @param arena The memory arena for allocations
     * @return Pointer to OrtSessionOptions
     */
    public static MemorySegment createSessionOptions(Arena arena) {
        checkInitialized();

        try {
            MemorySegment createSessionOptionsFnPtr = ortApi.get(ValueLayout.ADDRESS, CREATE_SESSION_OPTIONS_OFFSET);

            // OrtStatus* CreateSessionOptions(OrtSessionOptions** options)
            FunctionDescriptor desc = FunctionDescriptor.of(
                ValueLayout.ADDRESS,  // OrtStatus* return
                ValueLayout.ADDRESS   // options (OrtSessionOptions**)
            );

            MethodHandle mh = LINKER.downcallHandle(createSessionOptionsFnPtr, desc);
            MemorySegment optionsOut = arena.allocate(ValueLayout.ADDRESS);

            MemorySegment status = (MemorySegment) mh.invokeExact(optionsOut);
            checkStatus(status);

            return optionsOut.get(ValueLayout.ADDRESS, 0);

        } catch (Throwable t) {
            throw new RuntimeException("Failed to create session options", t);
        }
    }

    /**
     * Creates an inference session.
     *
     * @param arena The memory arena for allocations
     * @param env OrtEnv pointer
     * @param modelPath Path to the ONNX model
     * @param options OrtSessionOptions pointer
     * @return Pointer to OrtSession
     */
    public static MemorySegment createSession(Arena arena, MemorySegment env, Path modelPath, MemorySegment options) {
        checkInitialized();

        try {
            MemorySegment createSessionFnPtr = ortApi.get(ValueLayout.ADDRESS, CREATE_SESSION_OFFSET);

            // OrtStatus* CreateSession(OrtEnv* env, const ORTCHAR_T* model_path,
            // const OrtSessionOptions* options, OrtSession** out)
            FunctionDescriptor desc = FunctionDescriptor.of(
                ValueLayout.ADDRESS,  // OrtStatus* return
                ValueLayout.ADDRESS,  // env
                ValueLayout.ADDRESS,  // model_path
                ValueLayout.ADDRESS,  // options
                ValueLayout.ADDRESS   // out (OrtSession**)
            );

            MethodHandle mh = LINKER.downcallHandle(createSessionFnPtr, desc);
            MemorySegment sessionOut = arena.allocate(ValueLayout.ADDRESS);

            // On non-Windows, ORTCHAR_T is char (UTF-8)
            MemorySegment pathStr = arena.allocateUtf8String(modelPath.toAbsolutePath().toString());

            MemorySegment status = (MemorySegment) mh.invokeExact(env, pathStr, options, sessionOut);
            checkStatus(status);

            return sessionOut.get(ValueLayout.ADDRESS, 0);

        } catch (Throwable t) {
            throw new RuntimeException("Failed to create ONNX session: " + modelPath, t);
        }
    }

    /**
     * Checks if the library is initialized.
     */
    public static boolean isInitialized() {
        return libraryLookup != null && ortApi != null;
    }

    private static void checkInitialized() {
        if (!isInitialized()) {
            throw new IllegalStateException("ONNX Runtime native library not initialized. Call initialize() first.");
        }
    }

    private static void checkStatus(MemorySegment status) {
        // NULL status means success
        if (!status.equals(MemorySegment.NULL)) {
            // TODO: Extract error message from OrtStatus using GetErrorMessage
            throw new RuntimeException("ONNX Runtime operation failed (status != null)");
        }
    }

    /**
     * Gets the OrtApi struct pointer for advanced operations.
     *
     * @return The OrtApi memory segment
     */
    public static MemorySegment getOrtApi() {
        checkInitialized();
        return ortApi;
    }
}
