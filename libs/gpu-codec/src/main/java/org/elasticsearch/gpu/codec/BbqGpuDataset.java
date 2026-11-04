/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gpu.codec;

import com.nvidia.cuvs.BbqQuantizer;
import com.nvidia.cuvs.CagraIndexParams;
import com.nvidia.cuvs.CuVSHostMatrix;
import com.nvidia.cuvs.CuVSMatrix;
import com.nvidia.cuvs.CuVSResources;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * The BBQ-quantized vectors of a merged segment, on GPU, for building a CAGRA graph with NN-descent from the quantized
 * vectors rather than from the raw float vectors.
 * <p>
 * It mirrors how the CPU merge of {@code bbq_hnsw} scores vectors while building the graph (see
 * {@code ES818BinaryQuantizedVectorsReader#getRandomVectorScorerSupplierForMerge}): every vector is represented twice, by
 * its 1-bit code as the "document" and its 4-bit code as the "query", and pairs are scored asymmetrically, with the
 * same optimized scalar quantization (OSQ) corrections and the same centroid. So the dataset is a pair of cuVS
 * {@link BbqQuantizer}s:
 * <ul>
 *     <li>{@link BbqQuantizer.CodeLayout#PACKED_1B}: the 1-bit codes and corrections the flat format has already written
 *     for the merged segment, read back rather than recomputed. The bit order matches cuVS, but the flat format pads each
 *     code to a multiple of 64 dimensions, so only the first {@code dims / 8} bytes are copied.</li>
 *     <li>{@link BbqQuantizer.CodeLayout#PACKED_4B}: the 4-bit codes, quantized from the raw float vectors as the CPU merge
 *     does. They are packed as consecutive nibbles rather than in the bit-planes the CPU uses, which selects the int4
 *     tensor-core NN-descent kernel.</li>
 * </ul>
 * The data sent to the GPU takes 4.5 bits per dimension, plus a few corrections per vector, instead of 32 bits for floats.
 * <p>
 * Like the CPU merge, which writes its 4-bit vectors to a temporary file, the vectors are staged in a temporary file of
 * the segment's directory (see {@link StagedVectors}) rather than in memory, so that staging a large merge takes page
 * cache, which the OS can reclaim, rather than native memory. The file is deleted once uploaded.
 * <p>
 * cuVS reads the quantizers through views, so the device matrices must outlive any index built from them: close this
 * dataset only after the index, and before releasing the resources it was uploaded with.
 */
final class BbqGpuDataset implements AutoCloseable {

    /** Bits of the "query" codes, as for the CPU merge; see {@code BinaryQuantizer#B_QUERY} */
    private static final byte QUERY_BITS = 4;
    /** Per-vector values of each quantizer: lower and upper interval, additional correction, component sum, delta, delta sum */
    private static final int PER_VECTOR_VALUES = 6;

    private final List<CuVSMatrix> deviceMatrices;
    private final BbqQuantizer[] quantizers;
    private boolean closed;

    private BbqGpuDataset(List<CuVSMatrix> deviceMatrices, BbqQuantizer[] quantizers) {
        this.deviceMatrices = deviceMatrices;
        this.quantizers = quantizers;
    }

    /**
     * Whether a CAGRA graph can be built from the BBQ-quantized vectors of this dimension: cuVS's BBQ NN-descent needs the
     * dimension to be a multiple of 32.
     */
    static boolean isSupported(int dims) {
        return dims > 0 && dims % 32 == 0;
    }

    /** The bytes one vector takes on GPU: both codes, the per-vector values of both quantizers, and its row norm */
    static int bytesPerVector(int dims) {
        return dims / 8 + dims / 2 + (2 * PER_VECTOR_VALUES + 1) * Float.BYTES;
    }

    /**
     * The cuVS metric for a similarity function. Cosine vectors are normalized before they are quantized, so cosine is
     * scored as an inner product, which doesn't need the per-vector norms.
     */
    static CagraIndexParams.CuvsDistanceType metric(VectorSimilarityFunction similarityFunction) {
        return switch (similarityFunction) {
            case EUCLIDEAN -> CagraIndexParams.CuvsDistanceType.L2Expanded;
            case COSINE, DOT_PRODUCT, MAXIMUM_INNER_PRODUCT -> CagraIndexParams.CuvsDistanceType.InnerProduct;
        };
    }

    /**
     * Stages the quantized vectors in a temporary file, then uploads them to the GPU of the given resources, and deletes
     * the file.
     *
     * @param resources the resources to allocate the device memory with
     * @param directory the directory of the merged segment, in which to write the temporary file
     * @param segmentName the name of the merged segment, the prefix of the temporary file
     * @param context the context to create the temporary file with
     * @param binaryVectors the 1-bit vectors of the merged segment, in ordinal order
     * @param rawVectors the raw float vectors of the merged segment, in the same ordinal order
     * @param similarityFunction the similarity function of the field
     */
    static BbqGpuDataset create(
        CuVSResources resources,
        Directory directory,
        String segmentName,
        IOContext context,
        QuantizedByteVectorValues binaryVectors,
        FloatVectorValues rawVectors,
        VectorSimilarityFunction similarityFunction
    ) throws IOException {
        try (
            StagedVectors staged = StagedVectors.write(
                directory,
                segmentName,
                context,
                binaryVectors,
                rawVectors,
                similarityFunction,
                StagedVectors.BUFFER_BYTES
            )
        ) {
            return staged.upload(resources);
        }
    }

    /**
     * Packs 4-bit codes as consecutive nibbles, dimension {@code 2i} in the high nibble of byte {@code i}: cuVS's
     * {@link BbqQuantizer.CodeLayout#PACKED_4B}, which differs from both Lucene's {@code packNibbles} and the bit-planes of
     * {@code ESVectorUtil#stride4BitValues}.
     */
    static void packNibbles(int[] codes, byte[] packed) {
        assert codes.length == packed.length * 2;
        for (int i = 0; i < packed.length; i++) {
            packed[i] = (byte) ((codes[2 * i] << 4) | (codes[2 * i + 1] & 0x0F));
        }
    }

    /** The width of one quantization step: cuVS's {@code dequant_delta}, {@code (upper - lower) / (2^bits - 1)} */
    static float dequantDelta(float lower, float upper, int maxCode) {
        return (upper - lower) / maxCode;
    }

    BbqQuantizer[] quantizers() {
        return quantizers;
    }

    /** Frees the device memory; idempotent. Close only after any index built from this dataset is closed. */
    @Override
    public void close() {
        if (closed == false) {
            closed = true;
            deviceMatrices.forEach(CuVSMatrix::close);
        }
    }

    private static CuVSMatrix toDevice(
        CuVSResources resources,
        List<CuVSMatrix> deviceMatrices,
        MemorySegment segment,
        int rows,
        int columns,
        CuVSMatrix.DataType dataType
    ) {
        CuVSHostMatrix hostMatrix = (CuVSHostMatrix) DatasetUtilsImpl.fromMemorySegment(segment, rows, columns, dataType);
        // copies synchronously, so the segment can be unmapped as soon as it returns
        CuVSMatrix deviceMatrix = hostMatrix.toDevice(resources);
        deviceMatrices.add(deviceMatrix);
        return deviceMatrix;
    }

    /** A section of the staged file: one contiguous array, as cuVS takes it */
    record Section(long offset, long length) {
        long end() {
            return offset + length;
        }
    }

    /** The sections of one quantizer's code and per-vector values */
    record QuantizerSections(
        int codeBytes,
        Section codes,
        Section lowerIntervals,
        Section upperIntervals,
        Section additionalCorrections,
        Section componentSums,
        Section deltas,
        Section deltaSums
    ) {}

    /**
     * The quantized vectors, staged in one temporary file of the segment's directory, of one section per array cuVS takes.
     * <p>
     * cuVS needs each array contiguous, while the flat format (and the CPU merge's temporary file of 4-bit vectors)
     * interleaves each code with its corrections, so neither can be uploaded as is. The sizes of all arrays are known up
     * front, so the file is laid out in sections, and written in a single pass over the vectors: each value is appended to
     * a small buffer of its section, which is written at the section's next position when full. The file is then mapped
     * once, and each section uploaded in one copy. Closing deletes the file.
     */
    static final class StagedVectors implements Closeable {
        /** The size of each section's write buffer, so about 1MB for all sections; it must hold at least one code */
        static final int BUFFER_BYTES = 64 * 1024;

        private final FSDirectory directory;
        private final int numVectors;
        private final int dims;
        private final float[] centroid;
        private final float centroidDP;
        private final VectorSimilarityFunction similarityFunction;
        private final QuantizerSections docSections;
        private final QuantizerSections querySections;
        private final Section rowNorms;
        private final long fileLength;
        private String tempFile;

        private StagedVectors(
            FSDirectory directory,
            int numVectors,
            int dims,
            float[] centroid,
            float centroidDP,
            VectorSimilarityFunction similarityFunction
        ) {
            this.directory = directory;
            this.numVectors = numVectors;
            this.dims = dims;
            this.centroid = centroid;
            this.centroidDP = centroidDP;
            this.similarityFunction = similarityFunction;
            // every section's length is a multiple of 4 bytes, as dims is a multiple of 32, so every 4-byte value is aligned
            long[] offset = new long[1];
            this.docSections = quantizerSections(offset, dims / 8);
            this.querySections = quantizerSections(offset, dims / 2);
            this.rowNorms = nextSection(offset, (long) numVectors * Float.BYTES);
            this.fileLength = offset[0];
        }

        private QuantizerSections quantizerSections(long[] offset, int codeBytes) {
            long perVectorBytes = (long) numVectors * Float.BYTES;
            return new QuantizerSections(
                codeBytes,
                nextSection(offset, (long) numVectors * codeBytes),
                nextSection(offset, perVectorBytes),
                nextSection(offset, perVectorBytes),
                nextSection(offset, perVectorBytes),
                nextSection(offset, perVectorBytes),
                nextSection(offset, perVectorBytes),
                nextSection(offset, perVectorBytes)
            );
        }

        private static Section nextSection(long[] offset, long length) {
            Section section = new Section(offset[0], length);
            offset[0] += length;
            return section;
        }

        /**
         * Writes the quantized vectors to a temporary file: the 1-bit vectors read back from the flat format, and the 4-bit
         * vectors quantized from the raw vectors, as the CPU merge does.
         *
         * @param bufferBytes the size of each section's write buffer, see {@link #BUFFER_BYTES}
         */
        static StagedVectors write(
            Directory directory,
            String segmentName,
            IOContext context,
            QuantizedByteVectorValues binaryVectors,
            FloatVectorValues rawVectors,
            VectorSimilarityFunction similarityFunction,
            int bufferBytes
        ) throws IOException {
            final int numVectors = binaryVectors.size();
            final int dims = binaryVectors.dimension();
            assert isSupported(dims) : "unsupported dimension: " + dims;
            if (bufferBytes < dims / 2) {
                throw new IllegalArgumentException("a buffer of [" + bufferBytes + "] bytes can't hold a code of [" + dims + "] dims");
            }
            assert binaryVectors.getScalarEncoding() == QuantizedByteVectorValues.ScalarEncoding.SINGLE_BIT_QUERY_NIBBLE
                : "unexpected encoding: " + binaryVectors.getScalarEncoding();
            if (rawVectors.size() != numVectors || rawVectors.dimension() != dims) {
                throw new IllegalStateException(
                    "raw vectors ["
                        + rawVectors.size()
                        + "x"
                        + rawVectors.dimension()
                        + "] don't match the quantized vectors ["
                        + numVectors
                        + "x"
                        + dims
                        + "]"
                );
            }
            StagedVectors staged = new StagedVectors(
                MemorySegmentUtils.unwrapFSDirectory(directory),
                numVectors,
                dims,
                binaryVectors.getCentroid(),
                binaryVectors.getCentroidDP(),
                similarityFunction
            );
            boolean success = false;
            try {
                // creates the file, with a unique name, as the CPU merge creates its temporary file
                try (IndexOutput output = staged.directory.createTempOutput(segmentName, "bbq_gpu", context)) {
                    staged.tempFile = output.getName();
                }
                staged.writeFile(binaryVectors, rawVectors, bufferBytes);
                success = true;
                return staged;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(staged);
                }
            }
        }

        private void writeFile(QuantizedByteVectorValues binaryVectors, FloatVectorValues rawVectors, int bufferBytes) throws IOException {
            try (FileChannel channel = FileChannel.open(path(), StandardOpenOption.WRITE)) {
                QuantizerWriter doc = new QuantizerWriter(channel, docSections, bufferBytes);
                QuantizerWriter query = new QuantizerWriter(channel, querySections, bufferBytes);
                SectionWriter rowNormsWriter = new SectionWriter(channel, rowNorms, bufferBytes);

                OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(similarityFunction);
                float[] normalized = similarityFunction == VectorSimilarityFunction.COSINE ? new float[dims] : null;
                float[] residualScratch = new float[dims];
                int[] queryCodes = new int[dims];
                byte[] packedQueryCode = new byte[dims / 2];
                for (int ord = 0; ord < numVectors; ord++) {
                    // 1-bit "document" side: what the flat format wrote for this merge
                    byte[] docCode = binaryVectors.vectorValue(ord);
                    assert docCode.length >= dims / 8;
                    var docCorrections = binaryVectors.getCorrectiveTerms(ord);
                    doc.add(
                        docCode,
                        docCorrections.lowerInterval(),
                        docCorrections.upperInterval(),
                        docCorrections.additionalCorrection(),
                        docCorrections.quantizedComponentSum(),
                        1
                    );

                    // 4-bit "query" side: quantized from the raw vector against the same centroid, as the CPU merge does
                    float[] vector = rawVectors.vectorValue(ord);
                    if (normalized != null) {
                        System.arraycopy(vector, 0, normalized, 0, dims);
                        VectorUtil.l2normalize(normalized);
                        vector = normalized;
                    }
                    var queryCorrections = quantizer.scalarQuantize(vector, residualScratch, queryCodes, QUERY_BITS, centroid);
                    packNibbles(queryCodes, packedQueryCode);
                    query.add(
                        packedQueryCode,
                        queryCorrections.lowerInterval(),
                        queryCorrections.upperInterval(),
                        queryCorrections.additionalCorrection(),
                        queryCorrections.quantizedComponentSum(),
                        (1 << QUERY_BITS) - 1
                    );

                    // only read for CosineExpanded, which isn't used, see metric()
                    rowNormsWriter.putFloat(0f);
                }
                doc.finish();
                query.finish();
                rowNormsWriter.finish();
            }
        }

        /** Uploads the staged vectors, mapping the file once, then unmaps and deletes it */
        BbqGpuDataset upload(CuVSResources resources) throws IOException {
            List<CuVSMatrix> deviceMatrices = new ArrayList<>();
            boolean success = false;
            try (var mapped = MemorySegmentUtils.createFileBackedMemorySegment(path(), fileLength); Arena arena = Arena.ofConfined()) {
                MemorySegment file = mapped.memorySegment();
                CuVSMatrix deviceRowNorms = upload(resources, deviceMatrices, file, rowNorms, 1, CuVSMatrix.DataType.FLOAT);
                MemorySegment centroidSegment = arena.allocate(ValueLayout.JAVA_FLOAT, dims);
                MemorySegment.copy(centroid, 0, centroidSegment, ValueLayout.JAVA_FLOAT, 0, dims);
                CuVSMatrix deviceCentroid = toDevice(resources, deviceMatrices, centroidSegment, 1, dims, CuVSMatrix.DataType.FLOAT);
                CagraIndexParams.CuvsDistanceType metric = metric(similarityFunction);
                BbqQuantizer[] quantizers = new BbqQuantizer[] {
                    upload(
                        resources,
                        deviceMatrices,
                        file,
                        docSections,
                        BbqQuantizer.CodeLayout.PACKED_1B,
                        metric,
                        deviceCentroid,
                        deviceRowNorms
                    ),
                    upload(
                        resources,
                        deviceMatrices,
                        file,
                        querySections,
                        BbqQuantizer.CodeLayout.PACKED_4B,
                        metric,
                        deviceCentroid,
                        deviceRowNorms
                    ) };
                success = true;
                return new BbqGpuDataset(deviceMatrices, quantizers);
            } finally {
                if (success == false) {
                    deviceMatrices.forEach(CuVSMatrix::close);
                }
            }
        }

        private BbqQuantizer upload(
            CuVSResources resources,
            List<CuVSMatrix> deviceMatrices,
            MemorySegment file,
            QuantizerSections sections,
            BbqQuantizer.CodeLayout layout,
            CagraIndexParams.CuvsDistanceType metric,
            CuVSMatrix centroid,
            CuVSMatrix rowNorms
        ) {
            return new BbqQuantizer.Builder().withCodes(
                upload(resources, deviceMatrices, file, sections.codes(), sections.codeBytes(), CuVSMatrix.DataType.BYTE)
            )
                .withLowerIntervals(upload(resources, deviceMatrices, file, sections.lowerIntervals(), 1, CuVSMatrix.DataType.FLOAT))
                .withUpperIntervals(upload(resources, deviceMatrices, file, sections.upperIntervals(), 1, CuVSMatrix.DataType.FLOAT))
                .withAdditionalCorrections(
                    upload(resources, deviceMatrices, file, sections.additionalCorrections(), 1, CuVSMatrix.DataType.FLOAT)
                )
                .withQuantizedComponentSums(upload(resources, deviceMatrices, file, sections.componentSums(), 1, CuVSMatrix.DataType.INT))
                .withDequantDelta(upload(resources, deviceMatrices, file, sections.deltas(), 1, CuVSMatrix.DataType.FLOAT))
                .withDequantSumDelta(upload(resources, deviceMatrices, file, sections.deltaSums(), 1, CuVSMatrix.DataType.FLOAT))
                .withCentroid(centroid)
                .withRowNorm(rowNorms)
                .withLayout(layout)
                .withMetric(metric)
                .withCentroidNormSq(centroidDP)
                .build();
        }

        /** Uploads a section of the mapped file as a {@code numVectors x columns} matrix */
        private CuVSMatrix upload(
            CuVSResources resources,
            List<CuVSMatrix> deviceMatrices,
            MemorySegment file,
            Section section,
            int columns,
            CuVSMatrix.DataType dataType
        ) {
            assert section.length() == (long) numVectors * columns * dataType.bytes();
            MemorySegment slice = file.asSlice(section.offset(), section.length());
            return toDevice(resources, deviceMatrices, slice, numVectors, columns, dataType);
        }

        Path path() {
            return directory.getDirectory().resolve(tempFile);
        }

        long fileLength() {
            return fileLength;
        }

        QuantizerSections docSections() {
            return docSections;
        }

        QuantizerSections querySections() {
            return querySections;
        }

        Section rowNorms() {
            return rowNorms;
        }

        /** Deletes the temporary file, if it's left */
        @Override
        public void close() {
            if (tempFile != null) {
                IOUtils.deleteFilesIgnoringExceptions(path());
            }
        }
    }

    /**
     * Writes one section of the staged file, buffering values, and writing the buffer at the section's next position when
     * full. Values are written in the native order, the one cuVS reads them in.
     */
    private static final class SectionWriter {
        private final FileChannel channel;
        private final Section section;
        private final ByteBuffer buffer;
        private long position;

        SectionWriter(FileChannel channel, Section section, int bufferBytes) {
            this.channel = channel;
            this.section = section;
            this.buffer = ByteBuffer.allocate((int) Math.min(bufferBytes, section.length())).order(ByteOrder.nativeOrder());
            this.position = section.offset();
        }

        void put(byte[] bytes, int length) throws IOException {
            if (buffer.remaining() < length) {
                flush();
            }
            buffer.put(bytes, 0, length);
        }

        void putFloat(float value) throws IOException {
            if (buffer.remaining() < Float.BYTES) {
                flush();
            }
            buffer.putFloat(value);
        }

        void putInt(int value) throws IOException {
            if (buffer.remaining() < Integer.BYTES) {
                flush();
            }
            buffer.putInt(value);
        }

        private void flush() throws IOException {
            buffer.flip();
            if (position + buffer.remaining() > section.end()) {
                // would overwrite the next section
                throw new IllegalStateException("writing past the end of section " + section);
            }
            while (buffer.hasRemaining()) {
                position += channel.write(buffer, position);
            }
            buffer.clear();
        }

        /** Writes what's left in the buffer, and checks the whole section was written */
        void finish() throws IOException {
            flush();
            if (position != section.end()) {
                throw new IllegalStateException("wrote [" + (position - section.offset()) + "] bytes of section " + section);
            }
        }
    }

    /** Writes one quantizer's code and per-vector values, each to its section */
    private static final class QuantizerWriter {
        private final int codeBytes;
        private final SectionWriter codes;
        private final SectionWriter lowerIntervals;
        private final SectionWriter upperIntervals;
        private final SectionWriter additionalCorrections;
        private final SectionWriter componentSums;
        private final SectionWriter deltas;
        private final SectionWriter deltaSums;

        QuantizerWriter(FileChannel channel, QuantizerSections sections, int bufferBytes) {
            assert sections.codeBytes() <= bufferBytes;
            this.codeBytes = sections.codeBytes();
            this.codes = new SectionWriter(channel, sections.codes(), bufferBytes);
            this.lowerIntervals = new SectionWriter(channel, sections.lowerIntervals(), bufferBytes);
            this.upperIntervals = new SectionWriter(channel, sections.upperIntervals(), bufferBytes);
            this.additionalCorrections = new SectionWriter(channel, sections.additionalCorrections(), bufferBytes);
            this.componentSums = new SectionWriter(channel, sections.componentSums(), bufferBytes);
            this.deltas = new SectionWriter(channel, sections.deltas(), bufferBytes);
            this.deltaSums = new SectionWriter(channel, sections.deltaSums(), bufferBytes);
        }

        /**
         * @param code the code, of which only the first {@code codeBytes} bytes are written
         * @param maxCode the largest code value, {@code 2^bits - 1}, by which the interval is divided into steps
         */
        void add(byte[] code, float lower, float upper, float additionalCorrection, int componentSum, int maxCode) throws IOException {
            codes.put(code, codeBytes);
            float delta = dequantDelta(lower, upper, maxCode);
            lowerIntervals.putFloat(lower);
            upperIntervals.putFloat(upper);
            additionalCorrections.putFloat(additionalCorrection);
            componentSums.putInt(componentSum);
            deltas.putFloat(delta);
            deltaSums.putFloat(delta * componentSum);
        }

        void finish() throws IOException {
            codes.finish();
            lowerIntervals.finish();
            upperIntervals.finish();
            additionalCorrections.finish();
            componentSums.finish();
            deltas.finish();
            deltaSums.finish();
        }
    }
}
