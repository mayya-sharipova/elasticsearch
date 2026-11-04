/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gpu.codec;

import com.nvidia.cuvs.CagraIndexParams;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.QuantizedVectorsReader;
import org.elasticsearch.index.codec.vectors.BQVectorUtils;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.es93.ES93BinaryQuantizedVectorsFormat;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Checks, without a GPU, that the data {@link BbqGpuDataset} sends to cuVS means what cuVS expects: the bit order of the
 * codes, and the corrections, through a port of cuVS's BBQ distance ({@code bbq_calculate_metric} in
 * {@code cpp/src/preprocessing/quantize/detail/bbq_distance.cuh}).
 */
// Production grants cuvs-java load_native_libraries through the gpu plugin's policy, which isn't visible from this library's tests.
// Enforcing here would fail cuvs initialization, which is cached for the JVM and silently skips every GPU test that shares it.
@ESTestCase.WithoutEntitlements
public class BbqGpuDatasetTests extends ESTestCase {

    public void testIsSupported() {
        for (int dims : new int[] { 32, 64, 96, 384, 768, 1024, 1536, 3072 }) {
            assertTrue("dims " + dims, BbqGpuDataset.isSupported(dims));
        }
        for (int dims : new int[] { 0, 1, 8, 16, 63, 100, 1000 }) {
            assertFalse("dims " + dims, BbqGpuDataset.isSupported(dims));
        }
    }

    public void testMetric() {
        assertThat(BbqGpuDataset.metric(VectorSimilarityFunction.EUCLIDEAN), equalTo(CagraIndexParams.CuvsDistanceType.L2Expanded));
        for (var similarity : new VectorSimilarityFunction[] {
            VectorSimilarityFunction.COSINE,
            VectorSimilarityFunction.DOT_PRODUCT,
            VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT }) {
            assertThat(BbqGpuDataset.metric(similarity), equalTo(CagraIndexParams.CuvsDistanceType.InnerProduct));
        }
    }

    public void testBytesPerVector() {
        // 1-bit and 4-bit codes, then 6 values per quantizer and the row norm
        assertThat(BbqGpuDataset.bytesPerVector(1024), equalTo(128 + 512 + 13 * Float.BYTES));
    }

    /** cuVS's packed_4b: dimension 2i in the high nibble of byte i, dimension 2i + 1 in the low nibble */
    public void testPackNibbles() {
        int dims = randomIntBetween(1, 64) * 32;
        int[] codes = new int[dims];
        for (int i = 0; i < dims; i++) {
            codes[i] = randomIntBetween(0, 15);
        }
        byte[] packed = new byte[dims / 2];
        BbqGpuDataset.packNibbles(codes, packed);
        for (int d = 0; d < dims; d++) {
            int value = Byte.toUnsignedInt(packed[d / 2]);
            int code = d % 2 == 0 ? value >>> 4 : value & 0x0F;
            assertThat("dimension " + d, code, equalTo(codes[d]));
        }
    }

    /**
     * cuVS's packed_1b reads dimension d from bit {@code 7 - d % 8} of byte {@code d / 8}, as Lucene's packAsBinary writes it.
     * Reads the 1-bit vectors the bbq flat format writes, the way the GPU merge reads them, and checks they are in that bit
     * order, and with the corrections of the raw vectors read alongside, quantized against the centroid. The flat format
     * pads each code to a multiple of 64 dimensions, of which {@link BbqGpuDataset} copies the first {@code dims / 8} bytes.
     */
    public void testOneBitCodesFromFlatFormat() throws IOException {
        int dims = randomIntBetween(1, 8) * 32;
        int numVectors = randomIntBetween(10, 200);
        VectorSimilarityFunction similarity = randomFrom(VectorSimilarityFunction.values());
        boolean unit = similarity == VectorSimilarityFunction.COSINE || similarity == VectorSimilarityFunction.DOT_PRODUCT;
        float[][] vectors = clusteredVectors(numVectors, dims, unit);
        try (Directory dir = newDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig().setCodec(
                TestUtil.alwaysKnnVectorsFormat(new ES93BinaryQuantizedVectorsFormat())
            );
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                for (float[] vector : vectors) {
                    Document doc = new Document();
                    doc.add(new KnnFloatVectorField("v", vector, similarity));
                    writer.addDocument(doc);
                    if (rarely()) {
                        writer.flush();
                    }
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                KnnVectorsReader vectorsReader = ((CodecReader) getOnlyLeafReader(reader)).getVectorReader();
                if (vectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader fieldsReader) {
                    vectorsReader = fieldsReader.getFieldReader("v");
                }
                // the flat format itself, so the reader is the one the GPU writer reads the merged vectors from
                var flatReader = vectorsReader;
                var binaryVectors = (QuantizedByteVectorValues) ((QuantizedVectorsReader) flatReader).getQuantizedVectorValues("v");
                FloatVectorValues rawVectors = flatReader.getFloatVectorValues("v");
                assertThat(binaryVectors.getScalarEncoding(), equalTo(QuantizedByteVectorValues.ScalarEncoding.SINGLE_BIT_QUERY_NIBBLE));
                assertThat(binaryVectors.size(), equalTo(numVectors));
                assertThat(rawVectors.size(), equalTo(numVectors));

                float[] centroid = binaryVectors.getCentroid();
                assertThat((double) binaryVectors.getCentroidDP(), closeTo(VectorUtil.dotProduct(centroid, centroid), 1e-4));
                OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(similarity);
                int[] codes = new int[dims];
                for (int ord = 0; ord < numVectors; ord++) {
                    float[] vector = rawVectors.vectorValue(ord).clone();
                    if (similarity == VectorSimilarityFunction.COSINE) {
                        VectorUtil.l2normalize(vector);
                    }
                    var expected = quantizer.scalarQuantize(vector, new float[dims], codes, (byte) 1, centroid);
                    byte[] packed = binaryVectors.vectorValue(ord);
                    assertThat(packed.length, equalTo(BQVectorUtils.discretize(dims, 64) / 8));
                    for (int d = 0; d < dims; d++) {
                        int bit = (packed[d / 8] >>> (7 - d % 8)) & 1;
                        assertThat("ord " + ord + ", dimension " + d, bit, equalTo(codes[d]));
                    }
                    // the flat format may round differently in the last bit, e.g. normalizing for cosine
                    var corrections = binaryVectors.getCorrectiveTerms(ord);
                    assertThat(
                        (double) corrections.lowerInterval(),
                        closeTo(expected.lowerInterval(), 1e-5 * Math.max(1, Math.abs(expected.lowerInterval())))
                    );
                    assertThat(
                        (double) corrections.upperInterval(),
                        closeTo(expected.upperInterval(), 1e-5 * Math.max(1, Math.abs(expected.upperInterval())))
                    );
                    assertThat(
                        (double) corrections.additionalCorrection(),
                        closeTo(expected.additionalCorrection(), 1e-5 * Math.max(1, Math.abs(expected.additionalCorrection())))
                    );
                    assertThat(corrections.quantizedComponentSum(), equalTo(expected.quantizedComponentSum()));
                }
            }
        }
    }

    /**
     * Stages the vectors of a merged bbq segment in a temporary file, as the GPU merge does before uploading them, and
     * reads the file back: each section must hold one contiguous array of the layout cuVS takes, the 1-bit side copied
     * from the flat format without its padding, and the 4-bit side quantized from the raw vectors and packed as nibbles.
     * Write buffers are small and random, so that sections are written in several chunks. Closing the staged vectors must
     * delete the file.
     */
    public void testStagedVectors() throws IOException {
        int dims = randomIntBetween(1, 8) * 32;
        int numVectors = randomIntBetween(10, 200);
        VectorSimilarityFunction similarity = randomFrom(VectorSimilarityFunction.values());
        boolean unit = similarity == VectorSimilarityFunction.COSINE || similarity == VectorSimilarityFunction.DOT_PRODUCT;
        float[][] vectors = clusteredVectors(numVectors, dims, unit);
        try (Directory dir = newFSDirectory(createTempDir())) {
            IndexWriterConfig config = new IndexWriterConfig().setCodec(
                TestUtil.alwaysKnnVectorsFormat(new ES93BinaryQuantizedVectorsFormat())
            );
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                for (float[] vector : vectors) {
                    Document doc = new Document();
                    doc.add(new KnnFloatVectorField("v", vector, similarity));
                    writer.addDocument(doc);
                    if (rarely()) {
                        writer.flush();
                    }
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                KnnVectorsReader vectorsReader = ((CodecReader) getOnlyLeafReader(reader)).getVectorReader();
                if (vectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader fieldsReader) {
                    vectorsReader = fieldsReader.getFieldReader("v");
                }
                var binaryVectors = (QuantizedByteVectorValues) ((QuantizedVectorsReader) vectorsReader).getQuantizedVectorValues("v");
                FloatVectorValues rawVectors = vectorsReader.getFloatVectorValues("v");
                float[] centroid = binaryVectors.getCentroid();

                int bufferBytes = randomIntBetween(dims / 2, 4 * dims);
                String[] filesBefore = dir.listAll();
                Path stagedFile;
                try (
                    var staged = BbqGpuDataset.StagedVectors.write(
                        dir,
                        "_staged",
                        IOContext.DEFAULT,
                        binaryVectors,
                        rawVectors,
                        similarity,
                        bufferBytes
                    )
                ) {
                    stagedFile = staged.path();
                    // one file, of 7 arrays per quantizer and the row norms
                    assertThat(dir.listAll().length, equalTo(filesBefore.length + 1));
                    byte[] bytes = Files.readAllBytes(stagedFile);
                    assertThat((long) bytes.length, equalTo(staged.fileLength()));
                    assertThat(staged.fileLength(), equalTo((long) numVectors * BbqGpuDataset.bytesPerVector(dims)));
                    var doc = new StagedArrays(bytes, staged.docSections(), numVectors);
                    var query = new StagedArrays(bytes, staged.querySections(), numVectors);
                    ByteBuffer rowNorms = section(bytes, staged.rowNorms(), (long) numVectors * Float.BYTES);

                    OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(similarity);
                    int[] queryCodes = new int[dims];
                    byte[] packed = new byte[dims / 2];
                    for (int ord = 0; ord < numVectors; ord++) {
                        byte[] docCode = binaryVectors.vectorValue(ord);
                        var docCorrections = binaryVectors.getCorrectiveTerms(ord);
                        doc.assertVector(
                            ord,
                            Arrays.copyOf(docCode, dims / 8),
                            docCorrections.lowerInterval(),
                            docCorrections.upperInterval(),
                            docCorrections.additionalCorrection(),
                            docCorrections.quantizedComponentSum(),
                            1
                        );

                        float[] vector = rawVectors.vectorValue(ord).clone();
                        if (similarity == VectorSimilarityFunction.COSINE) {
                            VectorUtil.l2normalize(vector);
                        }
                        var queryCorrections = quantizer.scalarQuantize(vector, new float[dims], queryCodes, (byte) 4, centroid);
                        BbqGpuDataset.packNibbles(queryCodes, packed);
                        query.assertVector(
                            ord,
                            packed,
                            queryCorrections.lowerInterval(),
                            queryCorrections.upperInterval(),
                            queryCorrections.additionalCorrection(),
                            queryCorrections.quantizedComponentSum(),
                            15
                        );

                        assertThat(rowNorms.getFloat(), equalTo(0f));
                    }
                }
                assertFalse(Files.exists(stagedFile));
                assertArrayEquals(filesBefore, dir.listAll());
            }
        }
    }

    /** One quantizer's arrays, read back from the sections of the file {@link BbqGpuDataset.StagedVectors} wrote */
    private static final class StagedArrays {
        private final int codeBytes;
        private final ByteBuffer codes;
        private final ByteBuffer lowerIntervals;
        private final ByteBuffer upperIntervals;
        private final ByteBuffer additionalCorrections;
        private final ByteBuffer componentSums;
        private final ByteBuffer deltas;
        private final ByteBuffer deltaSums;

        StagedArrays(byte[] file, BbqGpuDataset.QuantizerSections sections, int numVectors) {
            this.codeBytes = sections.codeBytes();
            long perVectorBytes = (long) numVectors * Float.BYTES;
            codes = section(file, sections.codes(), (long) numVectors * codeBytes);
            lowerIntervals = section(file, sections.lowerIntervals(), perVectorBytes);
            upperIntervals = section(file, sections.upperIntervals(), perVectorBytes);
            additionalCorrections = section(file, sections.additionalCorrections(), perVectorBytes);
            componentSums = section(file, sections.componentSums(), perVectorBytes);
            deltas = section(file, sections.deltas(), perVectorBytes);
            deltaSums = section(file, sections.deltaSums(), perVectorBytes);
        }

        /** Checks the next vector's values, which must be the given ones, exactly */
        void assertVector(int ord, byte[] code, float lower, float upper, float additional, int sum, int maxCode) {
            byte[] stagedCode = new byte[codeBytes];
            codes.get(stagedCode);
            assertArrayEquals("ord " + ord, code, stagedCode);
            assertThat(lowerIntervals.getFloat(), equalTo(lower));
            assertThat(upperIntervals.getFloat(), equalTo(upper));
            assertThat(additionalCorrections.getFloat(), equalTo(additional));
            assertThat(componentSums.getInt(), equalTo(sum));
            float delta = BbqGpuDataset.dequantDelta(lower, upper, maxCode);
            assertThat(deltas.getFloat(), equalTo(delta));
            assertThat(deltaSums.getFloat(), equalTo(delta * sum));
        }
    }

    /** A section of the staged file, which must be {@code expectedBytes} long, in the native order cuVS reads it in */
    private static ByteBuffer section(byte[] file, BbqGpuDataset.Section section, long expectedBytes) {
        assertThat(section.length(), equalTo(expectedBytes));
        return ByteBuffer.wrap(file, Math.toIntExact(section.offset()), Math.toIntExact(section.length()))
            .slice()
            .order(ByteOrder.nativeOrder());
    }

    /**
     * For each similarity, quantizes clustered vectors as the merge does, the documents to 1 bit and the queries to 4 bits
     * against the same centroid, and scores them with the port of cuVS's distance: the centered dot product must match the
     * dequantized vectors', and ranking by the estimated distance must find most of the true nearest neighbors. A wrong
     * correction, such as an inner product scored with euclidean corrections, ranks no better than at random.
     */
    public void testAsymmetricDistance() {
        int dims = randomFrom(64, 96, 128);
        int numVectors = 1000;
        VectorSimilarityFunction similarity = randomFrom(VectorSimilarityFunction.values());
        boolean unit = similarity == VectorSimilarityFunction.COSINE || similarity == VectorSimilarityFunction.DOT_PRODUCT;
        float[][] vectors = clusteredVectors(numVectors, dims, unit);

        float[] centroid = new float[dims];
        for (float[] v : vectors) {
            for (int d = 0; d < dims; d++) {
                centroid[d] += v[d] / numVectors;
            }
        }
        if (similarity == VectorSimilarityFunction.COSINE) {
            VectorUtil.l2normalize(centroid);
        }
        float centroidDP = VectorUtil.dotProduct(centroid, centroid);

        OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(similarity);
        Quantized[] docs = new Quantized[numVectors];
        Quantized[] queries = new Quantized[numVectors];
        for (int i = 0; i < numVectors; i++) {
            docs[i] = quantize(quantizer, vectors[i], centroid, (byte) 1);
            queries[i] = quantize(quantizer, vectors[i], centroid, (byte) 4);
        }

        CagraIndexParams.CuvsDistanceType metric = BbqGpuDataset.metric(similarity);
        int numQueries = 20;
        double recallSum = 0;
        for (int q = 0; q < numQueries; q++) {
            int query = randomIntBetween(0, numVectors - 1);
            for (int doc = 0; doc < 5; doc++) {
                // the formula must compute the dot product of the dequantized centered vectors
                double expected = 0;
                for (int d = 0; d < dims; d++) {
                    expected += (double) docs[doc].dequantized(d) * queries[query].dequantized(d);
                }
                assertThat((double) centered(docs[doc], queries[query], dims), closeTo(expected, 1e-3 * Math.max(1, Math.abs(expected))));
            }

            Set<Integer> trueTop10 = IntStream.range(0, numVectors)
                .boxed()
                .sorted(Comparator.comparingDouble(doc -> -similarity.compare(vectors[query], vectors[doc])))
                .limit(10)
                .collect(Collectors.toSet());
            Set<Integer> estimatedTop50 = IntStream.range(0, numVectors)
                .boxed()
                .sorted(Comparator.comparingDouble(doc -> distance(docs[doc], queries[query], dims, metric, centroidDP)))
                .limit(50)
                .collect(Collectors.toSet());
            estimatedTop50.retainAll(trueTop10);
            recallSum += estimatedTop50.size() / 10.0;
        }
        // at random, 50 / numVectors = 0.05
        assertThat("similarity " + similarity + ", dims " + dims, recallSum / numQueries, greaterThan(0.7));
    }

    /** One quantized vector with its corrections, staged as {@link BbqGpuDataset} stages them */
    private record Quantized(int[] codes, float lower, float delta, float sumDelta, float additionalCorrection) {
        float dequantized(int d) {
            return lower + delta * codes[d];
        }
    }

    private static Quantized quantize(OptimizedScalarQuantizer quantizer, float[] vector, float[] centroid, byte bits) {
        int[] codes = new int[vector.length];
        var result = quantizer.scalarQuantize(vector, new float[vector.length], codes, bits, centroid);
        float delta = BbqGpuDataset.dequantDelta(result.lowerInterval(), result.upperInterval(), (1 << bits) - 1);
        return new Quantized(codes, result.lowerInterval(), delta, delta * result.quantizedComponentSum(), result.additionalCorrection());
    }

    /** The {@code centered} term of cuVS's {@code bbq_calculate_metric} */
    private static float centered(Quantized doc, Quantized query, int dims) {
        long raw = 0;
        for (int d = 0; d < dims; d++) {
            raw += (long) doc.codes[d] * query.codes[d];
        }
        return dims * doc.lower * query.lower + query.lower * doc.sumDelta + doc.lower * query.sumDelta + doc.delta * query.delta * raw;
    }

    /** cuVS's {@code bbq_calculate_metric}, for the metrics {@link BbqGpuDataset#metric} uses */
    private static float distance(Quantized doc, Quantized query, int dims, CagraIndexParams.CuvsDistanceType metric, float centroidDP) {
        float centered = centered(doc, query, dims);
        float corrections = doc.additionalCorrection + query.additionalCorrection;
        return switch (metric) {
            case L2Expanded -> Math.max(0, corrections - 2 * centered);
            case InnerProduct -> -(centered + corrections - centroidDP);
            default -> throw new AssertionError("unexpected metric " + metric);
        };
    }

    private float[][] clusteredVectors(int numVectors, int dims, boolean unit) {
        float[][] centers = new float[10][dims];
        for (float[] center : centers) {
            for (int d = 0; d < dims; d++) {
                center[d] = randomFloat() * 2 - 1;
            }
        }
        float[][] vectors = new float[numVectors][dims];
        for (float[] vector : vectors) {
            float[] center = randomFrom(centers);
            for (int d = 0; d < dims; d++) {
                vector[d] = center[d] + (float) random().nextGaussian() * 0.3f;
            }
            if (unit) {
                VectorUtil.l2normalize(vector);
            }
        }
        return vectors;
    }
}
