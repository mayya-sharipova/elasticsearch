/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gpu.codec;

import org.apache.logging.log4j.Level;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.elasticsearch.gpu.CuVSGPUSupport;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.gpu.codec.ES92GpuHnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.elasticsearch.gpu.codec.ES92GpuHnswVectorsFormat.DEFAULT_MAX_CONN;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * Merges of {@code bbq_hnsw} segments on GPU, which build the graph from the quantized vectors when the dimension is a
 * multiple of 32 (see {@link BbqGpuDataset}), and otherwise from the raw float vectors.
 */
public class ES97GpuHnswBBQQuantizedMergeTests extends ESTestCase {

    private static final String FIELD = "v";
    private static final int DOCS_PER_SEGMENT = 1000;
    private static final int NUM_SEGMENTS = 3;

    @BeforeClass
    public static void beforeClass() {
        assumeTrue("cuvs not supported", CuVSGPUSupport.instance().isSupported());
    }

    @TestLogging(value = "org.elasticsearch.gpu.codec.ES92GpuHnswVectorsWriter:DEBUG", reason = "checks which merge path builds the graph")
    public void testMerge() throws IOException {
        // 96 has a 1-bit code padded to 128 dimensions by the flat format; 100 isn't a multiple of 32, so uses the float path
        int dims = randomFrom(64, 96, 128, 384, 100);
        boolean quantized = BbqGpuDataset.isSupported(dims);
        VectorSimilarityFunction similarity = randomFrom(VectorSimilarityFunction.values());
        int numDocs = DOCS_PER_SEGMENT * NUM_SEGMENTS;
        float[][] vectors = clusteredVectors(
            numDocs,
            dims,
            similarity == VectorSimilarityFunction.COSINE || similarity == VectorSimilarityFunction.DOT_PRODUCT
        );

        // a graph threshold of 0, so that the flushed segments get a graph too
        var format = new ES97GpuHnswBBQVectorsFormat(
            CuVSResourceManager::pooling,
            CuVSGPUSupport.instance().getTotalGpuMemory(),
            DEFAULT_MAX_CONN,
            DEFAULT_BEAM_WIDTH,
            false,
            0,
            false
        );
        try (Directory dir = new MMapDirectory(createTempDir())) {
            IndexWriterConfig config = new IndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(format))
                .setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                for (int i = 0; i < numDocs; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("id", Integer.toString(i), Field.Store.YES));
                    doc.add(new KnnFloatVectorField(FIELD, vectors[i], similarity));
                    writer.addDocument(doc);
                    if ((i + 1) % DOCS_PER_SEGMENT == 0) {
                        writer.flush();
                    }
                }

                writer.getConfig().setMergePolicy(new TieredMergePolicy());
                String bbqBuild = "Building CAGRA graph from BBQ vectors: numVectors=[" + numDocs + "], dims=[" + dims + "]*";
                String component = ES92GpuHnswVectorsWriter.class.getCanonicalName();
                MockLog.assertThatLogger(() -> {
                    try {
                        writer.forceMerge(1);
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                },
                    ES92GpuHnswVectorsWriter.class,
                    quantized
                        ? new MockLog.SeenEventExpectation("bbq merge", component, Level.DEBUG, bbqBuild)
                        : new MockLog.UnseenEventExpectation("bbq merge", component, Level.DEBUG, bbqBuild),
                    new MockLog.UnseenEventExpectation("no CPU fallback", component, Level.WARN, "Failed to do merge on GPU*")
                );
                writer.commit();
            }
            // the quantized vectors were staged in temporary files, which must all be deleted once uploaded
            assertThat(Arrays.stream(dir.listAll()).filter(file -> file.contains("bbq_gpu")).toList(), empty());

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                KnnVectorsReader vectorsReader = ((CodecReader) leaf).getVectorReader();
                if (vectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader fieldsReader) {
                    vectorsReader = fieldsReader.getFieldReader(FIELD);
                }
                HnswGraph graph = ((HnswGraphProvider) vectorsReader).getGraph(FIELD);
                assertThat(graph, not(nullValue()));
                assertThat(graph.size(), equalTo(numDocs));

                // Recall of the true top 10 among the top 100 found, averaged over queries; a graph that doesn't match the
                // vectors would get about 100 / numDocs. See GpuCpuFallbackTests for the recall of CPU-built graphs.
                IndexSearcher searcher = newSearcher(reader);
                StoredFields storedFields = reader.storedFields();
                int numQueries = 20;
                double recallSum = 0;
                for (int q = 0; q < numQueries; q++) {
                    float[] query = vectors[randomIntBetween(0, numDocs - 1)];
                    Set<Integer> trueTop10 = IntStream.range(0, numDocs)
                        .boxed()
                        .sorted(Comparator.comparingDouble(doc -> -similarity.compare(query, vectors[doc])))
                        .limit(10)
                        .collect(Collectors.toSet());
                    TopDocs topDocs = searcher.search(new KnnFloatVectorQuery(FIELD, query, 100), 100);
                    int found = 0;
                    for (var scoreDoc : topDocs.scoreDocs) {
                        if (trueTop10.contains(Integer.parseInt(storedFields.document(scoreDoc.doc).get("id")))) {
                            found++;
                        }
                    }
                    recallSum += found / 10.0;
                }
                assertThat("similarity " + similarity + ", dims " + dims, recallSum / numQueries, greaterThan(0.5));
            }
        }
    }

    private float[][] clusteredVectors(int numVectors, int dims, boolean unit) {
        float[][] centers = new float[20][dims];
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
