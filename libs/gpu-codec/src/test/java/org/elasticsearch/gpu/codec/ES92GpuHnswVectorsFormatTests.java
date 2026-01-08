/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gpu.codec;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.gpu.GPUSupport;
import org.junit.BeforeClass;

import java.util.List;

// CuVS prints tons of logs to stdout
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "https://github.com/rapidsai/cuvs/issues/1310")
public class ES92GpuHnswVectorsFormatTests extends BaseKnnVectorsFormatTestCase {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    static Codec codec;

    @BeforeClass
    public static void beforeClass() {
        assumeTrue("cuvs not supported", GPUSupport.isSupported());
        codec = TestUtil.alwaysKnnVectorsFormat(new ES92GpuHnswVectorsFormat());
    }

    @Override
    protected Codec getCodec() {
        return codec;
    }

    @Override
    protected VectorSimilarityFunction randomSimilarity() {
        // COSINE is substituted with DOT_PRODUCT before reaching the GPU codec
        return RandomPicks.randomFrom(
            random(),
            List.of(
                VectorSimilarityFunction.DOT_PRODUCT,
                VectorSimilarityFunction.EUCLIDEAN,
                VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT
            )
        );
    }

    @Override
    protected VectorEncoding randomVectorEncoding() {
        return VectorEncoding.FLOAT32;
    }

    @Override
    public void testRandomBytes() throws Exception {
        // No bytes support
    }

    @Override
    public void testSortedIndexBytes() throws Exception {
        // No bytes support
    }

    @Override
    public void testByteVectorScorerIteration() throws Exception {
        // No bytes support
    }

    @Override
    public void testEmptyByteVectorData() throws Exception {
        // No bytes support
    }

    @Override
    public void testMergingWithDifferentByteKnnFields() throws Exception {
        // No bytes support
    }

    @Override
    public void testMismatchedFields() throws Exception {
        // No bytes support
    }

}
