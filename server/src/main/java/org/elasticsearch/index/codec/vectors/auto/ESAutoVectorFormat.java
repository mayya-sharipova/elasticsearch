/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.auto;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.index.codec.vectors.ES814ScalarQuantizedVectorsFormat;

import java.io.IOException;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MAX_DIMS_COUNT;

public class ESAutoVectorFormat extends KnnVectorsFormat {

    public static final String NAME = "ESAutoVectorFormat";
    public static final int VERSION_START = 0;
    public static final int VERSION_CURRENT = VERSION_START;

    static final String META_CODEC_NAME = "ES900AutoVectorsFormatMeta";
    static final String VECTOR_INDEX_CODEC_NAME = "ES900AutoVectorsFormatData";
    static final String META_EXTENSION = "vem";
    static final String VECTOR_INDEX_EXTENSION = "vex";
    static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

    private final FlatVectorsFormat flatVectorsFormat;

    public ESAutoVectorFormat() {
        super(NAME);
        this.flatVectorsFormat = new ES814ScalarQuantizedVectorsFormat(null, 7, false);
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new ESAutoVectorsWriter(state, DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH, flatVectorsFormat.fieldsWriter(state), 1, null);
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new ESAutoVectorsReader(state, flatVectorsFormat.fieldsReader(state));
    }

    @Override
    public int getMaxDimensions(String fieldName) {
        return MAX_DIMS_COUNT;
    }
}
