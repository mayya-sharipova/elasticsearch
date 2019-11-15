/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FilterScorer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.vectors.mapper.DenseVectorFieldMapper.DenseVectorFieldType;
import static org.elasticsearch.xpack.vectors.mapper.DenseVectorFieldMapper.M;
import static org.elasticsearch.xpack.vectors.mapper.DenseVectorFieldMapper.PRODUCT_CENTROIDS_COUNT;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;


public class AnnPQQuery extends Query {
    final DenseVectorFieldType fieldType;
    final Query coarseQuery;
    final float[] rQueryVector;
    final float [][] cqDistances; // squared distances between rQueryVector and all product centroids

    /**
     *
     * @param fieldType - DenseVectorFieldType
     * @param rQueryVector - residual query vector
     * @param coarseQuery - a query for coarse quantizer
     */
    public AnnPQQuery(DenseVectorFieldType fieldType, float[] rQueryVector, Query coarseQuery) {
        this.fieldType = fieldType;
        this.rQueryVector = rQueryVector;
        this.coarseQuery = coarseQuery;
        final int dims = fieldType.dims();
        final int pdims = dims/M; // number of dimensions in each product centroid
        
        // precalculate squared distances between rQueryVector and all product centroids
        // we use euclidean distance for this
        // sqrt((c1-rq1)^2 + (c2-rq2)^2 ...) can be converted to sqrt(||c||^2 + ||rq||^2 - 2*c*rq)
        // where ||c||^2 -- squared magnitude of product centroid
        // where ||rq||^2 -- squared magnitude of residual query vector
        float[] rSquaredMagnitudes = new float[M];
        for (int m = 0; m < M; m++) {
            for (int dim = m * pdims; dim < (m * pdims + pdims); dim++) {
                rSquaredMagnitudes[m] += rQueryVector[dim] * rQueryVector[dim];
            }
        }

        float[][][] productCentroids = fieldType.getProductCentroids();
        float[][] pcSquaredMagnitudes = fieldType.getProductCentroidsSquaredMagnitudes();
        cqDistances = new float[M][PRODUCT_CENTROIDS_COUNT];
        for (int m = 0; m < M; m++) {
            for (int c = 0; c < PRODUCT_CENTROIDS_COUNT; c++) {
                float dotProduct = 0;
                for (int dim = 0; dim < pdims; dim++) {
                    dotProduct += rQueryVector[m * pdims + dim] * productCentroids[m][c][dim];
                }
                cqDistances[m][c] = pcSquaredMagnitudes[m][c] + rSquaredMagnitudes[m] - 2 * dotProduct;
            }
        }
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        Weight coarseQueryWeight = coarseQuery.createWeight(searcher, org.apache.lucene.search.ScoreMode.COMPLETE_NO_SCORES, boost);

        return new Weight(this) {
            @Override
            public void extractTerms(Set<Term> terms) {
                coarseQueryWeight.extractTerms(terms);
            }

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                final BinaryDocValues values = context.reader().getBinaryDocValues(fieldType.getPCFieldName());
                Scorer cqScorer = coarseQueryWeight.scorer(context);
                if (cqScorer != null) {
                    return new ANNPQScorer(this, cqScorer, values, cqDistances);
                } else {
                    return null;
                }
            }

            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                return null;
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return true;
            }
        };
    }

    static class ANNPQScorer extends FilterScorer {
        private final BinaryDocValues values;
        private final float[][] cqDistances;

        private ANNPQScorer(Weight w, Scorer scorer, BinaryDocValues values, float [][] cqDistances) {
            super(scorer, w);
            this.values = values;
            this.cqDistances = cqDistances;
        }

        @Override
        public float score() throws IOException {
            values.advanceExact(docID());
            BytesRef valueBR = values.binaryValue();
            int offset = valueBR.offset;
            double score = 0;
            for (int i = 0; i < M; i++) {
                int docCentroid = valueBR.bytes[offset++] & 0xFF; // centroids are stored as unsigned bytes
                score += cqDistances[i][docCentroid];
            }
            score = 1/score; // as we want to get closest vectors fist, score is inversely proportional to distance
            return (float) score;
        };

        @Override
        public float getMaxScore(int upTo) {
            return Float.MAX_VALUE;
        }
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        AnnPQQuery other = (AnnPQQuery) obj;
        return Objects.equals(fieldType, other.fieldType)
            && Arrays.equals(rQueryVector, other.rQueryVector)
            && Objects.equals(coarseQuery, other.coarseQuery);
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("annpq  (field:").append(fieldType.name()).append(", coarse query:");
        sb.append(coarseQuery.toString()).append(")");
        return sb.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldType.hashCode(), coarseQuery.hashCode(), Arrays.hashCode(rQueryVector));
    }
}
