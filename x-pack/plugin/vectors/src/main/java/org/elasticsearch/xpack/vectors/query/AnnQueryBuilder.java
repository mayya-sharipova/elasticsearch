/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.vectors.query;


import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.xpack.vectors.mapper.DenseVectorFieldMapper;
import org.elasticsearch.xpack.vectors.mapper.DenseVectorFieldMapper.DenseVectorFieldType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A query that finds approximate nearest neighbours based on kmeans clustering
 */
public class AnnQueryBuilder extends AbstractQueryBuilder<AnnQueryBuilder> {

    public static final String NAME = "ann";
    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField NUMBER_OF_PROBES_FIELD = new ParseField("number_of_probes");
    public static final ParseField QUERY_VECTOR_FIELD = new ParseField("query_vector");


    private static ConstructingObjectParser<AnnQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(NAME, false,
        args -> {
            @SuppressWarnings("unchecked") List<Float> qvList = (List<Float>) args[2];
            float[] qv = new float[qvList.size()];
            int i = 0;
            for (Float f : qvList) {
                qv[i++] = f;
            };
            AnnQueryBuilder AnnQueryBuilder = new AnnQueryBuilder((String) args[0], (int) args[1], qv);
            return AnnQueryBuilder;
        });

    static {
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        PARSER.declareInt(constructorArg(), NUMBER_OF_PROBES_FIELD);
        PARSER.declareFloatArray(constructorArg(), QUERY_VECTOR_FIELD);
    }

    public static AnnQueryBuilder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String field;
    private final int numberOfProbes;
    private final float[] queryVector;

    public AnnQueryBuilder(String field, int numberOfProbes, float[] queryVector) {
        if (numberOfProbes < 1) {
            throw new IllegalArgumentException("[number_of_probes] should be greater than 0]");
        }
        this.field = field;
        this.numberOfProbes = numberOfProbes;
        this.queryVector = queryVector;
    }

    public AnnQueryBuilder(StreamInput in) throws IOException {
        super(in);
        field = in.readString();
        numberOfProbes = in.readInt();
        queryVector = in.readFloatArray();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeInt(numberOfProbes);
        out.writeFloatArray(queryVector);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(FIELD_FIELD.getPreferredName(), field);
        builder.field(NUMBER_OF_PROBES_FIELD.getPreferredName(), numberOfProbes);
        builder.field(QUERY_VECTOR_FIELD.getPreferredName(), queryVector);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected boolean doEquals(AnnQueryBuilder other) {
        return this.field.equals(other.field) &&
            this.numberOfProbes == other.numberOfProbes &&
            Arrays.equals(this.queryVector, other.queryVector);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(this.field, this.numberOfProbes, this.queryVector);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        MappedFieldType fieldType = context.getMapperService().fullName(field);
        if ((fieldType instanceof DenseVectorFieldType) == false ){
            throw new IllegalArgumentException("Field [" + field +
                "] is not of the expected type of [" + DenseVectorFieldMapper.CONTENT_TYPE + "]");
        }
        // compute numberOfProbes of centroids that are closest to the query
        // we use euclidean distance for this
        // sqrt((d1-q1)^2 + (d2-q2)^2 ...) can be converted to sqrt(||d||^2 + ||q||^2 - 2dq)
        // where ||d||^2 -- squared magnitude of centroid
        // since we are interested only in ranking and we don't need precise scores
        // sqrt(||d||^2 + ||q||^2 - 2dq) can be converted to ||d||^2 - 2dq

        float[][] centroids = ((DenseVectorFieldType)fieldType).getCentroids();
        float[] centroidsSquaredMagnitudes = ((DenseVectorFieldType)fieldType).getCentroidsSquaredMagnitudes();
        int dims = queryVector.length;

        float[] topMinDistances = new float[numberOfProbes];
        short[] topMinIndexes = new short[numberOfProbes];
        Arrays.fill(topMinDistances, Float.MAX_VALUE);

        for (short i = 0; i < centroids.length; i++){
            float dotProduct = 0;
            for (int dim = 0; dim < dims; dim++) {
                dotProduct += queryVector[dim] * centroids[i][dim];
            }
            float result = centroidsSquaredMagnitudes[i] - 2 * dotProduct;
            updateTop(topMinDistances, topMinIndexes, result, i);
        }

        // get codes for these centroids
        BytesRef[] centroidCodes = new BytesRef[numberOfProbes];
        for (int i = 0; i < numberOfProbes; i++) {
            byte[] centroidCode = new byte[2];
            centroidCode[0] = (byte) (topMinIndexes[i] >> 8);
            centroidCode[1] = (byte) topMinIndexes[i];
            centroidCodes[i] = new BytesRef(centroidCode);
        }

        String centroidFieldName = fieldType.name() + ".centroid";
        BoolQueryBuilder centroidShouldQuery = new BoolQueryBuilder();
        for (BytesRef centroidCode : centroidCodes) {
            BoolQueryBuilder filterCentroid = new BoolQueryBuilder();
            filterCentroid.filter(new TermQueryBuilder(centroidFieldName, centroidCode));
            centroidShouldQuery.should(filterCentroid);
        }
        centroidShouldQuery.minimumShouldMatch(1);
        return centroidShouldQuery.toQuery(context);
    }

    private static void updateTop(float[] minDistances, short[] minIndexes, float newDistance, short newIndex) {
        int n = minDistances.length;
        if (newDistance >= minDistances[n-1]){
            return;
        } else {
            minDistances[n-1] = newDistance;
            minIndexes[n-1] = newIndex;
        }
        // pun newDistance and newIndex in the right place
        for (int j = n - 2; j >= 0 ; j--) {
            if (minDistances[j] > minDistances[j + 1]) {
                float tmp = minDistances[j];
                minDistances[j] = minDistances[j+1];
                minDistances[j+1] = tmp;
                short tmpIndex = minIndexes[j];
                minIndexes[j] = minIndexes[j+1];
                minIndexes[j+1] = tmpIndex;
            } else {
                break;
            }
        }
    }
}
