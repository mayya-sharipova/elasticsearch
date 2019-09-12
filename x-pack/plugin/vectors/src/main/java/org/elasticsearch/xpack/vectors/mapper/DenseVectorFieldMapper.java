/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.vectors.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.ArrayValueMapperParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xpack.vectors.query.VectorDVIndexFieldData;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A {@link FieldMapper} for indexing a dense vector of floats.
 */
public class DenseVectorFieldMapper extends FieldMapper implements ArrayValueMapperParser {

    public static final String CONTENT_TYPE = "dense_vector";
    public static short MAX_DIMS_COUNT = 1024; //maximum allowed number of dimensions
    private static final byte INT_BYTES = 4;
    private static final int CENTROIDS_COUNT = 1000;

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new DenseVectorFieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setHasDocValues(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, DenseVectorFieldMapper> {
        private int dims = 0;
        private float[][] centroids = null;
        private float[] centroidsSquaredMagnitudes = null;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        public Builder dims(int dims) {
            if ((dims > MAX_DIMS_COUNT) || (dims < 1)) {
                throw new MapperParsingException("The number of dimensions for field [" + name +
                    "] should be in the range [1, " + MAX_DIMS_COUNT + "]");
            }
            this.dims = dims;
            return this;
        }

        public Builder buildCentroids() {
            if (centroids != null) return this;
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                try {
                    InputStream istream = getClass().getResourceAsStream("/centroids.txt");
                    byte[] bytes = istream.readAllBytes();
                    ByteBuffer buffer = ByteBuffer.wrap(bytes);
                    buffer.order(ByteOrder.LITTLE_ENDIAN);
                    FloatBuffer fbuffer = buffer.asFloatBuffer();
                    centroids = new float[CENTROIDS_COUNT][dims];
                    centroidsSquaredMagnitudes = new float[CENTROIDS_COUNT];
                    for (int i = 0; i < CENTROIDS_COUNT; i++) {
                        centroidsSquaredMagnitudes[i] = 0;
                        for (int dim = 0; dim < dims; dim++) {
                            centroids[i][dim] = fbuffer.get();
                            centroidsSquaredMagnitudes[i] += centroids[i][dim] * centroids[i][dim];
                        }
                    }
                    istream.close();
                } catch (IOException e) {
                    throw new MapperParsingException("Could not load centroids");
                }
                return null;
            });
            return this;
        }

        @Override
        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);
            fieldType().setDims(dims);
            fieldType().setCentroids(centroids);
            fieldType().setCentroidsSquaredMagnitudes(centroidsSquaredMagnitudes);
        }

        @Override
        public DenseVectorFieldType fieldType() {
            return (DenseVectorFieldType) super.fieldType();
        }

        @Override
        public DenseVectorFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new DenseVectorFieldMapper(
                    name, fieldType, defaultFieldType,
                    context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?,?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            DenseVectorFieldMapper.Builder builder = new DenseVectorFieldMapper.Builder(name);
            Object dimsField = node.remove("dims");
            if (dimsField == null) {
                throw new MapperParsingException("The [dims] property must be specified for field [" + name + "].");
            }
            int dims = XContentMapValues.nodeIntegerValue(dimsField);
            builder.dims(dims);
            builder.buildCentroids();
            return builder;
        }
    }

    public static final class DenseVectorFieldType extends MappedFieldType {
        private int dims;
        private float[][] centroids;
        private float[] centroidsSquaredMagnitudes;

        public DenseVectorFieldType() {}

        protected DenseVectorFieldType(DenseVectorFieldType ref) {
            super(ref);
        }

        public DenseVectorFieldType clone() {
            return new DenseVectorFieldType(this);
        }

        int dims() {
            return dims;
        }

        void setDims(int dims) {
            this.dims = dims;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            throw new UnsupportedOperationException(
                "Field [" + name() + "] of type [" + typeName() + "] doesn't support docvalue_fields or aggregations");
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new DocValuesFieldExistsQuery(name());
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            return new VectorDVIndexFieldData.Builder(true);
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new UnsupportedOperationException(
                "Field [" + name() + "] of type [" + typeName() + "] doesn't support queries");
        }

        public void setCentroids(float[][] centroids) {
            this.centroids = centroids;
        }

        public float[][] getCentroids() {
            return centroids;
        }

        public void setCentroidsSquaredMagnitudes(float[] centroidsSquaredMagnitudes) {
            this.centroidsSquaredMagnitudes = centroidsSquaredMagnitudes;
        }

        public float[] getCentroidsSquaredMagnitudes() {
            return centroidsSquaredMagnitudes;
        }
    }

    private DenseVectorFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                   Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        assert fieldType.indexOptions() == IndexOptions.NONE;
    }

    @Override
    protected DenseVectorFieldMapper clone() {
        return (DenseVectorFieldMapper) super.clone();
    }

    @Override
    public DenseVectorFieldType fieldType() {
        return (DenseVectorFieldType) super.fieldType();
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        if (context.externalValueSet()) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] can't be used in multi-fields");
        }
        int dims = fieldType().dims(); //number of vector dimensions

        // encode array of floats as array of integers and store into buf
        // this code is here and not int the VectorEncoderDecoder so not to create extra arrays
        byte[] bytes = indexCreatedVersion.onOrAfter(Version.V_7_5_0) ? new byte[dims * INT_BYTES + INT_BYTES] : new byte[dims * INT_BYTES];
        byte[] centroidCode = new byte[2]; // 2 bytes for centroid, max centroid value -- 1024
        Token token;
        while ((token = context.parser().nextToken()) != Token.END_OBJECT) {
            if (token == Token.FIELD_NAME) {
                String fieldName = context.parser().currentName();
                if (fieldName.equals("centroid")) {
                    token = context.parser().nextToken();
                    ensureExpectedToken(Token.VALUE_NUMBER, token, context.parser()::getTokenLocation);
                    short centroid = context.parser().shortValue();
                    centroidCode[0] = (byte) (centroid >> 8);
                    centroidCode[1] = (byte) centroid;
                } else if (fieldName.equals("value")) {
                    token = context.parser().nextToken();
                    ensureExpectedToken(Token.START_ARRAY, token, context.parser()::getTokenLocation);
                    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
                    double dotProduct = 0f;
                    int dim = 0;
                    for (token = context.parser().nextToken(); token != Token.END_ARRAY; token = context.parser().nextToken()) {
                        if (dim++ >= dims) {
                            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] of doc [" +
                                context.sourceToParse().id() + "] has exceeded the number of dimensions [" + dims + "] defined in mapping");
                        }
                        ensureExpectedToken(Token.VALUE_NUMBER, token, context.parser()::getTokenLocation);
                        float value = context.parser().floatValue(true);
                        byteBuffer.putFloat(value);
                        dotProduct += value * value;
                    }
                    if (dim != dims) {
                        throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] of doc [" +
                            context.sourceToParse().id() + "] has number of dimensions [" + dim +
                            "] less than defined in the mapping [" +  dims +"]");
                    }
                    if (indexCreatedVersion.onOrAfter(Version.V_7_5_0)) {
                        // encode vector magnitude at the end
                        float vectorMagnitude = (float) Math.sqrt(dotProduct);
                        byteBuffer.putFloat(vectorMagnitude);
                    }
                }
            }
        }
        BinaryDocValuesField field = new BinaryDocValuesField(fieldType().name(), new BytesRef(bytes));
        if (context.doc().getByKey(fieldType().name()) != null) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() +
                "] doesn't not support indexing multiple values for the same field in the same document");
        }
        context.doc().addWithKey(fieldType().name(), field);
        StringField centroidField = new StringField(fieldType().name() + ".centroid", new BytesRef(centroidCode), Field.Store.NO);
        context.doc().add(centroidField);
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        builder.field("dims", fieldType().dims());
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) {
        throw new AssertionError("parse is implemented directly");
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
