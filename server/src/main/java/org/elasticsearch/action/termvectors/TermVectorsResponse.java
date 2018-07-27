/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.termvectors;

import org.apache.lucene.index.Fields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BoostAttribute;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.termvectors.TermVectorsRequest.Flag;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class TermVectorsResponse extends ActionResponse implements ToXContentObject {

    private static class FieldStrings {
        // term statistics strings
        public static final String TTF = "ttf";
        public static final String DOC_FREQ = "doc_freq";
        public static final String TERM_FREQ = "term_freq";
        public static final String SCORE = "score";

        // field statistics strings
        public static final String FIELD_STATISTICS = "field_statistics";
        public static final String DOC_COUNT = "doc_count";
        public static final String SUM_DOC_FREQ = "sum_doc_freq";
        public static final String SUM_TTF = "sum_ttf";

        public static final String TOKENS = "tokens";
        public static final String POS = "position";
        public static final String START_OFFSET = "start_offset";
        public static final String END_OFFSET = "end_offset";
        public static final String PAYLOAD = "payload";
        public static final String _INDEX = "_index";
        public static final String _TYPE = "_type";
        public static final String _ID = "_id";
        public static final String _VERSION = "_version";
        public static final String FOUND = "found";
        public static final String TOOK = "took";
        public static final String TERMS = "terms";
        public static final String TERM_VECTORS = "term_vectors";
    }

    private BytesReference termVectors;
    private BytesReference headerRef;
    private String index;
    private String type;
    private String id;
    private long docVersion;
    private boolean exists = false;
    private boolean artificial = false;
    private long tookInMillis;
    private boolean hasScores = false;

    private boolean sourceCopied = false;

    int[] currentPositions = new int[0];
    int[] currentStartOffset = new int[0];
    int[] currentEndOffset = new int[0];
    BytesReference[] currentPayloads = new BytesReference[0];

    public TermVectorsResponse(String index, String type, String id) {
        this.index = index;
        this.type = type;
        this.id = id;
    }

    TermVectorsResponse() {
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeString(type);
        out.writeString(id);
        out.writeVLong(docVersion);
        final boolean docExists = isExists();
        out.writeBoolean(docExists);
        out.writeBoolean(artificial);
        out.writeVLong(tookInMillis);
        out.writeBoolean(hasTermVectors());
        if (hasTermVectors()) {
            out.writeBytesReference(headerRef);
            out.writeBytesReference(termVectors);
        }
    }

    private boolean hasTermVectors() {
        assert (headerRef == null && termVectors == null) || (headerRef != null && termVectors != null);
        return headerRef != null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        index = in.readString();
        type = in.readString();
        id = in.readString();
        docVersion = in.readVLong();
        exists = in.readBoolean();
        artificial = in.readBoolean();
        tookInMillis = in.readVLong();
        if (in.readBoolean()) {
            headerRef = in.readBytesReference();
            termVectors = in.readBytesReference();
        }
    }

    public Fields getFields() throws IOException {
        if (hasTermVectors() && isExists()) {
            if (!sourceCopied) { // make the bytes safe
                headerRef = new BytesArray(headerRef.toBytesRef(), true);
                termVectors = new BytesArray(termVectors.toBytesRef(), true);
            }
            TermVectorsFields termVectorsFields = new TermVectorsFields(headerRef, termVectors);
            hasScores = termVectorsFields.hasScores;
            return termVectorsFields;
        } else {
            return new Fields() {
                @Override
                public Iterator<String> iterator() {
                    return Collections.emptyIterator();
                }

                @Override
                public Terms terms(String field) throws IOException {
                    return null;
                }

                @Override
                public int size() {
                    return 0;
                }
            };
        }
    }

    public static TermVectorsResponse fromXContent(XContentParser parser) throws IOException{
        TermVectorsResponse tvResponse = new TermVectorsResponse();


        XContentParser.Token token = parser.currentToken();
        String currentFieldName = parser.currentName();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new XContentParseException(parser.getTokenLocation(), "[term_vector] Expected START_OBJECT but was: " + token);
        }

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (currentFieldName.equals(FieldStrings._INDEX)) {
                    tvResponse.index = parser.text();
                } else if (currentFieldName.equals(FieldStrings._TYPE)) {
                    tvResponse.type = parser.text();
                } else {
                    throw new IllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (currentFieldName.equals(FieldStrings.TERM_VECTORS)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        termVectorsFromXContent(parser, tvResponse);
                    }
                } else {
                    throw new IllegalArgumentException("Unexpected object field [" + currentFieldName + "]");
                }
            } else {
                throw new XContentParseException(parser.getTokenLocation(), "[term_vector] unexpected token: " + token);
            }
        }

        return tvResponse;
    }

    private static void termVectorsFromXContent (XContentParser parser, TermVectorsResponse tvResponse) throws IOException {
        final BytesStreamOutput output = new BytesStreamOutput(1);
        final BytesStreamOutput termsOutput = new BytesStreamOutput(1);
        final List<String> fields = new ArrayList<>();
        final List<Long> fieldOffset = new ArrayList<>();
        String currentFieldName = null;
        boolean hasFieldStatistics = false;
        boolean hasTermStatistics = false;
        boolean hasScores = false;

        XContentParser.Token token = parser.currentToken();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            // process term_vector for this field
            assert token == XContentParser.Token.FIELD_NAME;
            String tvFieldName = parser.currentName();
            assert parser.nextToken() == XContentParser.Token.START_OBJECT ;

            long sumTotalTermFreq = -1;
            long sumDocFreq = -1;
            int docCount = -1;
            long termsSize = 0;

            boolean hasPositions = false;
            boolean hasOffsets = false;
            boolean hasPayloads = false;

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (currentFieldName.equals(FieldStrings.FIELD_STATISTICS)) {
                        // process field_statistics
                        hasFieldStatistics = true;
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                            } else if (token.isValue()) {
                                if (currentFieldName.equals(FieldStrings.SUM_TTF)) {
                                    sumTotalTermFreq = parser.longValue();
                                } else if (currentFieldName.equals(FieldStrings.SUM_DOC_FREQ)) {
                                    sumDocFreq = parser.longValue();
                                } else if (currentFieldName.equals(FieldStrings.DOC_COUNT)) {
                                    docCount = parser.intValue();
                                }
                            }
                        }
                    } else if (currentFieldName.equals(FieldStrings.TERMS)){
                        // process terms
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            assert token == XContentParser.Token.FIELD_NAME;
                            BytesRef term = new BytesRef(parser.currentName());
                            assert parser.nextToken() == XContentParser.Token.START_OBJECT ;
                            // process individual term
                            int docFreq = -1;
                            int termFreq = -1;
                            long totalTermFreq = -1;
                            float score = -1f;
                            int[] positions = new int[0];
                            int[] startOffsets = new int[0];
                            int[] endOffsets = new int[0];
                            String[] payloads = new String[0];
                            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                if (token == XContentParser.Token.FIELD_NAME) {
                                    currentFieldName = parser.currentName();
                                } else if (token.isValue()) {
                                    if (currentFieldName.equals(FieldStrings.TTF)) {
                                        totalTermFreq = parser.longValue();
                                        hasTermStatistics = true;
                                    } else if (currentFieldName.equals(FieldStrings.DOC_FREQ)) {
                                        docFreq = parser.intValue();
                                    } else if (currentFieldName.equals(FieldStrings.TERM_FREQ)) {
                                        termFreq = parser.intValue();
                                    } else if (currentFieldName.equals(FieldStrings.SCORE)) {
                                        hasScores = true;
                                        score = parser.floatValue();
                                    }
                                } else if (token == XContentParser.Token.START_ARRAY) {
                                    assert currentFieldName == FieldStrings.TOKENS;
                                    assert termFreq > 0;
                                    ArrayUtil.grow(positions, termFreq);
                                    ArrayUtil.grow(startOffsets, termFreq);
                                    ArrayUtil.grow(endOffsets, termFreq);
                                    int curTokenIndex = -1;
                                    // process tokens
                                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                        if (token == XContentParser.Token.START_OBJECT) {
                                            // process individual token
                                           curTokenIndex ++;
                                            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                                if (token == XContentParser.Token.FIELD_NAME) {
                                                    currentFieldName = parser.currentName();
                                                } else if (token.isValue()) {
                                                    if (currentFieldName.equals(FieldStrings.POS)) {
                                                        hasPositions = true;
                                                        positions[curTokenIndex] = parser.intValue();
                                                    } else if (currentFieldName.equals(FieldStrings.START_OFFSET)) {
                                                        hasOffsets = true;
                                                        startOffsets[curTokenIndex] = parser.intValue();
                                                    } else if (currentFieldName.equals(FieldStrings.END_OFFSET)) {
                                                        endOffsets[curTokenIndex] = parser.intValue();
                                                    } else if (currentFieldName.equals(FieldStrings.PAYLOAD)) {
                                                        hasPayloads = true;
                                                        payloads[curTokenIndex] = parser.text();
                                                    }
                                                }
                                            }
                                        }
                                    }

                                }
                            }
                            writeTerm(termsOutput, term, docFreq, termFreq, totalTermFreq, score,
                                positions, startOffsets, endOffsets, payloads, hasPayloads);
                            termsSize ++;
                        }
                    }
                }
            }
            fields.add(tvFieldName);
            fieldOffset.add(output.position());
            output.writeVLong(termsSize);
            output.writeBoolean(hasPositions);
            output.writeBoolean(hasOffsets);
            output.writeBoolean(hasPayloads);
            if (hasFieldStatistics) {
                output.writeVLong(Math.max(0, sumTotalTermFreq + 1));
                output.writeVLong(Math.max(0, sumDocFreq + 1));
                output.writeVInt(Math.max(0, docCount + 1));
            }
            output.writeBytesReference(termsOutput.bytes());
            termsOutput.reset();
        }

        tvResponse.setTermVectorsField(output);

        BytesStreamOutput header = new BytesStreamOutput();
        header.writeString("TV"); // HEADER
        header.writeInt(-1); // CURRENT_VERSION
        header.writeBoolean(hasFieldStatistics);
        header.writeBoolean(hasTermStatistics);
        header.writeBoolean(hasScores);
        header.writeVInt(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            header.writeString(fields.get(i));
            header.writeVLong(fieldOffset.get(i).longValue());
        }
        header.close();
        tvResponse.setHeader( header.bytes());
    }

    private static void writeTerm(BytesStreamOutput output, BytesRef term, int docFreq, int termFreq, long totalTermFreq, float score,
            int[] positions, int[] startOffsets, int[] endOffsets, String[] payloads, boolean hasPayloads) throws IOException {
        output.writeVInt(term.length);
        output.writeBytes(term.bytes, term.offset, term.length);
        if (docFreq > -1) output.writeVInt(Math.max(0, docFreq + 1));
        if (totalTermFreq > -1) output.writeVLong(Math.max(0, totalTermFreq + 1));
        if (termFreq > 0) output.writeVInt(Math.max(0, termFreq + 1));
        for (int i = 0; i < termFreq; i++) {
            if (positions[i] >= 0) output.writeVInt(positions[i]);
            if (startOffsets[i] >= 0) output.writeVInt(startOffsets[i]);
            if (endOffsets[i] >= 0) output.writeVInt(endOffsets[i]);
            if (hasPayloads) {
                if (payloads[i] != null) {
                    BytesRef payload = new BytesRef(payloads[i]);
                    output.writeVInt(payload.length);
                    output.writeBytes(payload.bytes, payload.offset, payload.length);
                } else {
                    output.writeVInt(0);
                }
            }
        }
        if (score > -1f) output.writeFloat(Math.max(0, score));
    }


    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        assert index != null;
        assert type != null;
        assert id != null;
        builder.startObject();
        builder.field(FieldStrings._INDEX, index);
        builder.field(FieldStrings._TYPE, type);
        if (!isArtificial()) {
            builder.field(FieldStrings._ID, id);
        }
        builder.field(FieldStrings._VERSION, docVersion);
        builder.field(FieldStrings.FOUND, isExists());
        builder.field(FieldStrings.TOOK, tookInMillis);
        if (isExists()) {
            builder.startObject(FieldStrings.TERM_VECTORS);
            final CharsRefBuilder spare = new CharsRefBuilder();
            Fields theFields = getFields();
            Iterator<String> fieldIter = theFields.iterator();
            while (fieldIter.hasNext()) {
                buildField(builder, spare, theFields, fieldIter);
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    private void buildField(XContentBuilder builder, final CharsRefBuilder spare, Fields theFields, Iterator<String> fieldIter) throws IOException {
        String fieldName = fieldIter.next();
        builder.startObject(fieldName);
        Terms curTerms = theFields.terms(fieldName);
        // write field statistics
        buildFieldStatistics(builder, curTerms);
        builder.startObject(FieldStrings.TERMS);
        TermsEnum termIter = curTerms.iterator();
        BoostAttribute boostAtt = termIter.attributes().addAttribute(BoostAttribute.class);
        for (int i = 0; i < curTerms.size(); i++) {
            buildTerm(builder, spare, curTerms, termIter, boostAtt);
        }
        builder.endObject();
        builder.endObject();
    }

    private void buildTerm(XContentBuilder builder, final CharsRefBuilder spare, Terms curTerms, TermsEnum termIter, BoostAttribute boostAtt) throws IOException {
        // start term, optimized writing
        BytesRef term = termIter.next();
        spare.copyUTF8Bytes(term);
        builder.startObject(spare.toString());
        buildTermStatistics(builder, termIter);
        // finally write the term vectors
        PostingsEnum posEnum = termIter.postings(null, PostingsEnum.ALL);
        int termFreq = posEnum.freq();
        builder.field(FieldStrings.TERM_FREQ, termFreq);
        initMemory(curTerms, termFreq);
        initValues(curTerms, posEnum, termFreq);
        buildValues(builder, curTerms, termFreq);
        buildScore(builder, boostAtt);
        builder.endObject();
    }

    private void buildTermStatistics(XContentBuilder builder, TermsEnum termIter) throws IOException {
        // write term statistics. At this point we do not naturally have a
        // boolean that says if these values actually were requested.
        // However, we can assume that they were not if the statistic values are
        // <= 0.
        assert (((termIter.docFreq() > 0) && (termIter.totalTermFreq() > 0)) || ((termIter.docFreq() == -1) && (termIter.totalTermFreq() == -1)));
        int docFreq = termIter.docFreq();
        if (docFreq > 0) {
            builder.field(FieldStrings.DOC_FREQ, docFreq);
            builder.field(FieldStrings.TTF, termIter.totalTermFreq());
        }
    }

    private void buildValues(XContentBuilder builder, Terms curTerms, int termFreq) throws IOException {
        if (!(curTerms.hasPayloads() || curTerms.hasOffsets() || curTerms.hasPositions())) {
            return;
        }

        builder.startArray(FieldStrings.TOKENS);
        for (int i = 0; i < termFreq; i++) {
            builder.startObject();
            if (curTerms.hasPositions()) {
                builder.field(FieldStrings.POS, currentPositions[i]);
            }
            if (curTerms.hasOffsets()) {
                builder.field(FieldStrings.START_OFFSET, currentStartOffset[i]);
                builder.field(FieldStrings.END_OFFSET, currentEndOffset[i]);
            }
            if (curTerms.hasPayloads() && (currentPayloads[i].length() > 0)) {
                BytesRef bytesRef = currentPayloads[i].toBytesRef();
                builder.field(FieldStrings.PAYLOAD, bytesRef.bytes, bytesRef.offset, bytesRef.length);
            }
            builder.endObject();
        }
        builder.endArray();
    }

    private void initValues(Terms curTerms, PostingsEnum posEnum, int termFreq) throws IOException {
        for (int j = 0; j < termFreq; j++) {
            int nextPos = posEnum.nextPosition();
            if (curTerms.hasPositions()) {
                currentPositions[j] = nextPos;
            }
            if (curTerms.hasOffsets()) {
                currentStartOffset[j] = posEnum.startOffset();
                currentEndOffset[j] = posEnum.endOffset();
            }
            if (curTerms.hasPayloads()) {
                BytesRef curPayload = posEnum.getPayload();
                if (curPayload != null) {
                    currentPayloads[j] = new BytesArray(curPayload.bytes, 0, curPayload.length);
                } else {
                    currentPayloads[j] = null;
                }
            }
        }
    }

    private void initMemory(Terms curTerms, int termFreq) {
        // init memory for performance reasons
        if (curTerms.hasPositions()) {
            currentPositions = ArrayUtil.grow(currentPositions, termFreq);
        }
        if (curTerms.hasOffsets()) {
            currentStartOffset = ArrayUtil.grow(currentStartOffset, termFreq);
            currentEndOffset = ArrayUtil.grow(currentEndOffset, termFreq);
        }
        if (curTerms.hasPayloads()) {
            currentPayloads = new BytesArray[termFreq];
        }
    }

    private void buildFieldStatistics(XContentBuilder builder, Terms curTerms) throws IOException {
        long sumDocFreq = curTerms.getSumDocFreq();
        int docCount = curTerms.getDocCount();
        long sumTotalTermFrequencies = curTerms.getSumTotalTermFreq();
        if (docCount >= 0) {
            assert ((sumDocFreq >= 0)) : "docCount >= 0 but sumDocFreq ain't!";
            assert ((sumTotalTermFrequencies >= 0)) : "docCount >= 0 but sumTotalTermFrequencies ain't!";
            builder.startObject(FieldStrings.FIELD_STATISTICS);
            builder.field(FieldStrings.SUM_DOC_FREQ, sumDocFreq);
            builder.field(FieldStrings.DOC_COUNT, docCount);
            builder.field(FieldStrings.SUM_TTF, sumTotalTermFrequencies);
            builder.endObject();
        } else if (docCount == -1) { // this should only be -1 if the field
            // statistics were not requested at all. In
            // this case all 3 values should be -1
            assert ((sumDocFreq == -1)) : "docCount was -1 but sumDocFreq ain't!";
            assert ((sumTotalTermFrequencies == -1)) : "docCount was -1 but sumTotalTermFrequencies ain't!";
        } else {
            throw new IllegalStateException(
                    "Something is wrong with the field statistics of the term vector request: Values are " + "\n"
                            + FieldStrings.SUM_DOC_FREQ + " " + sumDocFreq + "\n" + FieldStrings.DOC_COUNT + " " + docCount + "\n"
                            + FieldStrings.SUM_TTF + " " + sumTotalTermFrequencies);
        }
    }

    public void setTookInMillis(long tookInMillis) {
        this.tookInMillis = tookInMillis;
    }

    public TimeValue getTook() {
        return new TimeValue(tookInMillis);
    }

    private void buildScore(XContentBuilder builder, BoostAttribute boostAtt) throws IOException {
        if (hasScores) {
            builder.field(FieldStrings.SCORE, boostAtt.getBoost());
        }
    }

    public boolean isExists() {
        return exists;
    }

    public void setExists(boolean exists) {
         this.exists = exists;
    }

    public void setFields(Fields termVectorsByField, Set<String> selectedFields, EnumSet<Flag> flags, Fields topLevelFields) throws IOException {
        setFields(termVectorsByField, selectedFields, flags, topLevelFields, null, null);
    }

    public void setFields(Fields termVectorsByField, Set<String> selectedFields, EnumSet<Flag> flags, Fields topLevelFields, @Nullable AggregatedDfs dfs,
                          TermVectorsFilter termVectorsFilter) throws IOException {
        TermVectorsWriter tvw = new TermVectorsWriter(this);

        if (termVectorsByField != null) {
            tvw.setFields(termVectorsByField, selectedFields, flags, topLevelFields, dfs, termVectorsFilter);
        }
    }

    public void setTermVectorsField(BytesStreamOutput output) {
        termVectors = output.bytes();
    }

    public void setHeader(BytesReference header) {
        headerRef = header;
    }

    public void setDocVersion(long version) {
        this.docVersion = version;

    }

    public Long getVersion() {
        return docVersion;
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public String getId() {
        return id;
    }

    public boolean isArtificial() {
        return artificial;
    }

    public void setArtificial(boolean artificial) {
        this.artificial = artificial;
    }
}
