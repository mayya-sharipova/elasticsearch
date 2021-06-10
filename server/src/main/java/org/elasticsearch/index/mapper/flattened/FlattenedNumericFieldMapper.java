/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.flattened;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.exc.InputCoercionException;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.DynamicFieldType;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.SimpleMappedFieldType;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.index.mapper.flattened.FlattenedFieldParser.SEPARATOR;

public class FlattenedNumericFieldMapper extends FieldMapper {
    public static final String CONTENT_TYPE = "flattened_numeric";

    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();
        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends FieldMapper.Builder {
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name) {
            super(name);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(meta);
        }

        @Override
        public FieldMapper build(ContentPath contentPath) {
            MultiFields multiFields = multiFieldsBuilder.build(this, contentPath);
            if (multiFields.iterator().hasNext()) {
                throw new IllegalArgumentException(CONTENT_TYPE + " field [" + name + "] does not support [fields]");
            }
            CopyTo copyTo = this.copyTo.build();
            if (copyTo.copyToFields().isEmpty() == false) {
                throw new IllegalArgumentException(CONTENT_TYPE + " field [" + name + "] does not support [copy_to]");
            }
            FlattenedNumericFieldType fieldType = new FlattenedNumericFieldType(
                buildFullName(contentPath),
                true,
                false,
                true,
                meta.getValue()
            );
            return new FlattenedNumericFieldMapper(name, fieldType, this);
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    public static final class FlattenedNumericFieldType extends SimpleMappedFieldType implements DynamicFieldType {

        public FlattenedNumericFieldType(
            String name,
            boolean indexed,
            boolean isStored,
            boolean hasDocValues,
            Map<String, String> meta
        ) {
            super(name, indexed, isStored, hasDocValues, TextSearchInfo.SIMPLE_MATCH_WITHOUT_TERMS, meta);
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return null;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            return null;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            throw new IllegalArgumentException("The top level flattened_numberic fields do not support sorting and aggregations. " +
                "Use specific field keys for this!");
        }

        @Override
        public MappedFieldType getChildFieldType(String childPath) {
            return new KeyedFlattenedFieldType(name(), childPath, this);
        }
    }


    /**
     * A field type that represents the values under a particular JSON key, used
     * when searching under a specific key as in 'my_flattened.key: some_value'.
     */
    public static final class KeyedFlattenedFieldType extends StringFieldType {
        private final String key;
        private final String rootName;

        KeyedFlattenedFieldType(String rootName, boolean indexed, boolean hasDocValues, String key, Map<String, String> meta) {
            super(rootName, indexed, false, hasDocValues, TextSearchInfo.SIMPLE_MATCH_ONLY,
                meta);
            this.key = key;
            this.rootName = rootName;
        }

        private KeyedFlattenedFieldType(String rootName, String key, FlattenedNumericFieldType ref) {
            this(rootName, ref.isSearchable(), ref.hasDocValues(), key, ref.meta());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        public String key() {
            return key;
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            Term term = new Term(name(), FlattenedFieldParser.createKeyedValue(key, ""));
            return new PrefixQuery(term);
        }

        @Override
        public Query rangeQuery(Object lowerTerm,
                                Object upperTerm,
                                boolean includeLower,
                                boolean includeUpper,
                                SearchExecutionContext context) {

            // We require range queries to specify both bounds because an unbounded query could incorrectly match
            // values from other keys. For example, a query on the 'first' key with only a lower bound would become
            // ("first\0value", null), which would also match the value "second\0value" belonging to the key 'second'.
            if (lowerTerm == null || upperTerm == null) {
                throw new IllegalArgumentException("[range] queries on keyed [" + CONTENT_TYPE +
                    "] fields must include both an upper and a lower bound.");
            }

            return super.rangeQuery(lowerTerm, upperTerm,
                includeLower, includeUpper, context);
        }

        @Override
        public Query fuzzyQuery(Object value, Fuzziness fuzziness, int prefixLength, int maxExpansions,
                                boolean transpositions, SearchExecutionContext context) {
            throw new UnsupportedOperationException("[fuzzy] queries are not currently supported on keyed " +
                "[" + CONTENT_TYPE + "] fields.");
        }

        @Override
        public Query regexpQuery(String value, int syntaxFlags, int matchFlags, int maxDeterminizedStates,
                                 MultiTermQuery.RewriteMethod method, SearchExecutionContext context) {
            throw new UnsupportedOperationException("[regexp] queries are not currently supported on keyed " +
                "[" + CONTENT_TYPE + "] fields.");
        }

        @Override
        public Query wildcardQuery(String value,
                                   MultiTermQuery.RewriteMethod method,
                                   boolean caseInsensitive,
                                   SearchExecutionContext context) {
            throw new UnsupportedOperationException("[wildcard] queries are not currently supported on keyed " +
                "[" + CONTENT_TYPE + "] fields.");
        }

        @Override
        public Query termQueryCaseInsensitive(Object value, SearchExecutionContext context) {
            return AutomatonQueries.caseInsensitiveTermQuery(new Term(name(), indexedValueForSearch(value)));
        }

        @Override
        public BytesRef indexedValueForSearch(Object value) {
            if (value == null) {
                return null;
            }
            long v = NumberFieldMapper.NumberType.objectToLong(value, true);

            byte[] keyBytes = (key + SEPARATOR).getBytes(StandardCharsets.UTF_8);
            byte[] fieldValue = new byte[keyBytes.length + 8];
            System.arraycopy(keyBytes, 0, fieldValue, 0, keyBytes.length);
            LongPoint.encodeDimension(v, fieldValue, keyBytes.length);
            return new BytesRef(fieldValue);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return (cache, breakerService) -> {
                final IndexFieldData keyedFlattenedFieldData = new FlattenedFieldMapper.KeyedFlattenedFieldData.Builder(
                    name(), key, CoreValuesSourceType.KEYWORD)
                    .build(cache, breakerService);
                return new FlattenedNumericIndexFieldData(keyedFlattenedFieldData, IndexNumericFieldData.NumericType.LONG);
            };
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + rootName + "." + key + "] of type [" + typeName() +
                    "] doesn't support formats.");
            }
            return SourceValueFetcher.identity(rootName + "." + key, context, format);
        }
    }

    private FlattenedNumericFieldMapper(String simpleName, MappedFieldType mappedFieldType, Builder builder) {
        super(simpleName, mappedFieldType, Lucene.KEYWORD_ANALYZER, MultiFields.empty(), CopyTo.empty());
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public FlattenedNumericFieldType fieldType() {
        return (FlattenedNumericFieldType) super.fieldType();
    }


    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        if (context.parser().currentToken() == XContentParser.Token.VALUE_NULL) {
            return;
        }
        if (mappedFieldType.isSearchable() == false && mappedFieldType.hasDocValues() == false) {
            context.parser().skipChildren();
            return;
        }
        XContentParser xContentParser = context.parser();
        context.doc().addAll(parse(xContentParser));
        if (mappedFieldType.hasDocValues() == false) {
            context.addToFieldNames(fieldType().name());
        }
    }

    private List<IndexableField> parse(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);

        ContentPath path = new ContentPath();
        List<IndexableField> fields = new ArrayList<>();
        parseObject(parser, path, fields);
        return fields;
    }

    private void parseObject(XContentParser parser,
                             ContentPath path,
                             List<IndexableField> fields) throws IOException {
        String currentName = null;
        while (true) {
            XContentParser.Token token = parser.nextToken();
            if (token == XContentParser.Token.END_OBJECT) {
                return;
            }
            if (token == XContentParser.Token.FIELD_NAME) {
                currentName = parser.currentName();
            } else {
                assert currentName != null;
                parseFieldValue(token, parser, path, currentName, fields);
            }
        }
    }

    private void parseArray(XContentParser parser,
                            ContentPath path,
                            String currentName,
                            List<IndexableField> fields) throws IOException {
        while (true) {
            XContentParser.Token token = parser.nextToken();
            if (token == XContentParser.Token.END_ARRAY) {
                return;
            }
            parseFieldValue(token, parser, path, currentName, fields);
        }
    }

    private void parseFieldValue(XContentParser.Token token,
                                 XContentParser parser,
                                 ContentPath path,
                                 String currentName,
                                 List<IndexableField> fields) throws IOException {
        if (token == XContentParser.Token.START_OBJECT) {
            path.add(currentName);
            parseObject(parser, path, fields);
            path.remove();
        } else if (token == XContentParser.Token.START_ARRAY) {
            parseArray(parser, path, currentName, fields);
        } else if (token.isValue()) {
            try {
                // TODO: handle coersion
                long numericValue = parser.longValue();
                addField(path, currentName, numericValue, fields);
            } catch (InputCoercionException | IllegalArgumentException | JsonParseException e) {
                // TODO: handle malformed values
            }
        } else if (token == XContentParser.Token.VALUE_NULL) {
            // TOD: handle null values
        } else {
            // Note that we throw an exception here just to be safe. We don't actually expect to reach
            // this case, since XContentParser verifies that the input is well-formed as it parses.
            throw new IllegalArgumentException("Encountered unexpected token [" + token.toString() + "].");
        }
    }

    private void addField(ContentPath path, String currentName, long value, List<IndexableField> fields) {
        String key = path.pathAsText(currentName);
        if (key.contains(SEPARATOR)) {
            throw new IllegalArgumentException("Keys in [flattened_numeric] fields cannot contain the reserved character \\0."
                + " Offending key: [" + key + "].");
        }
        if (fieldType().isSearchable() || fieldType().hasDocValues()) {
            byte[] keyBytes = (key + SEPARATOR).getBytes(StandardCharsets.UTF_8);
            byte[] fieldValue = new byte[keyBytes.length + 8];
            System.arraycopy(keyBytes, 0, fieldValue, 0, keyBytes.length);
            LongPoint.encodeDimension(value, fieldValue, keyBytes.length);
            if (fieldType().isSearchable()) {
                fields.add(new Field(fieldType().name(), fieldValue, Defaults.FIELD_TYPE));
            }
            if (fieldType().hasDocValues()) {
                fields.add(new SortedSetDocValuesField(fieldType().name(), new BytesRef(fieldValue)));
            }
        }
    }
}
