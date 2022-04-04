/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.fetch.subphase.highlight.matchhighlight;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.matchhighlight.PassageFormatter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.fetch.FetchSubPhase.HitContext;
import org.elasticsearch.search.fetch.subphase.highlight.FieldHighlightContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightUtils;
import org.elasticsearch.search.fetch.subphase.highlight.Highlighter;
import org.elasticsearch.search.fetch.subphase.highlight.LimitTokenOffsetAnalyzer;
import org.elasticsearch.search.fetch.subphase.highlight.SearchHighlightContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.lucene.search.uhighlight.CustomUnifiedHighlighter.MULTIVAL_SEP_CHAR;

public class MatchHighlighter implements Highlighter {
    @Override
    public boolean canHighlight(MappedFieldType fieldType) {
        return true;
    }

    //TODO: share Query across fields
    @Override
    public HighlightField highlight(FieldHighlightContext fieldContext) throws IOException {
        @SuppressWarnings("unchecked")
        Map<String, CustomMatchHighlighter> cache = (Map<String, CustomMatchHighlighter>) fieldContext.cache.computeIfAbsent(
            MatchHighlighter.class.getName(),
            k -> new HashMap<>()
        );
        CustomMatchHighlighter highlighter;
        if (cache.containsKey(fieldContext.fieldName) == false) {
            highlighter = buildHighlighter(fieldContext);
            cache.put(fieldContext.fieldName, highlighter);
        } else {
            highlighter = cache.get(fieldContext.fieldName);
        }
        CheckedSupplier<String, IOException> loadFieldValues = () -> {
            List<Object> fieldValues = loadFieldValues(
                fieldContext.context.getSearchExecutionContext(),
                fieldContext.fieldType,
                fieldContext.hitContext,
                fieldContext.forceSource
            );
            if (fieldValues.size() == 0) {
                return null;
            }
            return mergeFieldValues(fieldValues, MULTIVAL_SEP_CHAR);
        };
        return highlighter.highlightField(fieldContext.hitContext.readerContext(), fieldContext.hitContext.docId(), loadFieldValues);
    }

    private static CustomMatchHighlighter buildHighlighter(FieldHighlightContext fieldContext) throws IOException {
        SearchHighlightContext.FieldOptions fieldOptions = fieldContext.field.fieldOptions();
        FieldHighlighter fieldHighlighter = new FieldHighlighter(
            fieldOptions.fragmentCharSize(),
            fieldOptions.numberOfFragments(),
            new PassageFormatter("", fieldOptions.preTags()[0], fieldOptions.postTags()[0])
        );
        Analyzer analyzer = wrapAnalyzer(
            fieldContext.context.getSearchExecutionContext().getIndexAnalyzer(f -> Lucene.KEYWORD_ANALYZER),
            fieldOptions.maxAnalyzedOffset()
        );
        CustomMatchHighlighter highlighter = new CustomMatchHighlighter(
            fieldContext.fieldName, fieldContext.context.searcher(), analyzer, fieldHighlighter, fieldContext.query
        );
        return highlighter;
    }

    private static Analyzer wrapAnalyzer(Analyzer analyzer, Integer maxAnalyzedOffset) {
        if (maxAnalyzedOffset != null) {
            analyzer = new LimitTokenOffsetAnalyzer(analyzer, maxAnalyzedOffset);
        }
        return analyzer;
    }

    protected List<Object> loadFieldValues(
        SearchExecutionContext searchContext,
        MappedFieldType fieldType,
        HitContext hitContext,
        boolean forceSource
    ) throws IOException {
        return HighlightUtils.loadFieldValues(fieldType, searchContext, hitContext, forceSource)
            .stream()
            .<Object>map((s) -> convertFieldValue(fieldType, s))
            .toList();
    }

    protected static String convertFieldValue(MappedFieldType type, Object value) {
        if (value instanceof BytesRef) {
            return type.valueForDisplay(value).toString();
        } else {
            return value.toString();
        }
    }

    protected static String mergeFieldValues(List<Object> fieldValues, char valuesSeparator) {
        // postings highlighter accepts all values in a single string, as offsets etc. need to match with content
        // loaded from stored fields, we merge all values using a proper separator
        String rawValue = Strings.collectionToDelimitedString(fieldValues, String.valueOf(valuesSeparator));
        return rawValue.substring(0, Math.min(rawValue.length(), Integer.MAX_VALUE - 1));
    }
}
