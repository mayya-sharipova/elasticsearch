/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.highlight.SimpleHTMLEncoder;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.lucene.search.uhighlight.BoundedBreakIteratorScanner;
import org.elasticsearch.search.fetch.FetchSubPhase;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public final class HighlightUtils {

    // U+2029 PARAGRAPH SEPARATOR (PS): each value holds a discrete passage for highlighting (unified highlighter)
    public static final char PARAGRAPH_SEPARATOR = 8233;
    public static final char NULL_SEPARATOR = '\u0000';

    private HighlightUtils() {

    }

    /**
     * Load field values for highlighting.
     */
    public static List<Object> loadFieldValues(
        MappedFieldType fieldType,
        SearchExecutionContext searchContext,
        FetchSubPhase.HitContext hitContext
    ) throws IOException {
        if (fieldType.isStored()) {
            List<Object> values = hitContext.loadedFields().get(fieldType.name());
            return values == null ? List.of() : values;
        }
        ValueFetcher fetcher = fieldType.valueFetcher(searchContext, null);
        fetcher.setNextReader(hitContext.readerContext());
        return fetcher.fetchValues(hitContext.source(), hitContext.docId(), new ArrayList<>());
    }

    public static class Encoders {
        public static final Encoder DEFAULT = new DefaultEncoder();
        public static final Encoder HTML = new SimpleHTMLEncoder();
    }

    public static BreakIterator getBreakIterator(SearchHighlightContext.Field field) {
        final SearchHighlightContext.FieldOptions fieldOptions = field.fieldOptions();
        final Locale locale = fieldOptions.boundaryScannerLocale() != null ? fieldOptions.boundaryScannerLocale() : Locale.ROOT;
        final HighlightBuilder.BoundaryScannerType type = fieldOptions.boundaryScannerType() != null
            ? fieldOptions.boundaryScannerType()
            : HighlightBuilder.BoundaryScannerType.SENTENCE;
        int maxLen = fieldOptions.fragmentCharSize();
        switch (type) {
            case SENTENCE -> {
                if (maxLen > 0) {
                    return BoundedBreakIteratorScanner.getSentence(locale, maxLen);
                }
                return BreakIterator.getSentenceInstance(locale);
            }
            case WORD -> {
                // ignore maxLen
                return BreakIterator.getWordInstance(locale);
            }
            default -> throw new IllegalArgumentException("Invalid boundary scanner type: " + type);
        }
    }

}
