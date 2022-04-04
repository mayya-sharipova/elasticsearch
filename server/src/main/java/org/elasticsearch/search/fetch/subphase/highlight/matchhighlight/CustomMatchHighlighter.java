/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase.highlight.matchhighlight;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.matchhighlight.MatchRegionRetriever;
import org.apache.lucene.search.matchhighlight.OffsetRange;
import org.apache.lucene.search.matchhighlight.OffsetsRetrievalStrategySupplier;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


/**
 * Modified version of Lucene's MatchHighlighter for a single field
 */
public class CustomMatchHighlighter {
    private final String fieldName;
    private final IndexSearcher searcher;
    private final OffsetsRetrievalStrategySupplier offsetsRetrievalStrategies;
    private final Analyzer analyzer;
    private final FieldHighlighter fieldHighlighter;
    private final Query query;
    private final MatchRegionRetriever matchRetrieval;
    private final Map<String, List<OffsetRange>> offsets;

    public CustomMatchHighlighter(String fieldName, IndexSearcher searcher, Analyzer analyzer,
            FieldHighlighter fieldHighlighter, Query query) throws IOException {
        this.fieldName = fieldName;
        this.searcher = searcher;
        this.offsetsRetrievalStrategies = MatchRegionRetriever.computeOffsetRetrievalStrategies(searcher.getIndexReader(), analyzer);
        this.analyzer = analyzer;
        this.fieldHighlighter = fieldHighlighter;
        this.query = searcher.rewrite(query);
        this.matchRetrieval = new MatchRegionRetriever(searcher, this.query, offsetsRetrievalStrategies);
        this.offsets = new TreeMap<>();
    }

    /**
     * Highlights the field value.
     */
    public HighlightField highlightField(LeafReaderContext readerContext, int docId, CheckedSupplier<String, IOException> loadFieldValue)
            throws IOException {
        String fieldValue = loadFieldValue.get();
        if (fieldValue == null) {
            return null;
        }
        offsets.clear();
        matchRetrieval.highlightDocument(
            readerContext, docId, (field) -> Collections.singletonList(fieldValue), (field) -> true, offsets
        );

        String[] values = {fieldValue};
        String contiguousValue = contiguousFieldValue(fieldName, values);
        List<OffsetRange> valueRanges = computeValueRanges(fieldName, values);
        List<String> formattedValues = fieldHighlighter.format(contiguousValue, valueRanges, offsets.get(fieldName));
        return new HighlightField(fieldName, Text.convertFromStringArray(formattedValues.toArray(new String[0])));
    }

    private String contiguousFieldValue(String field, String[] values) {
        String value;
        if (values.length == 1) {
            value = values[0];
        } else {
            // TODO: This can be inefficient if offset gap is large but the logic
            // of applying offsets would get much more complicated so leaving for now
            // (would have to recalculate all offsets to omit gaps).
            String fieldGapPadding = " ".repeat(analyzer.getOffsetGap(field));
            value = String.join(fieldGapPadding, values);
        }
        return value;
    }

    private List<OffsetRange> computeValueRanges(String field, String[] values) {
        ArrayList<OffsetRange> valueRanges = new ArrayList<>();
        int offset = 0;
        for (CharSequence v : values) {
            valueRanges.add(new OffsetRange(offset, offset + v.length()));
            offset += v.length();
            offset += analyzer.getOffsetGap(field);
        }
        return valueRanges;
    }
}
