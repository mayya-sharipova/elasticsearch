/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase.highlight.matchhighlight;

import org.apache.lucene.search.matchhighlight.OffsetRange;
import org.apache.lucene.search.matchhighlight.Passage;
import org.apache.lucene.search.matchhighlight.PassageFormatter;
import org.apache.lucene.search.matchhighlight.PassageSelector;

import java.util.List;

import static org.apache.lucene.search.matchhighlight.FieldValueHighlighters.defaultPassageSelector;

/**
 * Modified version of Lucene's FieldValueHighlighters::highighted
 */
public class FieldHighlighter {
    private final int maxPassageWindow;
    private final int maxPassages;
    private final PassageFormatter passageFormatter;

    public FieldHighlighter(int maxPassageWindow, int maxPassages, PassageFormatter passageFormatter) {
        this.maxPassageWindow = maxPassageWindow;
        this.maxPassages = maxPassages;
        this.passageFormatter = passageFormatter;
    }

    public List<String> format(String contiguousValue, List<OffsetRange> valueRanges, List<OffsetRange> matchOffsets) {
        PassageSelector passageSelector = defaultPassageSelector();
        assert matchOffsets != null;
        List<Passage> bestPassages =
            passageSelector.pickBest(
                contiguousValue, matchOffsets, maxPassageWindow, maxPassages, valueRanges);
        return passageFormatter.format(contiguousValue, bestPassages, valueRanges);
    }
}
