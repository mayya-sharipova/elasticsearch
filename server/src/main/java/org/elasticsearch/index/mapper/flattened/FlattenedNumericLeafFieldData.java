/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.plain.LeafLongFieldData;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;

public class FlattenedNumericLeafFieldData extends LeafLongFieldData {
    private final KeyedFlattenedLeafFieldData keyedFlattedLFD;

    FlattenedNumericLeafFieldData(KeyedFlattenedLeafFieldData keyedFlattedLFD) {
        super(0L, IndexNumericFieldData.NumericType.LONG);
        this.keyedFlattedLFD = keyedFlattedLFD;
    }

    @Override
    public SortedNumericDocValues getLongValues() {
        SortedSetDocValues values = keyedFlattedLFD.getOrdinalsValues();
        return new SortedNumericDocValues() {
            private int count = 0;
            private long[] ords = new long[1];
            private int idx;

            @Override
            public int docID() {
                return values.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                int nextDoc = values.nextDoc();
                if (nextDoc != NO_MORE_DOCS) {
                    countValuesInDoc();
                }
                return nextDoc;
            }

            @Override
            public int advance(int target) throws IOException {
                int nextDoc = values.advance(target);
                if (nextDoc != NO_MORE_DOCS) {
                    countValuesInDoc();
                }
                return nextDoc;
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                if (values.advanceExact(target) == false) {
                    return false;
                }
                countValuesInDoc();
                return true;
            }

            @Override
            public long nextValue() throws IOException {
                if (idx >= count) {
                    throw new IndexOutOfBoundsException();
                }
                BytesRef value = values.lookupOrd(ords[idx++]);
                return NumericUtils.sortableBytesToLong(value.bytes, value.offset);
            }

            @Override
            public int docValueCount() {
                return count;
            }

            @Override
            public long cost() {
                return values.cost();
            }

            private void countValuesInDoc() throws IOException {
                idx = 0;
                count = 0;
                for (long ord = values.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = values.nextOrd()) {
                    count++;
                    if (ords.length < count) {
                        ords = ArrayUtil.grow(ords, count);
                    }
                    ords[count - 1] = ord;
                }
            }

        };
    }

    @Override
    public long ramBytesUsed() {
        return keyedFlattedLFD.ramBytesUsed();
    }

    @Override
    public void close() {
        keyedFlattedLFD.close();
    }

    @Override
    public FormattedDocValues getFormattedValues(DocValueFormat format) {
        SortedNumericDocValues values = getLongValues();
        return new FormattedDocValues() {
            @Override
            public boolean advanceExact(int docId) throws IOException {
                return values.advanceExact(docId);
            }

            @Override
            public int docValueCount() {
                return values.docValueCount();
            }

            @Override
            public Object nextValue() throws IOException {
                return format.format(values.nextValue());
            }
        };
    }
}
