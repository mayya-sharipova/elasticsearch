/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

public class FlattenedNumericIndexFieldData extends IndexNumericFieldData {
    private final IndexFieldData keyedFlattenedIFD;
    private final NumericType numericType;


    FlattenedNumericIndexFieldData(IndexFieldData keyedFlattenedIFD, NumericType numericType) {
        this.keyedFlattenedIFD = keyedFlattenedIFD;
        this.numericType = numericType;
    }

    @Override
    public String getFieldName() {
        return keyedFlattenedIFD.getFieldName();
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return numericType.getValuesSourceType();
    }

    @Override
    public LeafNumericFieldData load(LeafReaderContext context) {
        return new FlattenedNumericLeafFieldData((KeyedFlattenedLeafFieldData) keyedFlattenedIFD.load(context));
    }

    @Override
    public LeafNumericFieldData loadDirect(LeafReaderContext context) throws Exception {
        return new FlattenedNumericLeafFieldData((KeyedFlattenedLeafFieldData) keyedFlattenedIFD.loadDirect(context));
    }

    @Override
    protected boolean sortRequiresCustomComparator() {
        return true;
    }

    @Override
    public NumericType getNumericType() {
        return numericType;
    }

}
