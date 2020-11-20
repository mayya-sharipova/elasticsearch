/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.lucene.index;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MultiReader;

import java.io.IOException;
import java.util.function.Function;

/**
 * A {@link MultiReader} that reorders the leaves of the provided {@link DirectoryReader}.
 */
public class FilterMultiReader extends MultiReader {
    private final DirectoryReader original;

    public FilterMultiReader(DirectoryReader original, Function<DirectoryReader, LeafReader[]> orderLeaves) throws IOException {
        super(orderLeaves.apply(original));
        this.original = original;
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return original.getReaderCacheHelper();
    }

    public IndexReader getOriginal() {
        return original;
    }
}
