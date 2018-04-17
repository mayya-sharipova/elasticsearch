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

package org.elasticsearch.common.lucene.search.function;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.script.Script;

import java.io.IOException;
import java.util.*;

/**
 * A query that allows for
 */
public class ScriptScoreQuery extends Query {
    final Query subQuery;
    final ScriptScoreFunction function;
    private final Float minScore;

    public ScriptScoreQuery(Query subQuery, ScriptScoreFunction function, Float minScore) {
        this.subQuery = subQuery;
        this.function = function;
        this.minScore = minScore;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query rewritten = super.rewrite(reader);
        if (rewritten != this) {
            return rewritten;
        }
        Query newQ = subQuery.rewrite(reader);
        ScriptScoreFunction newFunction = (ScriptScoreFunction) function.rewrite(reader);
        boolean needsRewrite = (newQ != subQuery) || (newFunction != function);

        if (needsRewrite) {
            return new ScriptScoreQuery(newQ, newFunction, minScore);
        }
        return this;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
        if (needsScores == false && minScore == null) {
            return subQuery.createWeight(searcher, needsScores, boost);
        }
        boolean functionNeedsScores = function.needsScores();
        Weight subQueryWeight = subQuery.createWeight(searcher, functionNeedsScores, boost);
        return new Weight(this){
            @Override
            public void extractTerms(Set<Term> terms) {
                subQueryWeight.extractTerms(terms);
            }

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                Scorer subQueryScorer = subQueryWeight.scorer(context);
                if (subQueryScorer == null) {
                    return null;
                }
                final LeafScoreFunction leafFunction = function.getLeafScoreFunction(context);
                Scorer scriptScorer = new Scorer(this) {
                    @Override
                    public float score() throws IOException {
                        int docId = docID();
                        float subQueryScore = functionNeedsScores ? subQueryScorer.score() : 0f;
                        float score = (float) leafFunction.score(docId, subQueryScore);
                        if (score == Float.NEGATIVE_INFINITY || Float.isNaN(score)) {
                            throw new ElasticsearchException("script score query returned an invalid score: " + score + " for doc: " + docId);
                        }
                        return score;
                    }
                    @Override
                    public final int docID() {
                        return subQueryScorer.docID();
                    }

                    @Override
                    public final DocIdSetIterator iterator() {
                        return subQueryScorer.iterator();
                    }
                };

                if (minScore != null) {
                    scriptScorer = new MinScoreScorer(this, scriptScorer, minScore);
                }
                return scriptScorer;
            }

            @Override
            public Explanation explain(LeafReaderContext leafReaderContext, int docId) throws IOException {
                return null;
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                // If minScore is not null, then matches depend on statistics of the top-level reader.
                return minScore == null;
            }
        };
    }


    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("script score (").append(subQuery.toString(field)).append(", function: ");
        sb.append("{" + (function == null ? "" : function.toString()) + "}");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (sameClassAs(o) == false) {
            return false;
        }
        ScriptScoreQuery other = (ScriptScoreQuery) o;
        return Objects.equals(this.subQuery, other.subQuery)  &&
            Objects.equals(this.minScore, other.minScore) &&
            Objects.equals(this.function, other.function);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), subQuery, minScore, function);
    }
}
