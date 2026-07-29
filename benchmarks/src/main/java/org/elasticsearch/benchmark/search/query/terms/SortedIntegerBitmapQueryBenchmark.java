/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.search.query.terms;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.query.bitmapterms.IntBitmapIndexBKDQuery;
import org.elasticsearch.index.query.bitmapterms.IntBitmapIndexTermsQuery;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * Measures {@code bitmap_terms} execution on an index whose primary sort is the queried integer
 * field, for both indexing layouts: BKD points ({@link IntBitmapIndexBKDQuery}) and the
 * {@code index_terms} inverted index ({@link IntBitmapIndexTermsQuery}).
 * <p>
 * <b>Why a separate benchmark from {@link IntegerTermsQueryBenchmark}.</b> That benchmark varies
 * {@code nTerms} — the bitmap's <em>cardinality</em>. Under index sort, execution cost is instead
 * governed by the bitmap's <em>run count</em> R: doc order equals value order, so a run of
 * consecutive values covers a contiguous doc-id interval, and the matching set is a union of R
 * intervals rather than a set of C individual values. Cardinality alone therefore cannot show
 * whether a sort-aware execution strategy helps, so this benchmark varies R directly via
 * {@link #runLength} at each {@link #cardinality} — the two together span R from 1 to 1,000,000.
 * <p>
 * It also splits measurement into four modes, because the relevant strategies trade off against each
 * other and a single {@code search()} number averages the win and the regression together:
 * <ul>
 *   <li>{@link #topN} — {@code search(q, 10)}: benefits from lazy iteration / early termination.</li>
 *   <li>{@link #count} — {@code searcher.count(q)}: benefits from a {@code Weight#count()} fast path
 *       (a union of intervals has an exact size without iterating docs).</li>
 *   <li>{@link #collectAll} — drives the {@link DocIdSetIterator} directly, bypassing any collector
 *       count shortcut. This is the <em>regression canary</em>: strategies that stream doc values
 *       instead of sweeping the BKD tree can lose here on small filters, so the matrix deliberately
 *       reaches down to a 100-value bitmap.</li>
 *   <li>{@link #leadFilter} — the bitmap as a non-leading conjunction clause behind a selective
 *       filter, which exercises {@code advance()} rather than {@code nextDoc()}.</li>
 * </ul>
 * <p>
 * <b>Index layout.</b> Docs are written in ascending field order with {@code value = docId /
 * docsPerValue}, so {@code docsPerValue} sets {@code docFreq} — every value is carried by exactly
 * that many docs, and the bitmap matches {@code cardinality * docsPerValue} docs. That axis matters
 * for the {@code index_terms} path specifically, where per-term postings decoding — not term lookup —
 * dominates once {@code docFreq > 1}. A second, independent {@code lead} field supplies the selective
 * clause for {@link #leadFilter} without correlating with the sort.
 * <p>
 * <b>The {@code *_UNMARKED} control strategies</b> write byte-identical documents but omit
 * {@link IndexWriterConfig#setIndexSort}, so {@code reader.getMetaData().sort()} is null and no
 * sort-aware path can engage. Because the physical layout is unchanged, the delta against the
 * corresponding {@code *_SORTED} strategy isolates the code path with no locality or compression
 * confound. Note this is <em>not</em> a model of a genuinely unsorted index (which would have
 * randomly distributed values and far worse locality); it exists to isolate the optimization, not to
 * estimate what index sorting buys. They are off by default — see the {@code strategy} param.
 * <p>
 * <b>Intended use across two commits.</b> This harness is written so that adding a sort-aware
 * execution strategy requires <em>no change to this file</em>: the strategies construct
 * {@link IntBitmapIndexBKDQuery} / {@link IntBitmapIndexTermsQuery} through their public
 * constructors, and any sort detection belongs inside those classes. Running the identical harness on
 * the commit before and after the optimization is what makes the comparison attributable.
 * {@code setupTrial} cross-checks the analytic match count against both full iteration and
 * {@code searcher.count()}, so a strategy that is fast because it is wrong fails the run instead of
 * posting a good score.
 * <p>
 * <b>Runtime.</b> The default matrix is 2 strategies x 2 {@code docsPerValue} x 4
 * {@code cardinality} x 3 {@code runLength} = 48 forks per benchmark method. Iteration time is
 * shortened to 1s (queries here run in microseconds to low milliseconds, so 1s still yields ample
 * samples). Narrow the sweep while iterating with JMH's {@code -p} flag, e.g.
 * {@code -p runLength=1,4096 -p docsPerValue=1}. Each distinct (layout, sort marker, docsPerValue)
 * combination builds and force-merges a persistent 10M-doc index on first use under
 * {@code tests.index}; later forks reuse it.
 */
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
@SuppressWarnings("unused") // invoked by JMH
public class SortedIntegerBitmapQueryBenchmark {

    static {
        // IndexWriterConfig below picks up Elasticsearch's codec via Lucene's codec SPI (since
        // :server is on the classpath), whose static init reaches Elasticsearch's log4j config. That
        // config's %node_name converter throws unless a node name was set, which never happens in a
        // bare JMH JVM. Set a dummy one before anything can trigger that lookup.
        //
        // LogConfigurator#setNodeName is backed by a SetOnce and throws if called twice, so this class
        // must not touch another benchmark's static members — hence its own FIELD/N_DOCS constants and
        // its own copy of the index_terms field construction.
        LogConfigurator.setNodeName("benchmark");
    }

    /** The sorted integer field under test. */
    static final String FIELD = "f";

    /** Field carrying the selective clause for {@link #leadFilter}; independent of the sort field. */
    static final String LEAD_FIELD = "lead";

    /**
     * Distinct values of {@link #LEAD_FIELD}. An exact query on one of them matches
     * {@code N_DOCS / LEAD_CARDINALITY} docs, i.e. 0.1% of the index, scattered uniformly so the lead
     * iterator does not degenerate into a contiguous doc range under the index sort.
     */
    static final int LEAD_CARDINALITY = 1000;

    static final int N_DOCS = 10_000_000;

    static final int TOP_N = 10;

    private static final FieldType INDEX_TERMS_FIELD_TYPE;
    static {
        INDEX_TERMS_FIELD_TYPE = new FieldType();
        INDEX_TERMS_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        INDEX_TERMS_FIELD_TYPE.setDocValuesType(DocValuesType.SORTED_NUMERIC);
        INDEX_TERMS_FIELD_TYPE.setOmitNorms(true);
        INDEX_TERMS_FIELD_TYPE.setTokenized(false);
        INDEX_TERMS_FIELD_TYPE.freeze();
    }

    /**
     * Mirrors {@code NumberFieldMapper.IndexTermsIntegerField}: sortable-bytes term plus numeric doc
     * value, and no BKD points — what an {@code integer} field mapped with {@code index_terms: true}
     * produces. The mapper's version is not reachable from this module, so it is reproduced here and
     * must be kept in sync if the mapper changes.
     * <p>
     * {@link IntegerTermsQueryBenchmark} carries an identical copy. Do <em>not</em> deduplicate the two
     * by having one benchmark reference the other's members: that triggers the referenced benchmark's
     * static initializer, and both call
     * {@link org.elasticsearch.common.logging.LogConfigurator#setNodeName} — which is backed by a
     * {@code SetOnce} and throws {@code AlreadySetException} on the second call, failing index
     * construction at class-init time.
     */
    private static final class IndexTermsIntegerField extends Field {
        private final int numericVal;

        IndexTermsIntegerField(String name, int value) {
            super(name, encodeIndexTerm(value), INDEX_TERMS_FIELD_TYPE);
            this.numericVal = value;
        }

        @Override
        public Number numericValue() {
            return numericVal;
        }
    }

    /**
     * Mirrors {@code NumberFieldMapper#encodeIndexTerm}: encodes {@code value} as the same
     * sortable-bytes term {@link IntPoint} uses for BKD points, so unsigned byte-wise term order
     * matches numeric order (see {@link NumericUtils#intToSortableBytes}).
     */
    static BytesRef encodeIndexTerm(int value) {
        byte[] bytes = new byte[Integer.BYTES];
        NumericUtils.intToSortableBytes(value, bytes, 0);
        return new BytesRef(bytes);
    }

    /**
     * Builds a bitmap of about {@code cardinality} values arranged as contiguous runs of
     * {@code runLength}, spread evenly across {@code [0, distinctValues)}.
     * <p>
     * R = {@code cardinality / runLength} is the quantity a sort-aware strategy scales with, so
     * holding cardinality fixed and raising {@code runLength} lowers R without materially changing
     * how many docs match. Runs never merge: the stride between run starts is
     * {@code distinctValues * runLength / cardinality}, which is >= {@code runLength} whenever
     * {@code cardinality <= distinctValues}. Integer division means the realised cardinality can be
     * lower than requested when {@code runLength} does not divide it; callers must read the actual
     * value back off the returned bitmap rather than assume the request was honoured.
     * <p>
     * {@code runOptimize()} mirrors what a client is expected to do before serializing, and makes the
     * run structure explicit in the container encoding rather than leaving it implicit in an array
     * container.
     */
    static RoaringBitmap runStructuredBitmap(int distinctValues, int cardinality, int runLength) {
        if (cardinality > distinctValues) {
            throw new IllegalArgumentException("cardinality [" + cardinality + "] exceeds distinct values [" + distinctValues + "]");
        }
        int runs = Math.max(1, cardinality / runLength);
        long stride = (long) distinctValues / runs;
        RoaringBitmap bitmap = new RoaringBitmap();
        for (int run = 0; run < runs; run++) {
            long start = run * stride;
            bitmap.add(start, Math.min(start + runLength, distinctValues));
        }
        bitmap.runOptimize();
        return bitmap;
    }

    /**
     * How the field is indexed and whether the segment is marked as sorted by it.
     */
    public enum Strategy {
        /** BKD points plus doc values (default {@code integer} mapping), segment marked sorted. */
        BITMAP_BKD_SORTED(true, true),
        /** {@code index_terms} inverted index plus doc values, segment marked sorted. */
        BITMAP_TERMS_SORTED(false, true),
        /** Control: identical layout to {@link #BITMAP_BKD_SORTED} with no index-sort marker. */
        BITMAP_BKD_UNMARKED(true, false),
        /** Control: identical layout to {@link #BITMAP_TERMS_SORTED} with no index-sort marker. */
        BITMAP_TERMS_UNMARKED(false, false);

        private final boolean points;
        private final boolean sorted;

        Strategy(boolean points, boolean sorted) {
            this.points = points;
            this.sorted = sorted;
        }

        void addField(Document doc, int value) {
            if (points) {
                doc.add(new IntField(FIELD, value, Field.Store.NO));
            } else {
                doc.add(new IndexTermsIntegerField(FIELD, value));
            }
        }

        Query bitmapQuery(RoaringBitmap bitmap) {
            return points ? new IntBitmapIndexBKDQuery(FIELD, bitmap) : new IntBitmapIndexTermsQuery(FIELD, bitmap);
        }

        boolean sorted() {
            return sorted;
        }

        /**
         * Directory name. The sort marker changes segment metadata, so marked and unmarked variants
         * cannot share a directory even though their documents are identical.
         */
        String indexDir() {
            return (points ? "POINT" : "INDEX_TERMS") + (sorted ? "-SORTED" : "-UNMARKED");
        }
    }

    /**
     * Controls are omitted by default: the primary experiment is the same strategy before and after a
     * sort-aware implementation lands, not sorted vs. unmarked. Enable them with
     * {@code -p strategy=BITMAP_BKD_SORTED,BITMAP_BKD_UNMARKED,...}.
     */
    @Param({ "BITMAP_BKD_SORTED", "BITMAP_TERMS_SORTED" })
    public Strategy strategy;

    /** Docs sharing each field value, i.e. {@code docFreq}. Drives per-term postings cost. */
    @Param({ "1", "100" })
    public int docsPerValue;

    /**
     * Number of values in the bitmap, i.e. how many ids the caller is filtering by. The low end
     * matters: a lazy doc-values strategy is most at risk of losing to an eager BKD sweep on small
     * filters, so the regression canary needs 10- and 100-value bitmaps, while the high end is where
     * {@code bitmap_terms} is the intended tool at all (past {@code index.max_terms_count}, 65536).
     * <p>
     * Constrained by {@code cardinality * docsPerValue <= N_DOCS}; combinations that would exceed the
     * available distinct values are clamped, so the largest cardinality collapses onto
     * {@code N_DOCS / docsPerValue}. The effective figure is printed per trial.
     */
    @Param({ "10", "100", "10000", "1000000" })
    public int cardinality;

    /** Length of each contiguous value run. Higher means fewer runs (lower R) for the same cardinality. */
    @Param({ "1", "64", "4096" })
    public int runLength;

    private Directory directory;
    private DirectoryReader reader;
    private IndexSearcher searcher;
    private Query bitmapQuery;
    private Query leadFilterQuery;
    private Weight bitmapWeight;

    @Setup(Level.Trial)
    public void setupTrial() throws IOException {
        // @Fork(1) restarts the JVM per param combination, so nothing survives in memory between
        // trials. Persist the index on disk keyed by everything that affects its bytes, so only the
        // first trial for a given key pays to build and force-merge it.
        Path path = Path.of(System.getProperty("tests.index"), strategy.indexDir() + "-dpv" + docsPerValue + "-" + N_DOCS);
        directory = FSDirectory.open(path);
        if (DirectoryReader.indexExists(directory) == false) {
            buildIndex();
        }
        reader = DirectoryReader.open(directory);
        searcher = new IndexSearcher(reader);
        // Otherwise every iteration after the first measures a query-cache hit.
        searcher.setQueryCache(null);

        // A failed or interrupted build can leave a readable but incomplete index behind, and a stale
        // directory from an earlier run may not match today's parameters. Either would silently turn
        // every measurement below into a query over the wrong data.
        if (reader.maxDoc() != N_DOCS) {
            throw new IllegalStateException(
                "index at [" + path + "] has maxDoc [" + reader.maxDoc() + "], expected [" + N_DOCS + "]; delete it and rerun"
            );
        }
        // If the sort marker is missing, no sort-aware path can engage and the run would report "no
        // improvement" for a reason that has nothing to do with the optimization.
        for (LeafReaderContext context : reader.leaves()) {
            Sort sort = context.reader().getMetaData().sort();
            boolean sortedByField = sort != null && sort.getSort().length > 0 && FIELD.equals(sort.getSort()[0].getField());
            if (sortedByField != strategy.sorted()) {
                throw new IllegalStateException(
                    "segment sort marker ["
                        + sort
                        + "] does not match strategy ["
                        + strategy
                        + "] for index ["
                        + path
                        + "]; delete it and rerun"
                );
            }
        }

        int distinctValues = N_DOCS / docsPerValue;
        // JMH has no way to express conditional params, so invalid combinations are clamped rather
        // than skipped: a bitmap cannot hold more values than the index has, and a run cannot be
        // longer than the bitmap. Some cells therefore collapse onto the same effective shape; the
        // line printed below records what was actually measured.
        int effectiveCardinality = Math.min(cardinality, distinctValues);
        int effectiveRunLength = Math.min(runLength, effectiveCardinality);

        RoaringBitmap bitmap = runStructuredBitmap(distinctValues, effectiveCardinality, effectiveRunLength);
        bitmapQuery = strategy.bitmapQuery(bitmap);
        leadFilterQuery = new BooleanQuery.Builder().add(IntPoint.newExactQuery(LEAD_FIELD, 0), BooleanClause.Occur.FILTER)
            .add(bitmapQuery, BooleanClause.Occur.FILTER)
            .build();
        // Reused across invocations: creating a Weight is cheap and stateless here, while
        // scorerSupplier()/get() — where a strategy does its real work — stays inside the measured
        // region of collectAll().
        bitmapWeight = searcher.createWeight(searcher.rewrite(bitmapQuery), ScoreMode.COMPLETE_NO_SCORES, 1f);

        verifyMatches(bitmap);

        // Effective parameters are needed to interpret results, and to confirm that a before/after
        // comparison ran on identical inputs.
        System.out.println(
            "# bitmap: distinctValues="
                + distinctValues
                + " cardinality="
                + bitmap.getCardinality()
                + " runLength="
                + effectiveRunLength
                + " runs="
                + Math.max(1, bitmap.getCardinality() / effectiveRunLength)
                + " matchedDocs="
                + (long) bitmap.getCardinality() * docsPerValue
                + " serializedBytes="
                + bitmap.serializedSizeInBytes()
        );
    }

    /**
     * Cross-checks three independent views of the result set: the analytic count implied by the
     * bitmap and {@code docsPerValue}, full iteration of the scorer, and {@code searcher.count()}.
     * <p>
     * Every value the generator emits is below {@code distinctValues} and each such value has exactly
     * {@code docsPerValue} docs, so the analytic figure is exact rather than approximate. Comparing
     * iteration against {@code count()} additionally catches a {@code Weight#count()} fast path that
     * disagrees with the iterator — the most likely way a sort-aware optimization goes wrong while
     * looking fast.
     */
    private void verifyMatches(RoaringBitmap bitmap) throws IOException {
        long expected = (long) bitmap.getCardinality() * docsPerValue;
        long iterated = collectAll();
        if (iterated != expected) {
            throw new IllegalStateException("iterated [" + iterated + "] matches, expected [" + expected + "] for " + strategy);
        }
        long counted = searcher.count(bitmapQuery);
        if (counted != expected) {
            throw new IllegalStateException("count() returned [" + counted + "], expected [" + expected + "] for " + strategy);
        }
    }

    private void buildIndex() throws IOException {
        IndexWriterConfig config = new IndexWriterConfig(null);
        if (strategy.sorted()) {
            config.setIndexSort(new Sort(new SortedNumericSortField(FIELD, SortField.Type.INT)));
        }
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int docId = 0; docId < N_DOCS; docId++) {
                Document doc = new Document();
                strategy.addField(doc, docId / docsPerValue);
                doc.add(new IntField(LEAD_FIELD, docId % LEAD_CARDINALITY, Field.Store.NO));
                writer.addDocument(doc);
            }
            // Single segment: removes segment-count variance from the comparison. Real indices have
            // several segments, so absolute numbers here are optimistic for every strategy alike.
            writer.forceMerge(1);
        }
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() throws IOException {
        if (reader != null) {
            reader.close();
        }
        if (directory != null) {
            directory.close();
        }
    }

    /** Top-N retrieval: rewards lazy iteration that can stop before consuming the whole match set. */
    @Benchmark
    public TopDocs topN() throws IOException {
        return searcher.search(bitmapQuery, TOP_N);
    }

    /** Exhaustive count: rewards a {@code Weight#count()} fast path that avoids iterating docs. */
    @Benchmark
    public int count() throws IOException {
        return searcher.count(bitmapQuery);
    }

    /**
     * Full iteration of the matching doc set, driving the iterator directly so no collector count
     * shortcut can hide the cost. The canary for strategies that trade sweep throughput for laziness.
     */
    @Benchmark
    public long collectAll() throws IOException {
        long matched = 0;
        for (LeafReaderContext context : reader.leaves()) {
            ScorerSupplier supplier = bitmapWeight.scorerSupplier(context);
            if (supplier == null) {
                continue;
            }
            DocIdSetIterator iterator = supplier.get(Long.MAX_VALUE).iterator();
            for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
                matched++;
            }
        }
        return matched;
    }

    /**
     * The bitmap as a non-leading conjunction clause behind a 0.1%-selective filter. Counted rather
     * than top-N so the whole conjunction is evaluated, making this sensitive to {@code advance()}
     * cost — the access pattern a lazy strategy must not regress.
     */
    @Benchmark
    public int leadFilter() throws IOException {
        return searcher.count(leadFilterQuery);
    }
}
