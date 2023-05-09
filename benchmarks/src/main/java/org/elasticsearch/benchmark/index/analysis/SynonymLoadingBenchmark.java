/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.elasticsearch.Version;
import org.elasticsearch.analysis.common.ESSolrSynonymParser;
import org.elasticsearch.analysis.common.SynonymTokenFilterFactory;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.StandardTokenizerFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

@Fork(value = 1)
@Warmup(iterations = 1)
@Measurement(iterations = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class SynonymLoadingBenchmark {

    private abstract static class BenchmarkFunction {
        abstract void execute() throws IOException;
    }

    private class LoadFromFileBenchmarkFunction extends BenchmarkFunction {
        @Override
        void execute() throws IOException {
            String synonymsFile = SynonymLoadingBenchmark.class.getResource("synonyms.txt").getPath();
            final Path path = Paths.get(synonymsFile);
            Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
            final SynonymMap synonyms = buildSynonyms(analyzer, reader);
        }
    }

    private static SynonymMap buildSynonyms(Analyzer analyzer, Reader reader) {
        try {
            SynonymMap.Builder parser = new ESSolrSynonymParser(true, true, false, analyzer);
            ((ESSolrSynonymParser) parser).parse(reader);
            return parser.build();
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to build synonyms", e);
        }
    }

    private BenchmarkFunction benchmarkFunction;
    private Analyzer analyzer;

    @Setup
    public void setUp() {
        benchmarkFunction = new LoadFromFileBenchmarkFunction();

        Settings settings = Settings.builder()
            .put("index.version.created", Version.CURRENT)
            .put("index.number_of_replicas", 0)
            .put("index.number_of_shards", 1)
            .build();
        IndexMetadata meta = IndexMetadata.builder("index").settings(settings).build();
        IndexSettings idxSettings = new IndexSettings(meta, settings);
        TokenizerFactory tok = new StandardTokenizerFactory(idxSettings, null, "standard", idxSettings.getSettings());
        analyzer = SynonymTokenFilterFactory.buildSynonymAnalyzer(tok, Collections.emptyList(), Collections.emptyList());
    }

    @Benchmark
    public void benchmark() throws IOException {
        benchmarkFunction.execute();
    }
}
