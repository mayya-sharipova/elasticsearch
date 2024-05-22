/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.StandardTokenizerFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.BufferedReader;
import java.io.Reader;
import java.io.StringReader;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

public class SynonymsLoadingFromIndexTests extends ESSingleNodeTestCase {

    private static final int NUMBER_OF_RULES = 44722;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.getPlugins());
        plugins.add(MapperExtrasPlugin.class);
        return plugins;
    }

    public void testSynonymsLoadingFromIndexSingleDoc() throws Exception {
        final String indexName = "synonyms1";
        final String synonymSetID = "synonyms_set1";
        final String synonymsField = "synonyms";

        Settings settings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build();
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(synonymsField)
            .field("type", "object")
            .field("enabled", "false")
            .endObject()
            .endObject()
            .endObject();
        createIndex(indexName, settings, builder);
        BytesReference source = Streams.readFully(getClass().getResourceAsStream("synonyms1.json"));
        client().prepareIndex(indexName).setId(synonymSetID).setSource(source, XContentType.JSON).get();

        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        long memoryUsedStart = memoryBean.getHeapMemoryUsage().getUsed();
        long startTime = System.currentTimeMillis();

        GetResponse response = client().prepareGet(indexName, synonymSetID).get();
        assertTrue(response.isExists());

        @SuppressWarnings("unchecked")
        List<String> rulesList = (List<String>) response.getSource().get(synonymsField);
        assertEquals(NUMBER_OF_RULES, rulesList.size());
        StringBuilder sb = new StringBuilder();
        for (String line : rulesList) {
            sb.append(line).append(System.lineSeparator());
        }
        Reader synonymsReader = new StringReader(sb.toString());

        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("test_index", settings);
        TokenizerFactory tokenizerFactory = new StandardTokenizerFactory(idxSettings, node().getEnvironment(), "standard", settings);
        SynonymTokenFilterFactory synonymFactory = new SynonymTokenFilterFactory(idxSettings, node().getEnvironment(), "synonym", settings, null);
        Analyzer analyzer = SynonymTokenFilterFactory.buildSynonymAnalyzer(
            tokenizerFactory,
            Collections.emptyList(),
            Collections.emptyList()
        );
        SynonymMap synonymMap = synonymFactory.buildSynonyms(
            analyzer,
            new SynonymTokenFilterFactory.ReaderWithOrigin(synonymsReader, indexName)
        );
        assertEquals(NUMBER_OF_RULES, synonymMap.words.size());

        long endTime = System.currentTimeMillis();
        logger.info("Elapsed time: {} ms", (endTime - startTime));
        long memoryUsedEnd = memoryBean.getHeapMemoryUsage().getUsed();
        logger.info("Memory used: [{}] bytes", (memoryUsedEnd - memoryUsedStart));
    }

    public void testSynonymsLoadingFromIndexMultipleDocs() throws Exception {
        final String indexName = "synonyms1";
        final String synonymSetID = "synonyms_set1";
        final String synonymSetField = "synonyms_set";
        final String synonymsField = "synonyms";
        final String idField = "id";
        final String typeField = "type";
        final int numberOfRules = 100_000;

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(), numberOfRules)
            .build();
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(synonymSetField)
            .field("type", "keyword")
            .endObject()
            .startObject(synonymsField)
            .field("type", "match_only_text")
            .endObject()
            .startObject(idField)
            .field("type", "keyword")
            .endObject()
            .startObject(typeField)
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject();
        createIndex(indexName, settings, builder);

        final Path path = PathUtils.get(getClass().getResource("synonyms3.json").toURI());
        BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
        BulkRequestBuilder bulkBuilder = client().prepareBulk();
        String line;
        int docNum = 0;
        while ((line = reader.readLine()) != null) {
            bulkBuilder.add(client().prepareIndex(indexName).setSource(line, XContentType.JSON));
            docNum++;
            if (docNum % 10_000 == 0) {
                BulkResponse bulkResponse = bulkBuilder.get();
                assertNoFailures(bulkResponse);
                bulkBuilder = client().prepareBulk();
            }
        }
        assertEquals(numberOfRules, docNum);
        if (bulkBuilder.numberOfActions() > 0) {
            BulkResponse bulkResponse = bulkBuilder.get();
            assertNoFailures(bulkResponse);
        }
        reader.close();

        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        long memoryUsedStart = memoryBean.getHeapMemoryUsage().getUsed();
        long startTime = System.currentTimeMillis();

        client().admin().indices().prepareRefresh().get();
        SearchResponse response = client().prepareSearch(indexName)
            .setQuery(boolQuery().must(termQuery(synonymSetField, synonymSetID)).filter(termQuery(typeField, "synonym_rule")))
            .setSize(numberOfRules)
            .addSort("id", SortOrder.ASC)
            .setTrackTotalHits(true)
            .get();
        assertEquals(numberOfRules, response.getHits().getTotalHits().value);
        assertEquals(numberOfRules, response.getHits().getHits().length);

        StringBuilder sb = new StringBuilder();
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String rule = (String) sourceAsMap.get(synonymsField);
            sb.append(rule).append(System.lineSeparator());
        }
        Reader synonymsReader = new StringReader(sb.toString());

        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("test_index", settings);
        TokenizerFactory tokenizerFactory = new StandardTokenizerFactory(idxSettings, node().getEnvironment(), "standard", settings);
        SynonymTokenFilterFactory synonymFactory = new SynonymTokenFilterFactory(idxSettings, node().getEnvironment(), "synonym", settings, null);
        Analyzer analyzer = SynonymTokenFilterFactory.buildSynonymAnalyzer(
            tokenizerFactory,
            Collections.emptyList(),
            Collections.emptyList()
        );
        SynonymMap synonymMap = synonymFactory.buildSynonyms(
            analyzer,
            new SynonymTokenFilterFactory.ReaderWithOrigin(synonymsReader, indexName)
        );
        assertEquals(numberOfRules * 2, synonymMap.words.size());

        long endTime = System.currentTimeMillis();
        logger.info("Elapsed time: {} ms", (endTime - startTime));
        long memoryUsedEnd = memoryBean.getHeapMemoryUsage().getUsed();
        logger.info("Memory used: [{}] bytes", (memoryUsedEnd - memoryUsedStart));
    }

}
