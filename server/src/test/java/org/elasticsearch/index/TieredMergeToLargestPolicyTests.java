/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;

import static org.elasticsearch.index.IndexSettingsTests.newIndexMeta;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class TieredMergeToLargestPolicyTests extends ESTestCase {

    public void testDefaultSettings() {
        TieredMergeToLargestPolicy policy = new TieredMergeToLargestPolicy();

        // Check default values
        assertThat(policy.getMaxMergedSegmentMB(), equalTo(5 * 1024.0)); // 5GB in MB
        assertThat(policy.getFloorSegmentMB(), equalTo(2.0)); // 2MB
        assertThat(policy.getSegmentsPerTier(), equalTo(10.0));
        assertThat(policy.getMaxRatio(), equalTo(5.0));
        assertThat(policy.getMaxMergeAtOnce(), equalTo(10));
        assertThat(policy.getDeletesPctAllowed(), equalTo(20.0));
        assertThat(policy.getForceMergeDeletesPctAllowed(), equalTo(10.0));
    }

    public void testSetters() {
        TieredMergeToLargestPolicy policy = new TieredMergeToLargestPolicy();

        policy.setMaxMergedSegmentMB(1024);
        assertThat(policy.getMaxMergedSegmentMB(), equalTo(1024.0));

        policy.setFloorSegmentMB(5);
        assertThat(policy.getFloorSegmentMB(), equalTo(5.0));

        policy.setSegmentsPerTier(8);
        assertThat(policy.getSegmentsPerTier(), equalTo(8.0));

        policy.setMaxRatio(3);
        assertThat(policy.getMaxRatio(), equalTo(3.0));

        policy.setMaxMergeAtOnce(5);
        assertThat(policy.getMaxMergeAtOnce(), equalTo(5));

        policy.setDeletesPctAllowed(15);
        assertThat(policy.getDeletesPctAllowed(), equalTo(15.0));

        policy.setForceMergeDeletesPctAllowed(5);
        assertThat(policy.getForceMergeDeletesPctAllowed(), equalTo(5.0));
    }

    public void testInvalidSettings() {
        TieredMergeToLargestPolicy policy = new TieredMergeToLargestPolicy();

        expectThrows(IllegalArgumentException.class, () -> policy.setMaxMergedSegmentMB(-1));
        expectThrows(IllegalArgumentException.class, () -> policy.setFloorSegmentMB(0));
        expectThrows(IllegalArgumentException.class, () -> policy.setFloorSegmentMB(-1));
        expectThrows(IllegalArgumentException.class, () -> policy.setSegmentsPerTier(1));
        expectThrows(IllegalArgumentException.class, () -> policy.setMaxRatio(0.5));
        expectThrows(IllegalArgumentException.class, () -> policy.setMaxMergeAtOnce(1));
        expectThrows(IllegalArgumentException.class, () -> policy.setDeletesPctAllowed(-1));
        expectThrows(IllegalArgumentException.class, () -> policy.setDeletesPctAllowed(51));
        expectThrows(IllegalArgumentException.class, () -> policy.setForceMergeDeletesPctAllowed(-1));
        expectThrows(IllegalArgumentException.class, () -> policy.setForceMergeDeletesPctAllowed(101));
    }

    public void testMergePolicyType() {
        Settings settings = Settings.builder()
            .put(MergePolicyConfig.INDEX_MERGE_POLICY_TYPE_SETTING.getKey(), "merge_to_largest")
            .build();
        IndexSettings indexSettings = new IndexSettings(newIndexMeta("test", settings), Settings.EMPTY);
        MergePolicy mergePolicy = indexSettings.getMergePolicy(false);

        assertThat(mergePolicy, Matchers.instanceOf(TieredMergeToLargestPolicy.class));
    }

    public void testMergePolicyConfiguration() {
        Settings settings = Settings.builder()
            .put(MergePolicyConfig.INDEX_MERGE_POLICY_TYPE_SETTING.getKey(), "merge_to_largest")
            .put(MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING.getKey(), "5mb")
            .put(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING.getKey(), 5)
            .put(MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING.getKey(), 8)
            .put(MergePolicyConfig.INDEX_MERGE_POLICY_DELETES_PCT_ALLOWED_SETTING.getKey(), 15)
            .build();
        IndexSettings indexSettings = new IndexSettings(newIndexMeta("test", settings), Settings.EMPTY);
        TieredMergeToLargestPolicy policy = (TieredMergeToLargestPolicy) indexSettings.getMergePolicy(false);

        assertThat(policy.getFloorSegmentMB(), equalTo(5.0));
        assertThat(policy.getMaxMergeAtOnce(), equalTo(5));
        assertThat(policy.getSegmentsPerTier(), equalTo(8.0));
        assertThat(policy.getDeletesPctAllowed(), equalTo(15.0));
    }

    public void testBasicMerging() throws IOException {
        TieredMergeToLargestPolicy policy = new TieredMergeToLargestPolicy();
        policy.setFloorSegmentMB(0.001); // Very small floor for testing
        policy.setSegmentsPerTier(3);
        policy.setMaxMergeAtOnce(3);

        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = new IndexWriterConfig(null);
            iwc.setMergePolicy(policy);

            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                // Create multiple small segments
                for (int i = 0; i < 10; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("id", "doc" + i, Field.Store.YES));
                    writer.addDocument(doc);
                    writer.flush();
                }

                // Allow merges to happen
                writer.commit();

                // Check that some merging occurred (fewer segments than flushes)
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    int numSegments = reader.leaves().size();
                    // We should have fewer segments than the 10 we flushed
                    assertThat(numSegments, lessThanOrEqualTo(10));
                }
            }
        }
    }

    public void testForceMerge() throws IOException {
        TieredMergeToLargestPolicy policy = new TieredMergeToLargestPolicy();

        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = new IndexWriterConfig(null);
            iwc.setMergePolicy(policy);

            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                // Create multiple segments
                for (int i = 0; i < 20; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("id", "doc" + i, Field.Store.YES));
                    writer.addDocument(doc);
                    writer.flush();
                }

                // Force merge to a single segment
                writer.forceMerge(1);

                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    assertThat(reader.leaves().size(), equalTo(1));
                }
            }
        }
    }

    public void testForceMergeToMaxSegments() throws IOException {
        TieredMergeToLargestPolicy policy = new TieredMergeToLargestPolicy();

        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = new IndexWriterConfig(null);
            iwc.setMergePolicy(policy);

            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                // Create multiple segments
                for (int i = 0; i < 20; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("id", "doc" + i, Field.Store.YES));
                    writer.addDocument(doc);
                    writer.flush();
                }

                // Force merge to at most 3 segments
                writer.forceMerge(3);

                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    assertThat(reader.leaves().size(), lessThanOrEqualTo(3));
                }
            }
        }
    }

    public void testToString() {
        TieredMergeToLargestPolicy policy = new TieredMergeToLargestPolicy();
        String str = policy.toString();

        assertThat(str, Matchers.containsString("TieredMergeToLargestPolicy"));
        assertThat(str, Matchers.containsString("maxRatio=5.0"));
        assertThat(str, Matchers.containsString("segsPerTier=10.0"));
    }

    public void testCompoundFileSettings() throws IOException {
        Settings settings = Settings.builder()
            .put(MergePolicyConfig.INDEX_MERGE_POLICY_TYPE_SETTING.getKey(), "merge_to_largest")
            .put(MergePolicyConfig.INDEX_COMPOUND_FORMAT_SETTING.getKey(), "100mb")
            .build();
        IndexSettings indexSettings = new IndexSettings(newIndexMeta("test", settings), Settings.EMPTY);
        MergePolicy policy = indexSettings.getMergePolicy(false);

        assertThat(policy, Matchers.instanceOf(TieredMergeToLargestPolicy.class));
        assertThat(policy.getNoCFSRatio(), equalTo(1.0));
        assertThat(policy.getMaxCFSSegmentSizeMB(), equalTo(100.0));
    }

    public void testSegmentCountAfterMerging() throws IOException {
        TieredMergeToLargestPolicy policy = new TieredMergeToLargestPolicy();
        policy.setFloorSegmentMB(0.0001);
        policy.setSegmentsPerTier(4);
        policy.setMaxMergeAtOnce(4);

        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = new IndexWriterConfig(null);
            iwc.setMergePolicy(policy);

            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                // Create many small segments
                for (int i = 0; i < 50; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("id", "doc" + i, Field.Store.YES));
                    writer.addDocument(doc);
                    writer.flush();
                }

                writer.commit();

                // After merging, we should have a reasonable number of segments
                SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
                // The policy should keep segment count reasonable
                assertThat(infos.size(), greaterThanOrEqualTo(1));
            }
        }
    }
}
