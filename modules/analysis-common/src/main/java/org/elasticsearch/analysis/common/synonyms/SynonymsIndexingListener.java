/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common.synonyms;

import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;

public class SynonymsIndexingListener implements IndexingOperationListener {

    @Override
    public Engine.Index preIndex(ShardId shardId, Engine.Index operation) {
        // TODO: dry-run across all analyzers that use this synonyms set
        // fail operation if any analyzer fails
        return IndexingOperationListener.super.preIndex(shardId, operation);
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Exception ex) {
        // reload all analyzers
        IndexingOperationListener.super.postIndex(shardId, index, ex);
    }

    @Override
    public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {
        // TODO: dry-run across all analyzers that use this synonyms set
        // fail operation if any analyzer fails
        // Is the dry run necessary?
        IndexingOperationListener.super.postDelete(shardId, delete, result);
    }

    @Override
    public void postDelete(ShardId shardId, Engine.Delete delete, Exception ex) {
        // reload all analyzers
        IndexingOperationListener.super.postDelete(shardId, delete, ex);
    }
}
