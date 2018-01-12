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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.action.admin.indices.AliasesNotFoundException;

import java.util.List;

/**
 * Individual operation to perform on the cluster state as part of an {@link IndicesAliasesRequest}.
 */
public abstract class AliasAction {
    private final String index;

    private AliasAction(String index) {
        if (false == Strings.hasText(index)) {
            throw new IllegalArgumentException("[index] is required");
        }
        this.index = index;
    }

    /**
     * Get the index on which the operation should act.
     */
    public String getIndex() {
        return index;
    }

    /**
     * Should this action remove the index? Actions that return true from this will never execute
     * {@link #apply(NewAliasValidator, MetaData.Builder, IndexMetaData)}.
     */
    abstract boolean removeIndex();

    /**
     * Apply the action.
     *
     * @param aliasValidator call to validate a new alias before adding it to the builder
     * @param metadata metadata builder for the changes made by all actions as part of this request
     * @param index metadata for the index being changed
     * @return did this action make any changes?
     */
    abstract boolean apply(NewAliasValidator aliasValidator, MetaData.Builder metadata, IndexMetaData index);

    abstract String getAlias();

    /**
     * Validate a new alias.
     */
    @FunctionalInterface
    public interface NewAliasValidator {
        void validate(String alias, @Nullable String indexRouting, @Nullable String filter);
    }

    /**
     * Operation to add an alias to an index.
     */
    public static class Add extends AliasAction {
        private final String alias;

        @Nullable
        private final String filter;

        @Nullable
        private final String indexRouting;

        @Nullable
        private final String searchRouting;

        /**
         * Build the operation.
         */
        public Add(String index, String alias, @Nullable String filter, @Nullable String indexRouting, @Nullable String searchRouting) {
            super(index);
            if (false == Strings.hasText(alias)) {
                throw new IllegalArgumentException("[alias] is required");
            }
            this.alias = alias;
            this.filter = filter;
            this.indexRouting = indexRouting;
            this.searchRouting = searchRouting;
        }

        /**
         * Alias to add to the index.
         */
        @Override
        public String getAlias() {
            return alias;
        }

        @Override
        boolean removeIndex() {
            return false;
        }

        @Override
        boolean apply(NewAliasValidator aliasValidator, MetaData.Builder metadata, IndexMetaData index) {
            aliasValidator.validate(alias, indexRouting, filter);
            AliasMetaData newAliasMd = AliasMetaData.newAliasMetaDataBuilder(alias).filter(filter).indexRouting(indexRouting)
                    .searchRouting(searchRouting).build();
            // Check if this alias already exists
            AliasMetaData currentAliasMd = index.getAliases().get(alias);
            if (currentAliasMd != null && currentAliasMd.equals(newAliasMd)) {
                // It already exists, ignore it
                return false;
            }
            metadata.put(IndexMetaData.builder(index).putAlias(newAliasMd));
            return true;
        }
    }

    /**
     * Operation to remove an alias from an index.
     */
    public static class Remove extends AliasAction {
        private final String alias;

        /**
         * Build the operation.
         */
        public Remove(String index, String alias) {
            super(index);
            if (false == Strings.hasText(alias)) {
                throw new IllegalArgumentException("[alias] is required");
            }
            this.alias = alias;
        }

        /**
         * Alias to remove from the index.
         */
        @Override
        public String getAlias() {
            return alias;
        }

        @Override
        boolean removeIndex() {
            return false;
        }

        @Override
        boolean apply(NewAliasValidator aliasValidator, MetaData.Builder metadata, IndexMetaData index) {
            // As in the remove action an alias may contain wildcards, we first need to expand alias wildcards
            List<String> concreteAliases = metadata.findAliases(alias, getIndex());
            if (concreteAliases.isEmpty()) {
                throw new AliasesNotFoundException(alias);
            }
            Boolean changed = false;
            for (String concreteAlias : concreteAliases){
                if (index.getAliases().containsKey(concreteAlias)) {
                    metadata.put(IndexMetaData.builder(index).removeAlias(concreteAlias));
                    changed = true;
                }
            }
            return changed;
        }
    }

    /**
     * Operation to remove an index. This is an "alias action" because it allows us to remove an index at the same time as we remove add an
     * alias to replace it.
     */
    public static class RemoveIndex extends AliasAction {
        public RemoveIndex(String index) {
            super(index);
        }

        @Override
        boolean removeIndex() {
            return true;
        }

        @Override
        boolean apply(NewAliasValidator aliasValidator, MetaData.Builder metadata, IndexMetaData index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getAlias() {
            return null;
        }
    }
}
