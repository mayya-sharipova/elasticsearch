/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gpu.codec;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Links the parts of a CAGRA graph that can't be reached from node 0 to the part that can.
 * <p>
 * The graph is written as a single-level Lucene HNSW graph, which is searched from node 0, so a node that no path from node 0
 * leads to is never found. CAGRA's graph can split into such islands when the vectors form well separated clusters: all the
 * nearest neighbours of a node, and so all its edges, stay inside its cluster. CAGRA can link them itself, with the
 * {@code guarantee_connectivity} option of its optimize step, but cuVS doesn't expose that option yet.
 * <p>
 * {@link #connectComponents} is an adapted copy of Lucene's {@code HnswGraphBuilder#connectComponents} and
 * {@code HnswUtil#components}: for each island, it searches the graph for the reachable node closest to it and adds an edge from
 * that node. It's adapted to CAGRA's graph, which has a single level, fixed-size rows of neighbours without scores, and is searched
 * from node 0, so islands are linked to the part reachable from node 0 rather than to the largest one.
 * <p>
 * TODO: remove once a cuVS release exposes guarantee_connectivity (https://github.com/NVIDIA/cuvs/pull/3010), and set it in
 * {@link ES92GpuHnswVectorsWriter#createCagraIndexParams} instead.
 */
final class GraphConnectivity {
    private static final Logger logger = LogManager.getLogger(GraphConnectivity.class);

    /** The most islands linked in a graph, so that vectors with many tiny islands, such as duplicates, can't stall a flush or merge */
    static final int MAX_LINKED_ISLANDS = 1000;

    /** The number of candidates the search for the node to link an island from collects */
    private static final int NUM_CANDIDATES = 10;

    /** Reads the neighbours of a node, as many as the graph degree */
    @FunctionalInterface
    interface Rows {
        void read(int node, int[] neighbors);
    }

    private GraphConnectivity() {}

    /**
     * @param scorers   the scorers of the vectors of the graph's nodes, only created if some nodes can't be reached
     * @return the edges to add so that every node can be reached from node 0, indexed by node and null for a node that gets none,
     *         at most {@code degree} per node; or null if every node can already be reached. If more than
     *         {@code maxLinkedIslands} islands can't be reached, the remaining ones stay unreachable.
     */
    static int[][] connectComponents(
        Rows rows,
        int numNodes,
        int degree,
        IOSupplier<RandomVectorScorerSupplier> scorers,
        int maxLinkedIslands
    ) throws IOException {
        Graph graph = new Graph(rows, numNodes, degree);
        int numReached = graph.markReachable(0);
        if (numReached == numNodes) {
            return null;
        }
        long startTime = System.nanoTime();
        UpdateableRandomVectorScorer scorer = scorers.get().scorer();
        int numLinked = 0;
        for (int island = graph.nextUnreached(0); island != NO_MORE_DOCS; island = graph.nextUnreached(island + 1)) {
            if (numLinked == maxLinkedIslands) {
                break;
            }
            scorer.setScoringOrdinal(island);
            int from = graph.closestReachedWithRoom(scorer);
            if (from == -1) {
                break;
            }
            graph.addEdge(from, island);
            if (graph.hasRoom(island)) {
                graph.addEdge(island, from);
            }
            numReached += graph.markReachable(island);
            numLinked++;
        }
        if (logger.isDebugEnabled()) {
            logger.debug(
                "Linked [{}] islands of the CAGRA graph in [{}ms]; [{}] of [{}] nodes can be reached",
                numLinked,
                (System.nanoTime() - startTime) / 1_000_000.0,
                numReached,
                numNodes
            );
        }
        return graph.extraEdges;
    }

    /** The CAGRA graph with the edges added so far, which a Lucene graph search can walk */
    private static final class Graph extends HnswGraph {
        private final Rows rows;
        private final int numNodes;
        private final int degree;
        private final FixedBitSet reached;
        private final int[] stack;
        private int[][] extraEdges;
        // the neighbours of the node the search is on
        private final int[] neighbors;
        private int numNeighbors;
        private int upTo;

        Graph(Rows rows, int numNodes, int degree) {
            this.rows = rows;
            this.numNodes = numNodes;
            this.degree = degree;
            this.reached = new FixedBitSet(numNodes);
            this.stack = new int[numNodes];
            this.neighbors = new int[2 * degree];
        }

        /** Marks the nodes that can be reached from {@code start} and returns how many weren't marked yet */
        int markReachable(int start) {
            if (reached.getAndSet(start)) {
                return 0;
            }
            // a node is pushed only once, when it's marked, so the stack never holds more than all the nodes
            stack[0] = start;
            int top = 1;
            int count = 1;
            while (top > 0) {
                int numNodeNeighbors = readNeighbors(stack[--top]);
                for (int i = 0; i < numNodeNeighbors; i++) {
                    if (reached.getAndSet(neighbors[i]) == false) {
                        stack[top++] = neighbors[i];
                        count++;
                    }
                }
            }
            return count;
        }

        /** Returns the reached node with room for another edge whose vector scores best, or -1 if no such node is found */
        int closestReachedWithRoom(UpdateableRandomVectorScorer scorer) throws IOException {
            TopKnnCollector collector = new TopKnnCollector(NUM_CANDIDATES, Integer.MAX_VALUE);
            // the search starts from node 0, so it only visits reached nodes
            HnswGraphSearcher.search(scorer, collector, this, new Bits() {
                @Override
                public boolean get(int index) {
                    return hasRoom(index);
                }

                @Override
                public int length() {
                    return numNodes;
                }
            });
            int best = -1;
            float bestScore = Float.NEGATIVE_INFINITY;
            for (ScoreDoc candidate : collector.topDocs().scoreDocs) {
                if (candidate.score > bestScore) {
                    best = candidate.doc;
                    bestScore = candidate.score;
                }
            }
            return best;
        }

        /** Returns the first node from {@code start} that isn't reached, or {@link DocIdSetIterator#NO_MORE_DOCS} if there is none */
        int nextUnreached(int start) {
            // FixedBitSet#nextClearBit doesn't accept the length of the bit set
            return start < numNodes ? reached.nextClearBit(start) : NO_MORE_DOCS;
        }

        boolean hasRoom(int node) {
            return extraEdges == null || extraEdges[node] == null || extraEdges[node].length < degree;
        }

        void addEdge(int from, int to) {
            assert hasRoom(from);
            if (extraEdges == null) {
                extraEdges = new int[numNodes][];
            }
            int[] edges = extraEdges[from];
            if (edges == null) {
                extraEdges[from] = new int[] { to };
            } else {
                edges = Arrays.copyOf(edges, edges.length + 1);
                edges[edges.length - 1] = to;
                extraEdges[from] = edges;
            }
        }

        /** Reads the neighbours of {@code node}, with the edges added to it, into {@link #neighbors} and returns how many */
        private int readNeighbors(int node) {
            rows.read(node, neighbors);
            int count = degree;
            if (extraEdges != null && extraEdges[node] != null) {
                int[] edges = extraEdges[node];
                System.arraycopy(edges, 0, neighbors, count, edges.length);
                count += edges.length;
            }
            return count;
        }

        @Override
        public void seek(int level, int target) {
            assert level == 0 : "the graph has a single level";
            numNeighbors = readNeighbors(target);
            upTo = 0;
        }

        @Override
        public int nextNeighbor() {
            return upTo < numNeighbors ? neighbors[upTo++] : NO_MORE_DOCS;
        }

        @Override
        public int neighborCount() {
            return numNeighbors;
        }

        @Override
        public int size() {
            return numNodes;
        }

        @Override
        public int numLevels() {
            return 1;
        }

        @Override
        public int maxConn() {
            return degree;
        }

        @Override
        public int entryNode() {
            return 0;
        }

        @Override
        public NodesIterator getNodesOnLevel(int level) {
            assert level == 0 : "the graph has a single level";
            return new DenseNodesIterator(numNodes);
        }
    }
}
