/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gpu.codec;

import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.elasticsearch.gpu.CuVSGPUSupport;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Builds exact kNN graphs, like the ones CAGRA builds, of vectors in well separated clusters, so that no edge leaves a cluster,
 * and checks that {@link GraphConnectivity} makes every node reachable from node 0.
 */
// Production grants cuvs-java load_native_libraries through the gpu plugin's policy, which isn't visible from this library's tests.
// Enforcing here would fail cuvs initialization, which is cached for the JVM and silently skips every GPU test that shares it.
@ESTestCase.WithoutEntitlements
public class GraphConnectivityTests extends ESTestCase {

    @BeforeClass
    public static void beforeClass() {
        assumeTrue("cuvs not supported", CuVSGPUSupport.instance().isSupported());
    }

    public void testConnectedGraphGetsNoEdges() throws IOException {
        float[][] vectors = clusteredVectors(randomIntBetween(1, 5), randomIntBetween(20, 50), 8);
        int degree = randomIntBetween(4, 8);
        int[][] graph = knnGraph(vectors, degree, VectorSimilarityFunction.EUCLIDEAN);
        // a ring through all the nodes connects the graph
        for (int node = 0; node < graph.length; node++) {
            graph[node][degree - 1] = (node + 1) % graph.length;
        }

        int[][] extraEdges = GraphConnectivity.connectComponents(rows(graph), vectors.length, degree, () -> {
            throw new AssertionError("the vectors must not be read for a connected graph");
        }, GraphConnectivity.MAX_LINKED_ISLANDS);
        assertThat(extraEdges, nullValue());
    }

    public void testIslandsAreLinked() throws IOException {
        int numClusters = randomIntBetween(2, 20);
        int clusterSize = randomIntBetween(20, 50);
        int degree = randomIntBetween(4, 8);
        VectorSimilarityFunction similarity = randomFrom(VectorSimilarityFunction.EUCLIDEAN, VectorSimilarityFunction.COSINE);
        float[][] vectors = clusteredVectors(numClusters, clusterSize, 16);
        int[][] graph = knnGraph(vectors, degree, similarity);
        assertThat("no edge leaves a cluster", reachable(graph, null), lessThanOrEqualTo(clusterSize));

        int[][] extraEdges = GraphConnectivity.connectComponents(
            rows(graph),
            vectors.length,
            degree,
            () -> scorers(vectors, similarity),
            GraphConnectivity.MAX_LINKED_ISLANDS
        );

        assertThat(reachable(graph, extraEdges), equalTo(vectors.length));
        for (int[] edges : extraEdges) {
            if (edges != null) {
                assertThat(edges.length, lessThanOrEqualTo(degree));
            }
        }
    }

    public void testIslandIsLinkedFromItsClosestCluster() throws IOException {
        // clusters along a line, with node 0 in the first one: each cluster is closest to its two neighbours on the line, so
        // linking an island from the reached node closest to it never adds an edge to a cluster farther away. (A directed kNN
        // graph can also have small islands inside a cluster, which are linked from the same cluster.)
        int numClusters = randomIntBetween(3, 10);
        int clusterSize = 30;
        int degree = 6;
        float[][] vectors = new float[numClusters * clusterSize][4];
        for (int i = 0; i < vectors.length; i++) {
            vectors[i][0] = 100 * (i / clusterSize) + randomFloat();
            for (int d = 1; d < 4; d++) {
                vectors[i][d] = randomFloat();
            }
        }
        int[][] graph = knnGraph(vectors, degree, VectorSimilarityFunction.EUCLIDEAN);

        int[][] extraEdges = GraphConnectivity.connectComponents(
            rows(graph),
            vectors.length,
            degree,
            () -> scorers(vectors, VectorSimilarityFunction.EUCLIDEAN),
            GraphConnectivity.MAX_LINKED_ISLANDS
        );

        assertThat(reachable(graph, extraEdges), equalTo(vectors.length));
        for (int from = 0; from < vectors.length; from++) {
            if (extraEdges[from] != null) {
                for (int to : extraEdges[from]) {
                    assertThat(Math.abs(from / clusterSize - to / clusterSize), lessThanOrEqualTo(1));
                }
            }
        }
    }

    public void testNumberOfLinkedIslandsIsCapped() throws IOException {
        int numClusters = randomIntBetween(3, 10);
        int clusterSize = 20;
        int degree = 4;
        float[][] vectors = clusteredVectors(numClusters, clusterSize, 16);
        int[][] graph = knnGraph(vectors, degree, VectorSimilarityFunction.EUCLIDEAN);
        int maxLinkedIslands = randomIntBetween(1, numClusters - 2);

        int[][] extraEdges = GraphConnectivity.connectComponents(
            rows(graph),
            vectors.length,
            degree,
            () -> scorers(vectors, VectorSimilarityFunction.EUCLIDEAN),
            maxLinkedIslands
        );

        assertThat(reachable(graph, extraEdges), lessThan(vectors.length));
        int numFrom = 0;
        for (int[] edges : extraEdges) {
            if (edges != null) {
                numFrom++;
            }
        }
        // each linked island adds an edge from a reached node, and one back from the island
        assertThat(numFrom, lessThanOrEqualTo(2 * maxLinkedIslands));
    }

    /** Vectors in clusters of consecutive ordinals, with centers so far apart that a node's nearest neighbours are all in its cluster */
    private static float[][] clusteredVectors(int numClusters, int clusterSize, int dims) {
        float[][] vectors = new float[numClusters * clusterSize][dims];
        for (int c = 0; c < numClusters; c++) {
            float[] center = new float[dims];
            for (int d = 0; d < dims; d++) {
                center[d] = randomFloat() * 200 - 100;
            }
            for (int i = c * clusterSize; i < (c + 1) * clusterSize; i++) {
                for (int d = 0; d < dims; d++) {
                    vectors[i][d] = center[d] + (float) random().nextGaussian() * 0.1f;
                }
            }
        }
        return vectors;
    }

    /** The exact kNN graph: the {@code degree} nearest other nodes of each node */
    private static int[][] knnGraph(float[][] vectors, int degree, VectorSimilarityFunction similarity) {
        int[][] graph = new int[vectors.length][];
        for (int node = 0; node < vectors.length; node++) {
            float[] vector = vectors[node];
            int self = node;
            graph[node] = IntStream.range(0, vectors.length)
                .filter(other -> other != self)
                .boxed()
                .sorted(Comparator.comparingDouble(other -> -similarity.compare(vector, vectors[other])))
                .limit(degree)
                .mapToInt(Integer::intValue)
                .toArray();
        }
        return graph;
    }

    private static GraphConnectivity.Rows rows(int[][] graph) {
        return (node, neighbors) -> System.arraycopy(graph[node], 0, neighbors, 0, graph[node].length);
    }

    private static RandomVectorScorerSupplier scorers(float[][] vectors, VectorSimilarityFunction similarity) throws IOException {
        FloatVectorValues values = FloatVectorValues.fromFloats(List.of(vectors), vectors[0].length);
        return DefaultFlatVectorScorer.INSTANCE.getRandomVectorScorerSupplier(similarity, values);
    }

    /** The number of nodes reachable from node 0 */
    private static int reachable(int[][] graph, int[][] extraEdges) {
        boolean[] reached = new boolean[graph.length];
        Deque<Integer> stack = new ArrayDeque<>();
        reached[0] = true;
        stack.push(0);
        int count = 1;
        while (stack.isEmpty() == false) {
            int node = stack.pop();
            int[] edges = graph[node];
            if (extraEdges != null && extraEdges[node] != null) {
                edges = Arrays.copyOf(edges, edges.length + extraEdges[node].length);
                System.arraycopy(extraEdges[node], 0, edges, graph[node].length, extraEdges[node].length);
            }
            for (int neighbor : edges) {
                if (reached[neighbor] == false) {
                    reached[neighbor] = true;
                    stack.push(neighbor);
                    count++;
                }
            }
        }
        return count;
    }
}
