package org.elasticsearch.benchmark.kmeans;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.IntArray;
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

import java.util.concurrent.TimeUnit;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;

@Fork(1)
@Warmup(iterations = 0) // 10
@Measurement(iterations = 1) // 10
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class KMeansBenchmark {
    private final static int MAX_DIMS = 128;
    private final static int DIMS = 16;
    public final static int N = 1_000_000; // number of vectors
    public final static int K = (int) Math.sqrt(N); // number of centroids
    public final static int ITERS = 5; // number of iterations
    private final static boolean reportResults = false;

    @State(Scope.Benchmark)
    public static class VectorState {
        private float[][] vectors;

        @Setup
        public void loadVectors() {
            this.vectors = initVectors();
        }
    }

    @Benchmark
    public float[][] lloyds(VectorState state) {
        final BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        IntArray documentCentroids = bigArrays.newIntArray(KMeansBenchmark.N);
        float[][] centroids = KMeansFunctions.lloyds(state.vectors, documentCentroids);

        if (reportResults) {
            double distToCentroid = 0.0;
            double distToOtherCentroids = 0.0;
            for (int i = 0; i < KMeansBenchmark.N; i++) {
                int bestCentroid = documentCentroids.get(i);
                for (int c = 0; c < centroids.length; c++) {
                    if (c == bestCentroid) {
                        distToCentroid +=  KMeansFunctions.squaredDistance(centroids[c], state.vectors[i]);;
                    } else {
                        distToOtherCentroids += KMeansFunctions.squaredDistance(centroids[c], state.vectors[i]);
                    }
                }
            }
            distToCentroid /= KMeansBenchmark.N;
            distToOtherCentroids /= KMeansBenchmark.N * (centroids.length - 1);
            System.out.println("-------- Dist to centroid [" + distToCentroid + "], dist to other centroids [" + distToOtherCentroids + "].");
        }

        return centroids;
    }

    @Benchmark
    public float[][] kmeansSort(VectorState state) {
        final BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        IntArray documentCentroids = bigArrays.newIntArray(KMeansBenchmark.N);
        DoubleArray docCentroidDists = bigArrays.newDoubleArray(KMeansBenchmark.N);
        float[][] centroids = KMeansFunctions.kmeansSort(state.vectors, documentCentroids, docCentroidDists);

        if (reportResults) {
            double distToCentroid = 0.0;
            double distToOtherCentroids = 0.0;
            for (int i = 0; i < KMeansBenchmark.N; i++) {
                distToCentroid += docCentroidDists.get(i);
                int bestCentroid = documentCentroids.get(i);
                for (int c = 0; c < centroids.length; c++) {
                    if (c == bestCentroid) continue;
                    distToOtherCentroids += KMeansFunctions.squaredDistance(centroids[c], state.vectors[i]);
                }
            }
            distToCentroid /= KMeansBenchmark.N;
            distToOtherCentroids /= KMeansBenchmark.N * (centroids.length - 1);
            System.out.println("-------- Dist to centroid [" + distToCentroid + "], dist to other centroids [" + distToOtherCentroids + "].");
        }

        return centroids;
    }

    private static float[][] initVectors() {
        try {
            InputStream istream = KMeansBenchmark.class.getResourceAsStream("/train_data.txt");
            byte[] bytes = new byte[istream.available()];
            istream.read(bytes);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            FloatBuffer fbuffer = buffer.asFloatBuffer();
            float[][] vvectors = new float[N][DIMS];
            for (int i = 0; i < N; i++) {
                for (int dim = 0; dim < DIMS; dim++) {
                    vvectors[i][dim] = fbuffer.get();
                }
                for (int dim = DIMS; dim < MAX_DIMS; dim++) {
                    fbuffer.get();
                }
            }
            istream.close();
            System.out.println("-------- Vectors are loaded!");
            return vvectors;
        } catch (IOException e) {
            return null;
        }
    }
}
