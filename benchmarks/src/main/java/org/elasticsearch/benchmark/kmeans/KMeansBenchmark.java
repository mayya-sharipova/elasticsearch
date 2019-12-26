package org.elasticsearch.benchmark.kmeans;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@Fork(3)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class KMeansBenchmark {
    private final static int DIMS = 512;
    private final static int N = 1000; // number of vectors
    public final static int K = 1000; // number of  clusters
    public final static int ITERS = 5; // number of iterations
    private final static float[][] vectors = initVectors();

    @Benchmark
    public float[] lloyds(float[][] vectors) {
        return KMeansFunctions.lloyds(vectors);
    }

    @Benchmark
    public float[] kmeansSort(float[][] vectors) {
        return KMeansFunctions.kmeansSort(vectors);
    }


    @Benchmark
    @Warmup(iterations = 0)
    @Measurement(iterations = 1)
    public void testVectorFunctions(float[][] vectors) {


    }

    private static float[][] initVectors() {
        return null;
    }
}
