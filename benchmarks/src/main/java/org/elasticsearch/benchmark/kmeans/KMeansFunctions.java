package org.elasticsearch.benchmark.kmeans;

final class KMeansFunctions {

    static float[] lloyds(float[][] vectors) {
        float[] centroids = new float[KMeansBenchmark.K];

        // initialize centroids with random points

        for (int itr = 0; itr < KMeansBenchmark.ITERS; itr++) {

        }

        return null;
    }

    


    static float[] kmeansSort(float[][] vectors) {
        return null;
    }

    static double squaredDistance(float[] v1, float[] v2) {
        double dist = 0;
        for (int dim = 0; dim < v1.length; dim++) {
            float dif = v1[dim] - v2[dim];
            dist += dif * dif;
        }
        return dist;
    }
}
