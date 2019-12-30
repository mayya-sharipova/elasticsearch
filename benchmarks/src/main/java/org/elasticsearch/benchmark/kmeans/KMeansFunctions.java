package org.elasticsearch.benchmark.kmeans;

import java.util.Random;

import org.apache.lucene.util.InPlaceMergeSorter;

import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.IntArray;



final class KMeansFunctions {

    static float[][] lloyds(float[][] vectors, IntArray documentCentroids) {
        float[][] centroids = initCentroids(vectors);
        for (int itr = 0; itr < KMeansBenchmark.ITERS; itr++) {
            centroids = runKMeansStep(itr, centroids, vectors, documentCentroids);
        }
        return centroids;
    }

    /**
     * Runs one iteration of k-means. For each document vector, we first find the
     * nearest centroid, then update the location of the new centroid.
     */
    private static float[][] runKMeansStep(int itr, float[][] centroids, float[][] vectors, IntArray documentCentroids) {
        double distToCentroid = 0.0;
        double distToOtherCentroids = 0.0;

        float[][] newCentroids = new float[centroids.length][centroids[0].length];
        int[] newCentroidSize = new int[centroids.length];
        for (int i = 0; i < KMeansBenchmark.N; i++) {
            int bestCentroid = -1;
            double bestDist = Double.MAX_VALUE;
            for (int c = 0; c < centroids.length; c++) {
                double dist = squaredDistance(centroids[c], vectors[i]);
                distToOtherCentroids += dist;
                if (dist < bestDist) {
                    bestCentroid = c;
                    bestDist = dist;
                }
            }
            newCentroidSize[bestCentroid]++;
            for (int v = 0; v < vectors[i].length; v++) {
                newCentroids[bestCentroid][v] += vectors[i][v];
            }
            distToCentroid += bestDist;
            distToOtherCentroids -= bestDist;
            documentCentroids.set(i, bestCentroid);
        }

        for (int c = 0; c < newCentroids.length; c++) {
            for (int v = 0; v < newCentroids[c].length; v++) {
                newCentroids[c][v] /= newCentroidSize[c];
            }
        }

        distToCentroid /= KMeansBenchmark.N;
        distToOtherCentroids /= KMeansBenchmark.N * (centroids.length - 1);

//        System.out.println("Finished iteration [" + itr + "]. Dist to centroid [" + distToCentroid +
//            "], dist to other centroids [" + distToOtherCentroids + "].");

        return newCentroids;
    }


    /**
     * Based on the paper:
     * Phillips, Steven J. "Acceleration of k-means and related clustering algorithms."
     * Workshop on Algorithm Engineering and Experimentation. Springer, Berlin, Heidelberg, 2002.
     */
    static float[][] kmeansSort(float[][] vectors, IntArray documentCentroids, DoubleArray docCentroidDists) {
        float[][] centroids = initCentroids(vectors);
        double[][] cdists = new double[KMeansBenchmark.K][KMeansBenchmark.K]; // inter-centroid distances
        int[][] cdistIndexes = new int[KMeansBenchmark.K][KMeansBenchmark.K]; // for each centroid shows sorted by distances indexes of other centroids
        for (int itr = 0; itr < KMeansBenchmark.ITERS; itr++) {
            centroids = runKMeansSortStep(itr, centroids, cdists, cdistIndexes, vectors, documentCentroids, docCentroidDists);
        }
        return centroids;
    }

    private static float[][] runKMeansSortStep(int itr, float[][] centroids, double[][] cdists, int[][] cdistIndexes,
            float[][] vectors, IntArray documentCentroids, DoubleArray docCentroidDists) {
        double distToCentroid = 0.0;
        double distToOtherCentroids = 0.0;

        float[][] newCentroids = new float[centroids.length][centroids[0].length];
        int[] newCentroidSize = new int[centroids.length];
        for (int i = 0; i < KMeansBenchmark.N; i++) {
            int bestCentroid = itr == 0 ? -1 : documentCentroids.get(i);
            double bestDist = itr == 0 ? Double.MAX_VALUE: docCentroidDists.get(i);
            double inClassDist = bestDist;
            for (int c = 0; c < centroids.length; c++) {
                int theClass;
                if (itr == 0) {
                    theClass = c;
                } else {
                    theClass = cdistIndexes[bestCentroid][c];
                    if (cdists[bestCentroid][theClass] >= (4 * inClassDist)) {
                        break;
                    }
                }
                double dist = squaredDistance(centroids[theClass], vectors[i]);
                distToOtherCentroids += dist;
                if (dist < bestDist) {
                    bestCentroid = theClass;
                    bestDist = dist;
                }
            }
            newCentroidSize[bestCentroid]++;
            for (int v = 0; v < vectors[i].length; v++) {
                newCentroids[bestCentroid][v] += vectors[i][v];
            }
            distToCentroid += bestDist;
            distToOtherCentroids -= bestDist;
            documentCentroids.set(i, bestCentroid);
            docCentroidDists.set(i, bestDist);
        }

        for (int c = 0; c < newCentroids.length; c++) {
            for (int v = 0; v < newCentroids[c].length; v++) {
                newCentroids[c][v] /= newCentroidSize[c];
            }
        }
        for (int i = 0; i < newCentroids.length; i++) {
            for (int j = 0; j < i; j++) {
                cdists[i][j] = cdists[j][i];
            }
            for (int j = i+1; j < newCentroids.length; j++) {
                cdists[i][j] = squaredDistance(newCentroids[i], newCentroids[j]);
            }
            cdistIndexes[i] = new int[newCentroids.length];
            sortDistIndexes(cdists[i], cdistIndexes[i], newCentroids.length);
        }
        distToCentroid /= KMeansBenchmark.N;
        distToOtherCentroids /= KMeansBenchmark.N * (centroids.length - 1);


//        System.out.println("Finished iteration [" + itr + "]. Dist to centroid [" + distToCentroid +
//            "], dist to other centroids [" + distToOtherCentroids + "].");

        return newCentroids;
    }

    public static void sortDistIndexes(double[] srcDists, int[] indexes, int n) {
        double[] dists = new double[srcDists.length];
        System.arraycopy(srcDists, 0, dists, 0, dists.length);
        for (int i = 0 ; i < indexes.length; i++) {
            indexes[i] = i;
        }

        new InPlaceMergeSorter() {
            @Override
            public int compare(int i, int j) {
                return Double.compare(dists[i], dists[j]);
            }

            @Override
            public void swap(int i, int j) {
                double tempDist = dists[i];
                dists[i] = dists[j];
                dists[j] = tempDist;

                int tempIndex = indexes[i];
                indexes[i] = indexes[j];
                indexes[j] = tempIndex;
            }
        }.sort(0, n);
    }

    private static float[][] initCentroids(float[][] vectors) {
        final Random random = new Random(42);
        float[][] centroids = new float[KMeansBenchmark.K][];
        // initialize centroids with random points
        for (int i = 0; i < KMeansBenchmark.N; i++) {
            if (i < KMeansBenchmark.K) {
                centroids[i] = vectors[i];
            } else if (random.nextDouble() < KMeansBenchmark.K * (1.0 / i)) {
                int c = random.nextInt(KMeansBenchmark.K);
                centroids[c] = vectors[i];
            }
        }
        return centroids;
    }

    public static double squaredDistance(float[] v1, float[] v2) {
        double dist = 0;
        for (int dim = 0; dim < v1.length; dim++) {
            float dif = v1[dim] - v2[dim];
            dist += dif * dif;
        }
        return dist;
    }
}
