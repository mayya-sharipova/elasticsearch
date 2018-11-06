package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

public class VectorFieldUtils {
    static int INT_BYTES = Integer.BYTES;


    //**************STATIC HELPER METHODS************************************

    /**
     * Calculates cosine similarity between vectors v1 and v2
     * Vectors can be sparse or dense
     * @param v1Values - values for v1 vector
     * @param v1Dims - dimensions for v1 vector, or null of v1 vector is dense
     * @param v2Values - values for v2 vector
     * @param v2Dims - dimensions for v2 vector, or null of v2 vector is dense
     * @param v2Magnitude - magnitude of v2 vector
     * @return cosine similarity between vectors
     */
    static float cosineSimilarity(float[] v1Values, int[] v1Dims, float[] v2Values, int[] v2Dims, float v2Magnitude) {
        float v1DotProduct = 0f;
        float v1v2DotProduct = 0f;
        int v1Index = 0; int v1Dim = 0;
        int v2Index = 0; int v2Dim = 0;
        // find common dimensions among vectors v1 and v2 and calculate dotProduct based on common dimensions
        while (v1Index < v1Values.length && v2Index < v2Values.length) {
            // if a vector is dense, its dimensions array is null, and its dimensions are indexes of its values array
            // if a vector is sparse, its dimensions are in its dimensions array
            v1Dim = v1Dims == null ? v1Index : v1Dims[v1Index];
            v2Dim = v2Dims == null ? v2Index : v2Dims[v2Index];
            if (v1Dim == v2Dim) {
                v1v2DotProduct += v1Values[v1Index] * v2Values[v2Index];
                v1Index++;
                v2Index++;
            } else if (v1Dim > v2Dim) {
                v2Index++;
            } else {
                v1Index++;
            }
        }
        // calculate docProduct of vector v1 by itself
        for (int dim = 0; dim < v1Values.length; dim++) {
            v1DotProduct += v1Values[dim] * v1Values[dim];
        }
        final float v1Magnitude = (float) Math.sqrt(v1DotProduct);
        return v1v2DotProduct / (v1Magnitude * v2Magnitude);
    }

    /**
     * Decodes BytesRef into an array of floats
     * @param vectorBR - BytesRef, where:
     *    first 4 bytes is an integer representing a number of dimensions
     *    following by floats encoded as integers
     * @return - array of floats
     */
    static float[] decodeVector(BytesRef vectorBR) {
        int dimCount = NumericUtils.sortableBytesToInt(vectorBR.bytes, vectorBR.offset);
        float[] vector = new float[dimCount];
        int offset =  vectorBR.offset;
        for (int dim = 0; dim < dimCount; dim++) {
            offset = offset + INT_BYTES;
            vector[dim] = Float.intBitsToFloat(NumericUtils.sortableBytesToInt(vectorBR.bytes, offset));
        }
        return vector;
    }


}
