package com.fiberhome.ml.raha.feature;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 特征字典和向量确定性测试。
 */
class FeatureVectorizerTest {

    @Test
    void shouldBuildStableDictionaryAndSparseVector() {
        FeatureDictionary left = FeatureDictionary.build("Kingdom",
                Arrays.asList("Mordor", "", "Rohan", "Mordor"), 100);
        FeatureDictionary right = FeatureDictionary.build("Kingdom",
                Arrays.asList("Rohan", "Mordor", ""), 100);
        assertEquals(left.getVersion(), right.getVersion());
        FeatureVectorizer vectorizer = new FeatureVectorizer();
        double[] vector = vectorizer.vectorize("Mordor", left);
        assertArrayEquals(vector, vectorizer.fromSparseJson(
                vectorizer.toSparseJson(vector), left.size()), 0.000001d);
    }
}
