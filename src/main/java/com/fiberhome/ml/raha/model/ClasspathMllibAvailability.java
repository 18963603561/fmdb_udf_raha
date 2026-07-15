package com.fiberhome.ml.raha.model;

/**
 * 根据逻辑回归类是否可加载判断 Spark MLlib 运行时可用性。
 */
public final class ClasspathMllibAvailability implements MllibAvailability {

    @Override
    public boolean isAvailable() {
        try {
            Class.forName("org.apache.spark.ml.classification.LogisticRegression", false,
                    ClasspathMllibAvailability.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException | LinkageError exception) {
            return false;
        }
    }
}
