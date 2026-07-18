package com.fiberhome.ml.raha.cluster;

import java.util.Collection;

/**
 * 单列值聚类接口。
 */
public interface ColumnClusterer {

    ClusteringResult cluster(Collection<String> values);
}
