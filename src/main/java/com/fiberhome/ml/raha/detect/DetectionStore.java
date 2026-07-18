package com.fiberhome.ml.raha.detect;

import com.fiberhome.ml.raha.model.RahaColumnModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Optional;

/**
 * 检测批次和检测结果的最小持久化端口。
 */
public interface DetectionStore {

    Optional<DetectionBatch> findBatch(String detectionBatchId);

    long appendResults(DetectionBatch batch, RahaColumnModel model,
                       Dataset<Row> resultRows, String partitionDate);

    void saveBatch(DetectionBatch batch);
}
