package com.fiberhome.ml.raha.data.loader;

import com.fiberhome.ml.raha.data.DatasetSnapshot;
import com.fiberhome.ml.raha.util.HashUtils;

/**
 * 根据数据源版本、模式和规模创建输入快照元数据。
 */
public final class SnapshotMetadataFactory {

    public DatasetSnapshot create(DataLoadRequest request,
                                  String schemaHash,
                                  long rowCount,
                                  int columnCount,
                                  long createdAt) {
        if (request == null) {
            throw new IllegalArgumentException("数据加载请求不能为空");
        }
        String snapshotId = request.getSnapshotId();
        if (snapshotId == null || snapshotId.trim().isEmpty()) {
            String sourceVersion = request.getSourceVersion() == null
                    ? String.valueOf(createdAt) : request.getSourceVersion();
            String source = request.getDatasetId() + "|" + request.getInputReference()
                    + "|" + sourceVersion + "|" + schemaHash + "|" + rowCount;
            snapshotId = "snapshot-" + HashUtils.sha256Hex(source).substring(0, 24);
        }
        return new DatasetSnapshot(request.getDatasetId(), snapshotId,
                request.getInputReference(), request.getTableName(), request.getRowIdColumn(),
                schemaHash, rowCount, columnCount, request.getSourceVersion(), createdAt);
    }
}

