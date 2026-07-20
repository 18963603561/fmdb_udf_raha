package com.fiberhome.ml.raha.data.loader.metadata;

import com.fiberhome.ml.raha.data.domain.DatasetSnapshot;
import com.fiberhome.ml.raha.data.loader.DataLoadRequest;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityColumns;
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
        return create(request, schemaHash, rowCount, columnCount, createdAt,
                null);
    }

    /**
     * 根据平台版本或确定性数据内容指纹创建输入快照元数据。
     *
     * @param request 数据加载请求
     * @param schemaHash 输入模式哈希
     * @param rowCount 逻辑行数
     * @param columnCount 业务字段数
     * @param createdAt 快照创建时间
     * @param contentFingerprint 无平台来源版本时的数据内容指纹
     * @return 不可变数据集快照
     */
    public DatasetSnapshot create(DataLoadRequest request,
                                  String schemaHash,
                                  long rowCount,
                                  int columnCount,
                                  long createdAt,
                                  String contentFingerprint) {
        if (request == null) {
            throw new IllegalArgumentException("数据加载请求不能为空");
        }
        String snapshotId = request.getSnapshotId();
        if (snapshotId == null || snapshotId.trim().isEmpty()) {
            String sourceVersion = request.getSourceVersion();
            if (sourceVersion == null || sourceVersion.trim().isEmpty()) {
                sourceVersion = contentFingerprint == null
                        || contentFingerprint.trim().isEmpty()
                        ? "legacy-read-" + createdAt
                        : "content-" + contentFingerprint;
            }
            String source = request.getDatasetId() + "|" + request.getInputReference()
                    + "|" + sourceVersion + "|" + schemaHash + "|" + rowCount;
            snapshotId = "snapshot-" + HashUtils.sha256Hex(source).substring(0, 24);
        }
        return new DatasetSnapshot(request.getDatasetId(), snapshotId,
                request.getInputReference(), request.getTableName(),
                RowIdentityColumns.ROW_ID,
                schemaHash, rowCount, columnCount, request.getSourceVersion(), createdAt);
    }
}
