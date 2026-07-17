package com.fiberhome.ml.raha.fmdb;

import com.fiberhome.ml.raha.api.SampleRequest;
import com.fiberhome.ml.raha.data.RahaDataset;
import com.fiberhome.ml.raha.data.RowIdentityMode;
import com.fiberhome.ml.raha.data.TargetColumnResolver;
import com.fiberhome.ml.raha.support.HashUtils;
import com.fiberhome.ml.raha.support.RahaErrorCode;
import com.fiberhome.ml.raha.support.RahaException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sha2;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.to_json;
import static org.apache.spark.sql.functions.xxhash64;

/**
 * 使用当前 Spark 会话加载 FMDB 表、SQL 或 CSV，并解析稳定行身份。
 */
public final class FmdbDatasetLoader {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(FmdbDatasetLoader.class);
    /** 当前 Spark 会话。 */
    private final SparkSession sparkSession;

    public FmdbDatasetLoader(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    /**
     * 加载采样请求并解析完整内部数据集契约。
     *
     * @param request 采样请求
     * @return 算法数据集
     */
    public RahaDataset load(SampleRequest request) {
        return load(request.getInputReference(), request.getDatasetId(),
                request.getSourceType(), request.getRowKeyColumns(),
                request.getSnapshotId(), request.getTargetColumns());
    }

    /**
     * 加载检测或训练重放输入。
     *
     * @param inputReference 输入引用
     * @param datasetId 数据集标识
     * @param sourceType 来源类型
     * @param rowKeyColumns 行键字段
     * @param snapshotId 快照标识
     * @param targetColumns 目标字段
     * @return 算法数据集
     */
    public RahaDataset load(String inputReference, String datasetId, String sourceType,
                            List<String> rowKeyColumns, String snapshotId,
                            List<String> targetColumns) {
        long startedAt = System.currentTimeMillis();
        if (inputReference == null || inputReference.trim().isEmpty()) {
            throw new RahaException(RahaErrorCode.INVALID_REQUEST, "输入引用不能为空");
        }
        String resolvedSourceType = resolveSourceType(inputReference, sourceType);
        try {
            LOGGER.info("开始读取 Raha 输入，sourceType={}，inputReference={}",
                    resolvedSourceType, summarize(inputReference));
            Dataset<Row> original = read(inputReference, resolvedSourceType);
            List<String> columns = Arrays.asList(original.columns());
            if (columns.isEmpty()) {
                throw new RahaException(RahaErrorCode.INVALID_DATA, "输入不包含字段");
            }
            List<String> keys = rowKeyColumns == null
                    ? Collections.<String>emptyList() : new ArrayList<String>(rowKeyColumns);
            validateColumns(keys, columns, "行键字段");
            List<String> targets = TargetColumnResolver.resolve(targetColumns, columns);
            long inputRowCount = original.count();
            if (inputRowCount == 0L) {
                throw new RahaException(RahaErrorCode.INVALID_DATA, "输入表为空");
            }
            Column[] businessColumns = columnExpressions(columns);
            Dataset<Row> withJson = original.withColumn(RahaDataset.ROW_JSON,
                    to_json(struct(businessColumns)));
            String resolvedDatasetId = resolveDatasetId(inputReference, datasetId,
                    resolvedSourceType);
            String schemaHash = schemaHash(original.schema().fields());
            String resolvedSnapshotId = snapshotId == null || snapshotId.trim().isEmpty()
                    ? snapshot(withJson, schemaHash, inputRowCount) : snapshotId;
            Dataset<Row> rows;
            RowIdentityMode identityMode;
            if (keys.isEmpty()) {
                // 没有稳定业务键时必须按全部业务列分组，不能使用 Spark 分区序号。
                rows = original.groupBy(businessColumns)
                        .agg(count(lit(1)).cast("long").alias(RahaDataset.DUPLICATE_COUNT))
                        .withColumn(RahaDataset.ROW_JSON,
                                to_json(struct(columnExpressions(columns))))
                        .withColumn(RahaDataset.ROW_ID,
                                sha2(col(RahaDataset.ROW_JSON), 256));
                identityMode = RowIdentityMode.CONTENT_GROUP;
            } else {
                Column rowId = keys.size() == 1
                        ? coalesce(col(keys.get(0)).cast("string"), lit(""))
                        : sha2(to_json(struct(columnExpressions(keys))), 256);
                rows = withJson.withColumn(RahaDataset.ROW_ID, rowId)
                        .withColumn(RahaDataset.DUPLICATE_COUNT, lit(1L));
                long emptyKeyCount = rows.filter(col(RahaDataset.ROW_ID).equalTo("")).count();
                long distinctKeyCount = rows.select(RahaDataset.ROW_ID).distinct().count();
                if (emptyKeyCount > 0 || distinctKeyCount != inputRowCount) {
                    throw new RahaException(RahaErrorCode.INVALID_DATA,
                            "业务键必须非空且唯一，空键=" + emptyKeyCount
                                    + "，唯一键=" + distinctKeyCount
                                    + "，输入行=" + inputRowCount);
                }
                identityMode = RowIdentityMode.KEY;
            }
            Dataset<Row> cachedRows = rows.cache();
            long groupedCount = cachedRows.count();
            LOGGER.info("Raha 输入读取完成，datasetId={}，snapshotId={}，inputRows={}，"
                            + "groupedRows={}，targetColumns={}，elapsedMillis={}",
                    resolvedDatasetId, resolvedSnapshotId, inputRowCount, groupedCount,
                    targets, System.currentTimeMillis() - startedAt);
            return new RahaDataset(resolvedDatasetId, resolvedSnapshotId, inputReference,
                    resolvedSourceType, identityMode, keys, schemaHash, columns, targets,
                    cachedRows, inputRowCount);
        } catch (RahaException exception) {
            throw exception;
        } catch (RuntimeException exception) {
            LOGGER.error("Raha 输入读取失败，sourceType={}，inputReference={}",
                    resolvedSourceType, summarize(inputReference), exception);
            throw new RahaException(RahaErrorCode.STORAGE_ERROR, "输入读取失败", exception);
        }
    }

    private Dataset<Row> read(String reference, String sourceType) {
        if ("CSV".equals(sourceType)) {
            String path = reference.substring(4);
            if (path.startsWith("/") && !path.startsWith("file:")) {
                path = "file:" + path;
            }
            return sparkSession.read().option("header", "true")
                    .option("inferSchema", "false").csv(path);
        }
        if ("SQL".equals(sourceType)) {
            return sparkSession.sql(reference.substring(4));
        }
        return sparkSession.table(reference);
    }

    private static String resolveSourceType(String reference, String requested) {
        if (requested != null && !requested.trim().isEmpty()) {
            return requested.trim().toUpperCase();
        }
        if (reference.startsWith("csv:")) {
            return "CSV";
        }
        if (reference.startsWith("sql:")) {
            return "SQL";
        }
        return "TABLE";
    }

    private static String resolveDatasetId(String reference, String requested,
                                           String sourceType) {
        if (requested != null && !requested.trim().isEmpty()) {
            return requested.trim();
        }
        if ("TABLE".equals(sourceType)) {
            return reference.replaceAll("[^A-Za-z0-9_]+", "_");
        }
        if ("CSV".equals(sourceType)) {
            String path = reference.substring(4).replace('\\', '/');
            int slash = path.lastIndexOf('/');
            String file = slash >= 0 ? path.substring(slash + 1) : path;
            return file.replaceAll("\\.[^.]+$", "")
                    .replaceAll("[^A-Za-z0-9_]+", "_");
        }
        throw new RahaException(RahaErrorCode.INVALID_REQUEST,
                "SQL 来源必须指定稳定 datasetId");
    }

    private static void validateColumns(List<String> requested, List<String> available,
                                        String label) {
        for (String column : requested) {
            if (!available.contains(column)) {
                throw new RahaException(RahaErrorCode.INVALID_REQUEST,
                        label + "不存在：" + column);
            }
        }
    }

    private static String schemaHash(StructField[] fields) {
        StringBuilder builder = new StringBuilder();
        for (StructField field : fields) {
            builder.append(field.name()).append(':')
                    .append(field.dataType().catalogString()).append('|');
        }
        return HashUtils.sha256(builder.toString());
    }

    private static String snapshot(Dataset<Row> frame, String schemaHash, long countValue) {
        Row aggregate = frame.agg(sum(xxhash64(col(RahaDataset.ROW_JSON)))
                .alias("content_sum")).first();
        Object contentSum = aggregate.getAs("content_sum");
        return "snap_" + HashUtils.sha256(schemaHash + '|' + countValue + '|'
                + String.valueOf(contentSum)).substring(0, 24);
    }

    private static Column[] columnExpressions(List<String> columns) {
        Column[] expressions = new Column[columns.size()];
        for (int index = 0; index < columns.size(); index++) {
            expressions[index] = col(columns.get(index));
        }
        return expressions;
    }

    private static String summarize(String value) {
        return value.length() <= 160 ? value : value.substring(0, 160) + "...";
    }
}
