package com.fiberhome.ml.raha.data.loader;

import com.fiberhome.ml.raha.data.ColumnMetadata;
import com.fiberhome.ml.raha.data.DatasetSnapshot;
import com.fiberhome.ml.raha.data.RahaDataset;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 使用 Spark 文件数据源读取 CSV、JSON 或 Parquet，并生成只读 Raha 数据集。
 */
public final class FileRahaDatasetLoader implements RahaDatasetLoader {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(FileRahaDatasetLoader.class);
    /** Spark 会话。 */
    private final SparkSession sparkSession;
    /** 行标识校验器。 */
    private final RowIdValidator rowIdValidator;
    /** 模式哈希生成器。 */
    private final SchemaHasher schemaHasher;
    /** 字段元数据生成器。 */
    private final ColumnMetadataFactory columnMetadataFactory;
    /** 快照元数据生成器。 */
    private final SnapshotMetadataFactory snapshotMetadataFactory;
    /** 提供可测试时间的时钟。 */
    private final Clock clock;

    public FileRahaDatasetLoader(SparkSession sparkSession,
                                 RowIdValidator rowIdValidator,
                                 SchemaHasher schemaHasher,
                                 ColumnMetadataFactory columnMetadataFactory,
                                 SnapshotMetadataFactory snapshotMetadataFactory,
                                 Clock clock) {
        if (sparkSession == null || rowIdValidator == null || schemaHasher == null
                || columnMetadataFactory == null || snapshotMetadataFactory == null || clock == null) {
            throw new IllegalArgumentException("文件数据加载器依赖不能为空");
        }
        this.sparkSession = sparkSession;
        this.rowIdValidator = rowIdValidator;
        this.schemaHasher = schemaHasher;
        this.columnMetadataFactory = columnMetadataFactory;
        this.snapshotMetadataFactory = snapshotMetadataFactory;
        this.clock = clock;
    }

    @Override
    public LoadedDataset load(DataLoadRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("数据加载请求不能为空");
        }
        LOGGER.info("开始读取外部文件数据，datasetId={}，format={}，optionKeys={}",
                request.getDatasetId(), request.getFormat(), request.getOptions().keySet());
        try {
            DataFrameReader reader = sparkSession.read().format(request.getFormat().getSparkFormat());
            for (Map.Entry<String, String> option : request.getOptions().entrySet()) {
                reader = reader.option(option.getKey(), option.getValue());
            }
            Dataset<Row> dataFrame = reader.load(request.getInputReference());
            RowIdValidationResult rowIdResult = rowIdValidator.validate(
                    dataFrame, request.getRowIdColumn());
            String schemaHash = schemaHasher.hash(dataFrame.schema());
            List<ColumnMetadata> columns = columnMetadataFactory.create(dataFrame.schema(), request);
            DatasetSnapshot snapshot = snapshotMetadataFactory.create(request, schemaHash,
                    rowIdResult.getRowCount(), columns.size(), clock.millis());
            RahaDataset dataset = new RahaDataset(request.getDatasetId(), snapshot.getSnapshotId(),
                    request.getTableName(), request.getRowIdColumn(), columns, dataFrame,
                    schemaHash, Collections.emptyMap());
            LOGGER.info("外部文件数据读取完成，datasetId={}，snapshotId={}，rowCount={}，columnCount={}",
                    request.getDatasetId(), snapshot.getSnapshotId(),
                    snapshot.getRowCount(), snapshot.getColumnCount());
            return new LoadedDataset(dataset, snapshot);
        } catch (DataValidationException exception) {
            LOGGER.error("外部文件数据校验失败，datasetId={}，errorCode={}",
                    request.getDatasetId(), exception.getErrorCode(), exception);
            throw exception;
        } catch (Exception exception) {
            // 外部文件系统或 Spark 数据源异常统一转换为稳定数据加载错误。
            LOGGER.error("外部文件数据读取失败，datasetId={}，format={}",
                    request.getDatasetId(), request.getFormat(), exception);
            throw new DataValidationException(DataValidationErrorCode.DATA_LOAD_FAILED,
                    "外部文件数据读取失败", exception);
        }
    }
}
