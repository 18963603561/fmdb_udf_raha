package com.fiberhome.ml.raha.data.loader;

import com.fiberhome.ml.raha.data.domain.ColumnMetadata;
import com.fiberhome.ml.raha.data.domain.DatasetSnapshot;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    /** 行身份生成和逻辑去重服务。 */
    private final RowIdentityService rowIdentityService;
    /** 模式哈希生成器。 */
    private final SchemaHasher schemaHasher;
    /** 字段元数据生成器。 */
    private final ColumnMetadataFactory columnMetadataFactory;
    /** 快照元数据生成器。 */
    private final SnapshotMetadataFactory snapshotMetadataFactory;
    /** 提供可测试时间的时钟。 */
    private final Clock clock;

    public FileRahaDatasetLoader(SparkSession sparkSession,
                                 RowIdentityService rowIdentityService,
                                 RowIdValidator rowIdValidator,
                                 SchemaHasher schemaHasher,
                                 ColumnMetadataFactory columnMetadataFactory,
                                 SnapshotMetadataFactory snapshotMetadataFactory,
                                 Clock clock) {
        if (sparkSession == null || rowIdentityService == null
                || rowIdValidator == null || schemaHasher == null
                || columnMetadataFactory == null || snapshotMetadataFactory == null || clock == null) {
            throw new IllegalArgumentException("文件数据加载器依赖不能为空");
        }
        this.sparkSession = sparkSession;
        this.rowIdentityService = rowIdentityService;
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
        if (!request.getFormat().isFileFormat()) {
            throw new IllegalArgumentException("文件加载器不支持 FMDB 表或 SQL 数据源");
        }
        LOGGER.info("开始读取外部文件数据，datasetId={}，format={}，optionKeys={}",
                request.getDatasetId(), request.getFormat(), request.getOptions().keySet());
        try {
            DataFrameReader reader = sparkSession.read().format(request.getFormat().getSparkFormat());
            for (Map.Entry<String, String> option : request.getOptions().entrySet()) {
                reader = reader.option(option.getKey(), option.getValue());
            }
            Dataset<Row> source = reader.load(request.getInputReference());
            org.apache.spark.sql.types.StructType businessSchema =
                    rowIdentityService.businessSchema(source.schema());
            String schemaHash = schemaHasher.hash(businessSchema);
            List<ColumnMetadata> columns = columnMetadataFactory.create(
                    businessSchema, request);
            RowIdentityResult identity = rowIdentityService.identify(
                    source, request.getRowIdentityConfig());
            Dataset<Row> dataFrame = identity.getDataFrame();
            RowIdValidationResult rowIdResult = rowIdValidator.validate(
                    dataFrame, RowIdentityColumns.ROW_ID);
            DatasetSnapshot snapshot = snapshotMetadataFactory.create(request, schemaHash,
                    rowIdResult.getRowCount(), columns.size(), clock.millis());
            RahaDataset dataset = new RahaDataset(request.getDatasetId(), snapshot.getSnapshotId(),
                    request.getTableName(), RowIdentityColumns.ROW_ID, columns, dataFrame,
                    schemaHash, Collections.emptyMap());
            LOGGER.info("外部文件数据读取完成，datasetId={}，snapshotId={}，"
                            + "sourceRowCount={}，logicalRowCount={}，columnCount={}",
                    request.getDatasetId(), snapshot.getSnapshotId(),
                    identity.getMetrics().getSourceRowCount(), snapshot.getRowCount(),
                    snapshot.getColumnCount());
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
