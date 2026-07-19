package com.fiberhome.ml.raha.fmdb;

import com.fiberhome.ml.raha.data.domain.ColumnMetadata;
import com.fiberhome.ml.raha.data.domain.DatasetSnapshot;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.loader.DataFormat;
import com.fiberhome.ml.raha.data.loader.DataLoadRequest;
import com.fiberhome.ml.raha.data.loader.DataValidationErrorCode;
import com.fiberhome.ml.raha.data.loader.DataValidationException;
import com.fiberhome.ml.raha.data.loader.LoadedDataset;
import com.fiberhome.ml.raha.data.loader.RahaDatasetLoader;
import com.fiberhome.ml.raha.data.loader.RowIdValidationResult;
import com.fiberhome.ml.raha.data.loader.RowIdValidator;
import com.fiberhome.ml.raha.data.loader.RowIdentityColumns;
import com.fiberhome.ml.raha.data.loader.RowIdentityResult;
import com.fiberhome.ml.raha.data.loader.RowIdentityService;
import com.fiberhome.ml.raha.data.loader.SchemaHasher;
import com.fiberhome.ml.raha.data.loader.SnapshotMetadataFactory;
import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 通过 FMDB Spark Catalog 或只读 SQL 加载表级数据，并转换为 Raha 数据集。
 */
public final class FmdbDatasetLoader implements RahaDatasetLoader {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(FmdbDatasetLoader.class);
    /** 由 FMDB 平台初始化的 Spark 会话。 */
    private final SparkSession sparkSession;
    /** 稳定行标识校验器。 */
    private final RowIdValidator rowIdValidator;
    /** 行身份生成和逻辑去重服务。 */
    private final RowIdentityService rowIdentityService;
    /** 模式哈希生成器。 */
    private final SchemaHasher schemaHasher;
    /** FMDB 模式解析器。 */
    private final FmdbSchemaResolver schemaResolver;
    /** 快照元数据生成器。 */
    private final SnapshotMetadataFactory snapshotFactory;
    /** 提供可测试审计时间的时钟。 */
    private final Clock clock;

    public FmdbDatasetLoader(SparkSession sparkSession,
                             RowIdentityService rowIdentityService,
                             RowIdValidator rowIdValidator,
                             SchemaHasher schemaHasher,
                             FmdbSchemaResolver schemaResolver,
                             SnapshotMetadataFactory snapshotFactory,
                             Clock clock) {
        if (sparkSession == null || rowIdentityService == null
                || rowIdValidator == null || schemaHasher == null
                || schemaResolver == null || snapshotFactory == null || clock == null) {
            throw new IllegalArgumentException("FMDB 数据加载器依赖不能为空");
        }
        this.sparkSession = sparkSession;
        this.rowIdentityService = rowIdentityService;
        this.rowIdValidator = rowIdValidator;
        this.schemaHasher = schemaHasher;
        this.schemaResolver = schemaResolver;
        this.snapshotFactory = snapshotFactory;
        this.clock = clock;
    }

    @Override
    public LoadedDataset load(DataLoadRequest request) {
        if (request == null || (request.getFormat() != DataFormat.FMDB_TABLE
                && request.getFormat() != DataFormat.FMDB_SQL)) {
            throw new IllegalArgumentException("FMDB 加载请求必须使用表或 SQL 格式");
        }
        long startedAt = clock.millis();
        LOGGER.info("开始读取 FMDB 数据，datasetId={}，sourceType={}，tableName={}",
                request.getDatasetId(), request.getFormat(), request.getTableName());
        try {
            Dataset<Row> source = request.getFormat() == DataFormat.FMDB_TABLE
                    ? readTable(request.getInputReference())
                    : readSql(request.getInputReference());
            org.apache.spark.sql.types.StructType businessSchema =
                    rowIdentityService.businessSchema(source.schema());
            String schemaHash = schemaHasher.hash(businessSchema);
            List<ColumnMetadata> columns = schemaResolver.resolve(
                    businessSchema, request);
            RowIdentityResult identity = rowIdentityService.identify(
                    source, request.getRowIdentityConfig());
            Dataset<Row> frame = identity.getDataFrame();
            RowIdValidationResult rowId = rowIdValidator.validate(
                    frame, RowIdentityColumns.ROW_ID);
            DatasetSnapshot snapshot = snapshotFactory.create(request, schemaHash,
                    rowId.getRowCount(), columns.size(), clock.millis());
            RahaDataset dataset = new RahaDataset(request.getDatasetId(),
                    snapshot.getSnapshotId(), request.getTableName(),
                    RowIdentityColumns.ROW_ID, columns, frame, schemaHash,
                    Collections.emptyMap());
            LOGGER.info("FMDB 数据读取完成，datasetId={}，snapshotId={}，"
                            + "sourceRowCount={}，logicalRowCount={}，columnCount={}，"
                            + "elapsedMillis={}",
                    request.getDatasetId(), snapshot.getSnapshotId(),
                    identity.getMetrics().getSourceRowCount(), snapshot.getRowCount(),
                    snapshot.getColumnCount(),
                    clock.millis() - startedAt);
            return new LoadedDataset(dataset, snapshot);
        } catch (DataValidationException exception) {
            LOGGER.error("FMDB 数据校验失败，datasetId={}，errorCode={}",
                    request.getDatasetId(), exception.getErrorCode(), exception);
            throw exception;
        } catch (RuntimeException exception) {
            // Catalog、SQL 解析或 FMDB 外部读取异常统一转换为稳定加载错误。
            LOGGER.error("FMDB 数据读取失败，datasetId={}，sourceType={}",
                    request.getDatasetId(), request.getFormat(), exception);
            throw new DataValidationException(DataValidationErrorCode.DATA_LOAD_FAILED,
                    "FMDB 数据读取失败", exception);
        }
    }

    private Dataset<Row> readTable(String tableName) {
        LOGGER.debug("调用 FMDB Catalog 读取表，tableName={}", tableName);
        return sparkSession.table(tableName);
    }

    private Dataset<Row> readSql(String sqlText) {
        String normalized = sqlText.trim().toLowerCase(Locale.ROOT);
        // FMDB 数据加载只允许查询语句，禁止通过适配器修改输入表或执行管理命令。
        if (!normalized.startsWith("select ") && !normalized.startsWith("with ")) {
            throw new IllegalArgumentException("FMDB SQL 数据源只允许 SELECT 或 WITH 查询");
        }
        LOGGER.debug("调用 FMDB 执行只读 SQL，sqlLength={}", sqlText.length());
        return sparkSession.sql(sqlText);
    }
}
