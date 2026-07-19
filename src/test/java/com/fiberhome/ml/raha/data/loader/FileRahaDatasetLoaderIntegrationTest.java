package com.fiberhome.ml.raha.data.loader;

import com.fiberhome.ml.raha.data.loader.identity.RowIdentityColumns;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityService;
import com.fiberhome.ml.raha.data.loader.identity.RowIdValidator;
import com.fiberhome.ml.raha.data.loader.metadata.ColumnMetadataFactory;
import com.fiberhome.ml.raha.data.loader.metadata.SchemaHasher;
import com.fiberhome.ml.raha.data.loader.metadata.SnapshotMetadataFactory;
import com.fiberhome.ml.raha.data.loader.validation.DataValidationErrorCode;
import com.fiberhome.ml.raha.data.loader.validation.DataValidationException;
import com.fiberhome.ml.raha.data.domain.ColumnMetadata;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 使用 Spark 本地模式验证文件读取、行标识、模式哈希和快照元数据。
 */
class FileRahaDatasetLoaderIntegrationTest {

    @TempDir
    Path tempDir;

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldLoadCsvAndGenerateStableSnapshot() throws IOException {
        Path csv = writeCsv("valid.csv",
                "id,email,amount",
                "1,a@example.com,10.5",
                "2,b@example.com,20");
        DataLoadRequest request = new DataLoadRequest(
                "dataset", csv.toString(), "test_table",
                RowIdentityConfig.sourceKey("id"), DataFormat.CSV,
                csvOptions(), new LinkedHashSet<String>(Arrays.asList("email", "amount")),
                Collections.<String>emptySet(), Collections.singleton("email"),
                null, "source-v1");
        FileRahaDatasetLoader loader = loader();

        LoadedDataset first = loader.load(request);
        LoadedDataset second = loader.load(request);

        assertEquals(2L, first.getSnapshot().getRowCount());
        assertEquals(3, first.getSnapshot().getColumnCount());
        assertEquals(first.getSnapshot().getSnapshotId(), second.getSnapshot().getSnapshotId());
        assertEquals(first.getSnapshot().getSchemaHash(), second.getSnapshot().getSchemaHash());
        assertEquals(2L, first.getDataset().getDataFrame().count());
        ColumnMetadata id = first.getDataset().getColumns().get(0);
        ColumnMetadata email = first.getDataset().getColumns().get(1);
        assertFalse(id.isDetectable());
        assertTrue(email.isDetectable());
        assertTrue(email.isSensitive());
    }

    @Test
    void shouldDeduplicateConflictingSourceKey() throws IOException {
        Path csv = writeCsv("duplicate.csv", "id,name", "1,A", "1,B");

        LoadedDataset loaded = loader().load(request(csv, "id"));

        assertEquals(1L, loaded.getSnapshot().getRowCount());
        assertEquals(2L, ((Number) loaded.getDataset().getDataFrame().first()
                .getAs(RowIdentityColumns.DUPLICATE_COUNT)).longValue());
    }

    @Test
    void shouldRejectBlankRowId() throws IOException {
        Path csv = writeCsv("blank.csv", "id,name", "1,A", ",B");

        DataValidationException exception = assertThrows(DataValidationException.class,
                () -> loader().load(request(csv, "id")));

        assertEquals(DataValidationErrorCode.ROW_ID_NULL_OR_BLANK, exception.getErrorCode());
    }

    @Test
    void shouldRejectMissingRowIdColumn() throws IOException {
        Path csv = writeCsv("missing.csv", "code,name", "1,A", "2,B");

        DataValidationException exception = assertThrows(DataValidationException.class,
                () -> loader().load(request(csv, "id")));

        assertEquals(DataValidationErrorCode.ROW_KEY_COLUMN_MISSING,
                exception.getErrorCode());
    }

    @Test
    void shouldConvertFileFailureToStableErrorCode() {
        Path missing = tempDir.resolve("not-found.csv");

        DataValidationException exception = assertThrows(DataValidationException.class,
                () -> loader().load(request(missing, "id")));

        assertEquals(DataValidationErrorCode.DATA_LOAD_FAILED, exception.getErrorCode());
    }

    private FileRahaDatasetLoader loader() {
        SparkSession sparkSession = SparkTestSession.get();
        return new FileRahaDatasetLoader(sparkSession, new RowIdentityService(),
                new RowIdValidator(),
                new SchemaHasher(), new ColumnMetadataFactory(), new SnapshotMetadataFactory(),
                Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC));
    }

    private DataLoadRequest request(Path path, String rowIdColumn) {
        return new DataLoadRequest("dataset", path.toString(), "test_table",
                RowIdentityConfig.sourceKey(rowIdColumn),
                DataFormat.CSV, csvOptions(), Collections.<String>emptySet(),
                Collections.<String>emptySet(), Collections.<String>emptySet(),
                "snapshot-v1", "source-v1");
    }

    private static Map<String, String> csvOptions() {
        Map<String, String> options = new LinkedHashMap<String, String>();
        options.put("header", "true");
        options.put("inferSchema", "true");
        options.put("mode", "FAILFAST");
        return options;
    }

    private Path writeCsv(String fileName, String... lines) throws IOException {
        Path path = tempDir.resolve(fileName);
        Files.write(path, Arrays.asList(lines), StandardCharsets.UTF_8);
        return path;
    }
}
