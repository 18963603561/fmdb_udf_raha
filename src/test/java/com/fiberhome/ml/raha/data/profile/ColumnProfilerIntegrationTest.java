package com.fiberhome.ml.raha.data.profile;

import com.fiberhome.ml.raha.data.ColumnMetadata;
import com.fiberhome.ml.raha.data.ColumnProfile;
import com.fiberhome.ml.raha.data.RahaDataset;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.repository.DefaultColumnProfileRepository;
import com.fiberhome.ml.raha.repository.InMemoryRahaRepository;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import com.fiberhome.ml.raha.util.HashUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 使用 Spark 本地模式验证基础、数值、分位数、字符和类型画像。
 */
class ColumnProfilerIntegrationTest {

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldProfileAndPersistAllColumns() {
        RahaDataset dataset = dataset();
        InMemoryRahaRepository repository = new InMemoryRahaRepository();
        ColumnProfileService service = new ColumnProfileService(
                new ColumnProfiler(), new DefaultColumnProfileRepository(repository),
                Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC));

        RahaDataset profiled = service.profileAndSave(dataset,
                new ArtifactVersion("config-v1", "snapshot-v1", "profile-stage", 1));

        assertEquals(4, profiled.getProfiles().size());
        assertEquals(4, repository.size());
        ColumnProfile amount = profiled.getProfiles().get("amount");
        assertEquals(4L, amount.getTotalCount());
        assertEquals(0L, amount.getNullCount());
        assertEquals(4L, amount.getDistinctCount());
        assertEquals(3L, amount.getNumericCount());
        assertEquals(2L, amount.getTypeCounts().get("INTEGER"));
        assertEquals(1L, amount.getTypeCounts().get("DECIMAL"));
        assertEquals(0L, amount.getTypeCounts().get("ALPHANUMERIC"));
        assertEquals(1L, amount.getTypeCounts().get("LETTER"));
        assertEquals(0L, amount.getTypeCounts().get("MIXED"));
        assertEquals(0.75d, amount.getNumericRatio(), 0.0001d);
        assertEquals(10.0d, amount.getNumericMin(), 0.0001d);
        assertEquals(1000.0d, amount.getNumericMax(), 0.0001d);
        assertNotNull(amount.getNumericStandardDeviation());
        assertTrue(amount.getNumericStandardDeviation() > 0.0d);
        assertNotNull(amount.getNumericMedian());
        assertTrue(amount.getNumericQ1() <= amount.getNumericMedian());
        assertTrue(amount.getNumericMedian() <= amount.getNumericQ3());

        ColumnProfile code = profiled.getProfiles().get("code");
        assertEquals(4L, code.getNonNullCount());
        assertEquals(1.0d, code.getNonNullRatio(), 0.0001d);
        assertEquals(1L, code.getDuplicateCount());
        assertEquals(0.25d, code.getDuplicateRatio(), 0.0001d);
        assertEquals(2L, code.getTypeCounts().get("LETTER"));
        assertEquals(1L, code.getTypeCounts().get("ALPHANUMERIC"));
        assertEquals(1L, code.getTypeCounts().get("HAS_SYMBOL"));
        assertEquals(3, code.getValueHashFrequencies().size());
        assertEquals(2L, code.getValueHashFrequencies().get(HashUtils.sha256Hex("ABC")));

        ColumnProfile note = profiled.getProfiles().get("note");
        assertEquals(1L, note.getNullCount());
        assertEquals(3L, note.getNonNullCount());
        assertEquals(0.75d, note.getNonNullRatio(), 0.0001d);
        assertEquals(1L, note.getBlankCount());
        assertEquals(1, profiled.getProfiles().get("id").getMinLength());
        assertEquals(1, profiled.getProfiles().get("id").getMaxLength());
    }

    private static RahaDataset dataset() {
        SparkSession sparkSession = SparkTestSession.get();
        List<Row> rows = Arrays.asList(
                RowFactory.create("1", "10", "ABC", null),
                RowFactory.create("2", "20.5", "A1", "x y"),
                RowFactory.create("3", "1000", "###", ""),
                RowFactory.create("4", "bad", "ABC", "z")
        );
        StructType schema = new StructType()
                .add("id", DataTypes.StringType, false)
                .add("amount", DataTypes.StringType, true)
                .add("code", DataTypes.StringType, true)
                .add("note", DataTypes.StringType, true);
        Dataset<Row> dataFrame = sparkSession.createDataFrame(rows, schema);
        List<ColumnMetadata> columns = Arrays.asList(
                new ColumnMetadata("id", 0, "string", false, false, false),
                new ColumnMetadata("amount", 1, "string", true, true, false),
                new ColumnMetadata("code", 2, "string", true, true, false),
                new ColumnMetadata("note", 3, "string", true, true, false));
        return new RahaDataset("dataset", "snapshot-v1", "test_table", "id",
                columns, dataFrame, "schema-hash", Collections.<String, ColumnProfile>emptyMap());
    }
}
