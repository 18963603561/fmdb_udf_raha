package com.fiberhome.ml.raha.evaluation;

import com.fiberhome.ml.raha.data.ColumnMetadata;
import com.fiberhome.ml.raha.data.RahaDataset;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.repository.CellLabelRepository;
import com.fiberhome.ml.raha.repository.DefaultCellLabelRepository;
import com.fiberhome.ml.raha.repository.InMemoryRahaRepository;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证 Spark 全量真值差异、标签持久化和行集合不一致拒绝逻辑。
 */
class GroundTruthDifferenceServiceIntegrationTest {

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldGenerateAndPersistCellGroundTruthLabels() {
        CellLabelRepository repository = new DefaultCellLabelRepository(
                new InMemoryRahaRepository());
        GroundTruthDifferenceService service = new GroundTruthDifferenceService(
                repository, fixedClock());
        RahaDataset dirty = dataset("dataset", "snapshot-v1", Arrays.asList(
                RowFactory.create("1", "ABC"), RowFactory.create("2", "BAD")));
        RahaDataset truth = dataset("truth", "truth-v1", Arrays.asList(
                RowFactory.create("1", "ABC"), RowFactory.create("2", "ABC")));

        GroundTruthDifferenceResult result = service.compareAndSave(
                "evaluation-job", dirty, truth, version());

        assertEquals(2, result.getLabels().size());
        assertEquals(1L, result.getPositiveCount());
        assertEquals(1L, result.getNegativeCount());
        assertEquals(2, repository.findByJob("evaluation-job").size());
    }

    @Test
    void shouldRejectDifferentRowSets() {
        GroundTruthDifferenceService service = new GroundTruthDifferenceService(
                new DefaultCellLabelRepository(new InMemoryRahaRepository()), fixedClock());
        RahaDataset dirty = dataset("dataset", "snapshot-v1", Arrays.asList(
                RowFactory.create("1", "ABC"), RowFactory.create("2", "BAD")));
        RahaDataset truth = dataset("truth", "truth-v1", Arrays.asList(
                RowFactory.create("1", "ABC"), RowFactory.create("3", "ABC")));

        assertThrows(IllegalArgumentException.class,
                () -> service.compareAndSave("evaluation-job", dirty, truth, version()));
    }

    private static RahaDataset dataset(String datasetId,
                                       String snapshotId,
                                       List<Row> rows) {
        StructType schema = new StructType()
                .add("id", DataTypes.StringType, false)
                .add("code", DataTypes.StringType, true);
        return new RahaDataset(datasetId, snapshotId, datasetId, "id",
                Arrays.asList(
                        new ColumnMetadata("id", 0, "string", false, false, false),
                        new ColumnMetadata("code", 1, "string", true, true, false)),
                SparkTestSession.get().createDataFrame(rows, schema),
                "schema-v1", Collections.emptyMap());
    }

    private static Clock fixedClock() {
        return Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC);
    }

    private static ArtifactVersion version() {
        return new ArtifactVersion("config-v1", "snapshot-v1", "truth-stage", 1);
    }
}
