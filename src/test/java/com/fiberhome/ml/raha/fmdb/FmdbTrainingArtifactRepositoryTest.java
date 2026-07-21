package com.fiberhome.ml.raha.fmdb;

import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.data.domain.ColumnMetadata;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityColumns;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyMetrics;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.feature.domain.FeatureDefinition;
import com.fiberhome.ml.raha.feature.domain.FeatureDictionary;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import com.fiberhome.ml.raha.data.type.FeatureType;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.data.type.LabelSource;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationMetrics;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationResult;
import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.InMemoryFmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbTrainingArtifactRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbTrainingCellRecord;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbTrainingColumnArtifactRecord;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbTrainingExampleRecord;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPhysicalTable;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbPersistenceConfig;
import com.fiberhome.ml.raha.service.train.TrainingArtifactMaterializationResult;
import com.fiberhome.ml.raha.service.train.TrainingArtifactMaterializationService;
import com.fiberhome.ml.raha.service.train.TrainingMergeMetrics;
import com.fiberhome.ml.raha.service.train.TrainingMergeResult;
import com.fiberhome.ml.raha.util.HashUtils;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** 验证训练三类最终物理产物的模式、开关和幂等追加。 */
class FmdbTrainingArtifactRepositoryTest {

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldAppendTrainingArtifactsIdempotentlyAndReadByBatch() {
        InMemoryFmdbTableGateway gateway = new InMemoryFmdbTableGateway(
                SparkTestSession.get());
        FmdbPersistenceConfig config = FmdbPersistenceConfig.builder()
                .table(FmdbPhysicalTable.TRAINING_CELL, true)
                .build();
        FmdbTrainingArtifactRepository repository =
                new FmdbTrainingArtifactRepository(SparkTestSession.get(), gateway,
                        config);
        FmdbTrainingColumnArtifactRecord column = new FmdbTrainingColumnArtifactRecord(
                "training-1", "dataset-1", "source-v1", "schema-1", "merge-v1",
                "{\"mergedCount\":1}", "value", "profile-v1", "{}",
                "strategy-v1", "{}", "dict-v1", "{}", null, null, null,
                2000L);
        FmdbTrainingCellRecord cell = new FmdbTrainingCellRecord(
                "training-1", "dataset-1", "snapshot-1", "row-1", "value",
                "cell-1", "bad", "dict-v1", "{\"0\":1.0}", "{}", "cluster-1",
                0.1d, 1, null, "HUMAN", "annotation-1", 1.0d, 2000L);
        FmdbTrainingExampleRecord example = new FmdbTrainingExampleRecord(
                "model-set-1", "training-1", "dataset-1", "row-1", "value",
                "cell-1", "bad", "dict-v1", "{\"0\":1.0}", 1, "HUMAN",
                "annotation-1", 1.0d, "cluster-1", 2000L, "2026-07");

        assertEquals(1L, repository.saveColumnArtifacts(
                Collections.singletonList(column)));
        assertEquals(0L, repository.saveColumnArtifacts(
                Collections.singletonList(column)));
        assertEquals(1L, repository.saveTrainingCells(
                Collections.singletonList(cell)));
        assertEquals(1L, repository.saveTrainingExamples(
                Collections.singletonList(example)));
        Dataset<Row> cells = repository.findTrainingCells("dataset-1", "training-1",
                "value");
        Dataset<Row> examples = repository.findTrainingExamples("dataset-1", "2026-07",
                "model-set-1", "value");
        assertEquals(1L, cells.count());
        assertEquals(1L, examples.count());
        assertEquals("bad", cells.first().getAs("cell_value"));
        assertEquals(1, ((Number) examples.first().getAs("label")).intValue());
    }

    @Test
    void shouldMaterializeTrustedCellValueAndOnlyLabeledExamples() {
        InMemoryFmdbTableGateway gateway = new InMemoryFmdbTableGateway(
                SparkTestSession.get());
        FmdbPersistenceConfig config = FmdbPersistenceConfig.builder()
                .table(FmdbPhysicalTable.TRAINING_CELL, true)
                .build();
        FmdbTrainingArtifactRepository repository =
                new FmdbTrainingArtifactRepository(SparkTestSession.get(), gateway,
                        config);
        StructType schema = new StructType()
                .add("id", DataTypes.StringType, false)
                .add("value", DataTypes.StringType, true)
                .add(RowIdentityColumns.ROW_ID, DataTypes.StringType, false)
                .add(RowIdentityColumns.ROW_CONTENT_HASH, DataTypes.StringType, false)
                .add(RowIdentityColumns.DUPLICATE_COUNT, DataTypes.LongType, false);
        Row row = RowFactory.create("A", "bad", "row-1", "hash-1", 1L);
        RahaDataset dataset = new RahaDataset("dataset-1", "training-snapshot",
                "orders", RowIdentityColumns.ROW_ID,
                Arrays.asList(new ColumnMetadata("id", 0, "string", false,
                                false, false),
                        new ColumnMetadata("value", 1, "string", true, true,
                                false)),
                SparkTestSession.get().createDataFrame(Collections.singletonList(row),
                        schema), "schema-1", Collections.emptyMap());
        CellCoordinate coordinate = new CellCoordinate("dataset-1",
                "training-snapshot", "row-1", "value");
        SparseFeatureRow feature = new SparseFeatureRow(coordinate.toCellId(),
                "value", coordinate, HashUtils.md5Hex("bad"), "bad",
                "dict-v1", Collections.singletonMap(0, 1.0d),
                Collections.singletonMap("hit", "1"));
        Map<Integer, FeatureDefinition> definitions = new LinkedHashMap<Integer,
                FeatureDefinition>();
        definitions.put(0, new FeatureDefinition(0, "hit", FeatureType.BINARY,
                "test", 0.0d));
        FeatureDictionary dictionary = new FeatureDictionary("dict-v1", "value",
                definitions, 1000L);
        FeatureAssemblyResult features = new FeatureAssemblyResult(
                Collections.singletonMap("value", dictionary),
                Collections.singletonList(feature),
                new FeatureAssemblyMetrics(1L, 1L, 1L, 0L));
        CellLabel label = new CellLabel(coordinate.toCellId(), 1, LabelSource.HUMAN,
                1.0d, null, null, "tester", 1000L);
        LabelPropagationResult propagation = new LabelPropagationResult(
                Collections.singletonList(label), Collections.emptyList(),
                new LabelPropagationMetrics(1L, 0L, 0L, 0L));
        Map<String, Object> context = new LinkedHashMap<String, Object>();
        context.put("annotationBatchId", "annotation-1");
        TrainingMergeResult merge = new TrainingMergeResult(dataset,
                Collections.singletonList(label), "training-1", "training-snapshot",
                "merge-v1", new TrainingMergeMetrics(1L, 1L, 1L, 1L, 0L, 0L,
                0L, 0L), context);
        TrainingArtifactMaterializationResult result =
                new TrainingArtifactMaterializationService(repository,
                        fixedClock(2000L)).materialize(merge, features, null,
                        propagation, "model-set-1", "strategy-v1", null, null);

        assertEquals(1, result.getMaterializedCellCount());
        assertEquals(1, result.getMaterializedExampleCount());
        assertEquals("bad", String.valueOf((Object) gateway.read(
                FmdbPhysicalTable.TRAINING_CELL.getTableName()).first()
                .getAs("cell_value")));
        assertEquals(1L, gateway.read(FmdbPhysicalTable.TRAINING_EXAMPLE
                .getTableName()).count());
    }

    private static Clock fixedClock(long millis) {
        return Clock.fixed(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
    }
}
