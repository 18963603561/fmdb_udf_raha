package com.fiberhome.ml.raha.label;

import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.loader.ColumnMetadataFactory;
import com.fiberhome.ml.raha.data.loader.DataFormat;
import com.fiberhome.ml.raha.data.loader.DataLoadRequest;
import com.fiberhome.ml.raha.data.loader.FileRahaDatasetLoader;
import com.fiberhome.ml.raha.data.loader.LoadedDataset;
import com.fiberhome.ml.raha.data.loader.RowIdValidator;
import com.fiberhome.ml.raha.data.loader.SchemaHasher;
import com.fiberhome.ml.raha.data.loader.SnapshotMetadataFactory;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.data.type.LabelSource;
import com.fiberhome.ml.raha.sampling.domain.AnnotationTask;
import com.fiberhome.ml.raha.sampling.domain.AnnotationTaskStatus;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证真值自动标注只在评测模式工作且只生成零一标签。
 */
class GroundTruthLabelAdapterIntegrationTest {

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldGenerateGroundTruthLabelsOnlyInEvaluationMode() throws Exception {
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC);
        RahaDataset dirty = load("alignment/iteration5-dirty.csv", "dirty", clock);
        RahaDataset clean = load("alignment/iteration5-clean.csv", "clean", clock);
        AnnotationTask task = new AnnotationTask("task", "job", "6", 1, 1.0d,
                Collections.singletonMap("amount", "cluster-v1|cluster-1"),
                "sampling-v1", 500L, 2000L);
        GroundTruthLabelAdapter adapter = new GroundTruthLabelAdapter(clock);

        GroundTruthLabelingResult result = adapter.label(
                JobType.EVALUATION, task, dirty, clean);

        assertEquals(5, result.getLabels().size());
        assertEquals(1, result.getLabels().stream().filter(
                label -> label.getLabel() == 1).count());
        assertEquals(LabelSource.GROUND_TRUTH,
                result.getLabels().get(0).getLabelSource());
        assertEquals(AnnotationTaskStatus.COMPLETED,
                result.getCompletedTask().getStatus());
        assertThrows(IllegalStateException.class, () -> adapter.label(
                JobType.DETECTION, task, dirty, clean));
    }

    private static RahaDataset load(String resource,
                                    String datasetId,
                                    Clock clock) throws URISyntaxException {
        Path path = Paths.get(GroundTruthLabelAdapterIntegrationTest.class
                .getClassLoader().getResource(resource).toURI());
        FileRahaDatasetLoader loader = new FileRahaDatasetLoader(
                SparkTestSession.get(), new RowIdValidator(), new SchemaHasher(),
                new ColumnMetadataFactory(), new SnapshotMetadataFactory(), clock);
        LoadedDataset loaded = loader.load(new DataLoadRequest(
                datasetId, path.toString(), datasetId, "id", DataFormat.CSV,
                csvOptions(), Collections.<String>emptySet(), Collections.<String>emptySet(),
                Collections.<String>emptySet(), null, "source-v1"));
        return loaded.getDataset();
    }

    private static Map<String, String> csvOptions() {
        Map<String, String> options = new LinkedHashMap<String, String>();
        options.put("header", "true");
        options.put("inferSchema", "false");
        return options;
    }
}
