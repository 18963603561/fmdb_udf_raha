package com.fiberhome.ml.raha.job;

import com.fiberhome.ml.raha.config.ConfigVersioner;
import com.fiberhome.ml.raha.config.RahaConfigValidator;
import com.fiberhome.ml.raha.config.RahaJobConfig;
import com.fiberhome.ml.raha.data.JobStatus;
import com.fiberhome.ml.raha.data.JobType;
import com.fiberhome.ml.raha.data.RahaDataset;
import com.fiberhome.ml.raha.data.loader.ColumnMetadataFactory;
import com.fiberhome.ml.raha.data.loader.DataFormat;
import com.fiberhome.ml.raha.data.loader.DataLoadRequest;
import com.fiberhome.ml.raha.data.loader.FileRahaDatasetLoader;
import com.fiberhome.ml.raha.data.loader.RowIdValidator;
import com.fiberhome.ml.raha.data.loader.SchemaHasher;
import com.fiberhome.ml.raha.data.loader.SnapshotMetadataFactory;
import com.fiberhome.ml.raha.data.profile.ColumnProfileService;
import com.fiberhome.ml.raha.data.profile.ColumnProfiler;
import com.fiberhome.ml.raha.repository.DefaultColumnProfileRepository;
import com.fiberhome.ml.raha.repository.DefaultJobRepository;
import com.fiberhome.ml.raha.repository.DefaultStageRepository;
import com.fiberhome.ml.raha.repository.InMemoryRahaRepository;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 验证迭代 2 的实际文件读取、快照绑定、画像生成和仓储持久化链路。
 */
class Iteration2PipelineIntegrationTest {

    @TempDir
    Path tempDir;

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldRunLoadAndProfilePipelineEndToEnd() throws IOException {
        Path csv = tempDir.resolve("pipeline.csv");
        Files.write(csv, Arrays.asList(
                "id,name,amount",
                "1,Alice,10",
                "2,Bob,20.5",
                "3,Carol,30"), StandardCharsets.UTF_8);
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC);
        InMemoryRahaRepository repository = new InMemoryRahaRepository();
        RahaJobConfig config = RahaJobConfig.defaults(
                JobType.DETECTION, "dataset", csv.toString(), "id");
        RahaJobOrchestrator orchestrator = new RahaJobOrchestrator(
                new RahaConfigValidator(), new ConfigVersioner(),
                new IdempotencyKeyGenerator(), new DefaultRahaIdGenerator(),
                new StageFailureDecider(), new DefaultJobRepository(repository),
                new DefaultStageRepository(repository), clock);
        FileRahaDatasetLoader loader = new FileRahaDatasetLoader(
                SparkTestSession.get(), new RowIdValidator(), new SchemaHasher(),
                new ColumnMetadataFactory(), new SnapshotMetadataFactory(), clock);
        ColumnProfileService profileService = new ColumnProfileService(
                new ColumnProfiler(), new DefaultColumnProfileRepository(repository), clock);
        DataLoadRequest loadRequest = new DataLoadRequest(
                "dataset", csv.toString(), "test_table", "id", DataFormat.CSV,
                csvOptions(), Collections.<String>emptySet(), Collections.<String>emptySet(),
                Collections.<String>emptySet(), null, "source-v1");

        RahaJob job = orchestrator.submit(config);
        JobRunResult result = orchestrator.execute(job, config, Arrays.asList(
                new DataLoadStageHandler(loader, loadRequest),
                new ColumnProfileStageHandler(profileService)));

        assertEquals(JobStatus.SUCCEEDED, result.getJob().getStatus());
        assertNotNull(result.getJob().getSnapshotId());
        RahaDataset dataset = (RahaDataset) result.getAttributes().get(StageAttributeKeys.RAHA_DATASET);
        assertNotNull(dataset);
        assertEquals(3L, dataset.getDataFrame().count());
        assertEquals(3, dataset.getProfiles().size());
        assertEquals(6, repository.size());
    }

    private static Map<String, String> csvOptions() {
        Map<String, String> options = new LinkedHashMap<String, String>();
        options.put("header", "true");
        options.put("inferSchema", "true");
        return options;
    }
}

