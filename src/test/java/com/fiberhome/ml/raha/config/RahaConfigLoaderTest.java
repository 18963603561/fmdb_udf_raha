package com.fiberhome.ml.raha.config;

import com.fiberhome.ml.raha.data.JobType;
import com.fiberhome.ml.raha.data.StrategyFamily;
import com.fiberhome.ml.raha.performance.BenchmarkDatasetSpec;
import com.fiberhome.ml.raha.performance.CapacityBand;
import com.fiberhome.ml.raha.performance.ProductionResourceAdvisor;
import com.fiberhome.ml.raha.performance.ProductionResourceRecommendation;
import com.fiberhome.ml.raha.retention.RetentionTableRule;
import com.fiberhome.ml.raha.security.ResultValueMode;
import com.fiberhome.ml.raha.security.ResultValueProtectionPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证类路径默认配置、外部文件覆盖、系统属性覆盖和错误定位。
 */
class RahaConfigLoaderTest {

    /** JUnit 提供的隔离配置目录。 */
    @TempDir
    Path temporaryDirectory;

    @Test
    void shouldBuildAllDefaultConfigurationGroupsFromBundledResource() {
        RahaConfigFactory factory = new RahaConfigFactory(
                new RahaConfigLoader().load());
        RahaJobConfig job = factory.jobConfig(JobType.DETECTION,
                "dataset", "source_table", "id");
        ResultValueProtectionPolicy protection =
                factory.resultValueProtectionPolicy();
        List<BenchmarkDatasetSpec> benchmarks = factory.benchmarkDatasetSpecs();

        new RahaConfigValidator().validate(job);
        assertFalse(job.isSaveIntermediate());
        assertEquals(20260714L, job.getRandomSeed());
        assertEquals(30, job.getResultRetentionDays());
        assertEquals(1000, job.getStrategyConfig().getMaxStrategyCount());
        assertTrue(job.getStrategyConfig().getStrategyFamilies()
                .contains(StrategyFamily.RVD));
        assertEquals(0.01d, job.getFeatureConfig().getRareValueRatio(), 0.000001d);
        assertEquals(0.5d, job.getModelConfig().getThreshold(), 0.000001d);
        assertEquals(4, job.getResourceConfig().getMaxParallelColumns());
        assertEquals(20, job.getSamplingConfig().getLabelingBudget());
        assertEquals(100, factory.logisticRegressionTrainingConfig()
                .getMaxIterations());
        assertEquals(0.5d, factory.labelPropagationConfig()
                .getMaxDirectWeightRatio(), 0.000001d);
        assertEquals("F_DW_RAHADETECT", factory.udfConfig().getDetectFunction());
        assertEquals(ResultValueMode.MASKED, protection.getMode());
        assertTrue(protection.isAllColumnsSensitive());
        assertEquals(1, protection.getVisiblePrefix());
        assertEquals(4, benchmarks.size());
        assertEquals("wide-normal-error", benchmarks.get(3).getName());
        assertEquals(7, new RetentionTableRule(
                "raha_intermediate", "updated_at").getRetentionDays());
    }

    @Test
    void shouldMergeExternalOverridesAndChangeConfigurationVersion() throws Exception {
        Path override = temporaryDirectory.resolve("raha-override.properties");
        Files.write(override, Arrays.asList(
                "raha.strategy.max-count=77",
                "raha.resource.max-parallel-columns=9",
                "raha.feature.rare-value-ratio=0.05",
                "raha.profile.quantile-accuracy=20000",
                "raha.model.context-signal.null-weight=0.25",
                "raha.label.max-direct-weight-ratio=0.25",
                "raha.sampling.coverage-score-exponent-cap=10.0",
                "raha.udf.detect-function=F_CUSTOM_DETECT",
                "raha.security.result.mode=HASH_ONLY",
                "raha.performance.benchmark.small-low-error.rows=12345",
                "raha.performance.partition.target-rows=1000"),
                StandardCharsets.UTF_8);
        RahaConfigFactory defaults = new RahaConfigFactory(
                new RahaConfigLoader().load());
        RahaConfigFactory overridden = new RahaConfigFactory(
                new RahaConfigLoader().load(override));

        RahaJobConfig defaultJob = defaults.jobConfig(JobType.DETECTION,
                "dataset", "source_table", "id");
        RahaJobConfig overriddenJob = overridden.jobConfig(JobType.DETECTION,
                "dataset", "source_table", "id");
        ProductionResourceRecommendation recommendation =
                new ProductionResourceAdvisor(overridden.productionResourceSettings())
                        .recommend(500000L, 20, 512L * 1024L * 1024L);

        assertEquals(77, overriddenJob.getStrategyConfig().getMaxStrategyCount());
        assertEquals(9, overriddenJob.getResourceConfig().getMaxParallelColumns());
        assertEquals(0.05d, overriddenJob.getFeatureConfig()
                .getRareValueRatio(), 0.000001d);
        assertEquals(0.25d, overriddenJob.getModelConfig()
                .getNullContextSignalWeight(), 0.000001d);
        assertEquals(0.25d, overridden.labelPropagationConfig()
                .getMaxDirectWeightRatio(), 0.000001d);
        assertEquals(10.0d, overriddenJob.getSamplingConfig()
                .getCoverageScoreExponentCap(), 0.000001d);
        assertEquals("F_CUSTOM_DETECT", overridden.udfConfig().getDetectFunction());
        assertEquals(ResultValueMode.HASH_ONLY,
                overridden.resultValueProtectionPolicy().getMode());
        assertEquals(12345L, overridden.benchmarkDatasetSpecs().get(0).getRowCount());
        assertEquals(CapacityBand.SMALL, recommendation.getCapacityBand());
        assertEquals(500, recommendation.getPartitionCount());
        assertNotEquals(new ConfigVersioner().versionOf(defaultJob),
                new ConfigVersioner().versionOf(overriddenJob));
        assertNotEquals(defaultJob.getExecutionConfigFingerprint(),
                overriddenJob.getExecutionConfigFingerprint());
    }

    @Test
    void shouldApplySystemPropertyAfterBundledResourceAndExternalFile() throws Exception {
        String key = "raha.resource.max-parallel-columns";
        String previous = System.getProperty(key);
        Path override = temporaryDirectory.resolve("system-priority.properties");
        Files.write(override, Arrays.asList(key + "=6"), StandardCharsets.UTF_8);
        try {
            System.setProperty(key, "7");

            ResourceConfig config = new RahaConfigFactory(
                    new RahaConfigLoader().load(override)).resourceConfig();

            assertEquals(7, config.getMaxParallelColumns());
        } finally {
            if (previous == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, previous);
            }
        }
    }

    @Test
    void shouldReportInvalidPropertyKeyAndMissingExternalFile() throws Exception {
        Path invalid = temporaryDirectory.resolve("invalid.properties");
        Path unknown = temporaryDirectory.resolve("unknown.properties");
        Files.write(invalid, Arrays.asList(
                "raha.feature.trim-value=not-boolean"), StandardCharsets.UTF_8);
        Files.write(unknown, Arrays.asList(
                "raha.feature.unknown-option=true"), StandardCharsets.UTF_8);

        RahaConfigurationException invalidValue = assertThrows(
                RahaConfigurationException.class,
                () -> new RahaConfigFactory(new RahaConfigLoader().load(invalid))
                        .featureConfig());
        RahaConfigurationException missingFile = assertThrows(
                RahaConfigurationException.class,
                () -> new RahaConfigLoader().load(
                        temporaryDirectory.resolve("missing.properties")));
        RahaConfigurationException unknownKey = assertThrows(
                RahaConfigurationException.class,
                () -> new RahaConfigLoader().load(unknown));

        assertEquals("raha.feature.trim-value", invalidValue.getPropertyKey());
        assertTrue(missingFile.getMessage().contains("missing.properties"));
        assertEquals("raha.feature.unknown-option", unknownKey.getPropertyKey());
    }
}
