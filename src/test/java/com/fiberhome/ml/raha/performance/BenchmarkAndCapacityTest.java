package com.fiberhome.ml.raha.performance;

import com.fiberhome.ml.raha.production.ProductionReadinessChecker;
import com.fiberhome.ml.raha.production.ProductionReadinessContext;
import com.fiberhome.ml.raha.production.ProductionReadinessReport;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.List;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证基准数据维度、阶段资源指标、资源建议和生产就绪门禁。
 */
class BenchmarkAndCapacityTest {

    @Test
    void shouldCoverScaleWidthAndErrorRateDimensions() {
        List<BenchmarkDatasetSpec> specs = BenchmarkDatasetCatalog.standard();
        Set<BenchmarkScale> scales = new HashSet<BenchmarkScale>();
        Set<Double> errorRates = new HashSet<Double>();
        int maximumColumns = 0;
        for (BenchmarkDatasetSpec spec : specs) {
            scales.add(spec.getScale());
            errorRates.add(spec.getErrorRate());
            maximumColumns = Math.max(maximumColumns, spec.getDataColumnCount());
        }

        assertEquals(4, specs.size());
        assertEquals(4, scales.size());
        assertTrue(errorRates.size() >= 3);
        assertTrue(maximumColumns >= 200);
    }

    @Test
    void shouldMeasureStageTimeAndHeapUsage() {
        StagePerformanceMonitor monitor = new StagePerformanceMonitor(
                Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC));
        StageExecutionContext context = new StageExecutionContext(
                1000L, 10, 30, 8, true);

        MeasuredStageResult<Integer> result = monitor.measure(
                "strategy", context, () -> 42);

        assertEquals(42, result.getResult().intValue());
        assertEquals("strategy", result.getMetric().getStageName());
        assertTrue(result.getMetric().isSucceeded());
        assertTrue(result.getMetric().getElapsedMillis() >= 0L);
        assertTrue(result.getMetric().getUsedHeapBeforeBytes() >= 0L);
        assertEquals(8, result.getMetric().getContext().getPartitionCount());
    }

    @Test
    void shouldCaptureClusterResourceDeltasAndBuildMultiStageReport() {
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC);
        AtomicInteger captures = new AtomicInteger();
        StageResourceProbe probe = () -> {
            int sequence = captures.getAndIncrement();
            return new StageResourceSnapshot(100L + sequence * 50L,
                    200L + sequence * 80L, 300L + sequence * 120L,
                    400L + sequence * 160L, 1000L + sequence);
        };
        StagePerformanceMonitor monitor = new StagePerformanceMonitor(clock, probe);
        StageExecutionContext context = new StageExecutionContext(
                1000L, 10, 20, 8, false);

        StagePerformanceMetric first = monitor.measure(
                "profile", context, () -> "ok").getMetric();
        StagePerformanceMetric second = monitor.measure(
                "strategy", context, () -> "ok").getMetric();
        PerformanceBaselineReport report = new PerformanceBaselineReport(
                new BenchmarkDatasetSpec("test", BenchmarkScale.SMALL,
                        1000L, 10, 0.01d, 8, 1L),
                Arrays.asList(first, second), 1000L);

        assertEquals(50L, first.getHeapDeltaBytes());
        assertEquals(80L, first.getExecutorMemoryDeltaBytes());
        assertEquals(120L, first.getDiskDeltaBytes());
        assertEquals(160L, first.getNetworkDeltaBytes());
        assertEquals(2, report.getStageMetrics().size());
        assertTrue(report.getTotalElapsedMillis() >= 0L);
    }

    @Test
    void shouldRecommendBoundedResourcesForDifferentCapacityBands() {
        ProductionResourceAdvisor advisor = new ProductionResourceAdvisor();

        ProductionResourceRecommendation small = advisor.recommend(
                500000L, 20, 512L * 1024L * 1024L);
        ProductionResourceRecommendation large = advisor.recommend(
                50000000L, 150, 512L * 1024L * 1024L * 1024L);
        ProductionResourceRecommendation extraLarge = advisor.recommend(
                200000000L, 300, 2048L * 1024L * 1024L * 1024L);

        assertEquals(CapacityBand.SMALL, small.getCapacityBand());
        assertEquals(CapacityBand.LARGE, large.getCapacityBand());
        assertEquals(CapacityBand.EXTRA_LARGE, extraLarge.getCapacityBand());
        assertTrue(large.getPartitionCount() > small.getPartitionCount());
        assertTrue(extraLarge.getPartitionCount() <= 2000);
        assertTrue(large.isCacheEnabled());
        assertTrue(large.getMaxRvdColumnPairs() <= 500);
        assertTrue(large.getIntermediateRetentionDays()
                < large.getDetectionRetentionDays());
    }

    @Test
    void shouldKeepProductionGateClosedUntilTargetClusterBaselinePasses() {
        ProductionReadinessChecker checker = new ProductionReadinessChecker();
        ProductionReadinessReport engineeringReady = checker.check(
                context(false));
        ProductionReadinessReport fullyReady = checker.check(context(true));

        assertFalse(engineeringReady.isReady());
        assertFalse(engineeringReady.getChecks().get("targetClusterBaseline"));
        assertTrue(fullyReady.isReady());
    }

    private static ProductionReadinessContext context(boolean clusterBaselineReady) {
        return new ProductionReadinessContext(true, true, true, true, true,
                true, true, true, clusterBaselineReady);
    }
}
