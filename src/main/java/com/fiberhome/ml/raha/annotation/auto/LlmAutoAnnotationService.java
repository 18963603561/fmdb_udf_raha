package com.fiberhome.ml.raha.annotation.auto;

import com.fiberhome.ml.raha.annotation.excel.AnnotationWorkbookAdapter;
import com.fiberhome.ml.raha.annotation.excel.AnnotationWorkbookData;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbJsonCodec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 编排工作簿读取、上下文分批、模型重试、结果合并、回写和审计报告。
 */
public final class LlmAutoAnnotationService {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            LlmAutoAnnotationService.class);
    /** 标注工作簿读取器。 */
    private final AnnotationWorkbookAdapter workbookAdapter;
    /** 自动标注工作簿写入器。 */
    private final AnnotationAutoLabelWorkbookWriter workbookWriter;
    /** 提示词构造器。 */
    private final LlmPromptBuilder promptBuilder;
    /** 模型响应校验器。 */
    private final LlmResponseValidator responseValidator;
    /** 批次合并器。 */
    private final AutoAnnotationMergeService mergeService;
    /** 当前服务使用的时钟。 */
    private final Clock clock;
    /** 测试或定制场景注入的模型客户端，为空时创建 HTTP 客户端。 */
    private final LlmClient suppliedClient;

    public LlmAutoAnnotationService(AnnotationWorkbookAdapter workbookAdapter,
                                    Clock clock) {
        this(workbookAdapter, clock, null);
    }

    public LlmAutoAnnotationService(AnnotationWorkbookAdapter workbookAdapter,
                                    Clock clock, LlmClient suppliedClient) {
        if (workbookAdapter == null || clock == null) {
            throw new IllegalArgumentException("自动标注服务依赖不能为空");
        }
        this.workbookAdapter = workbookAdapter;
        this.workbookWriter = new AnnotationAutoLabelWorkbookWriter();
        this.promptBuilder = new LlmPromptBuilder();
        this.responseValidator = new LlmResponseValidator();
        this.mergeService = new AutoAnnotationMergeService();
        this.clock = clock;
        this.suppliedClient = suppliedClient;
    }

    /**
     * 执行一次自动标注任务。
     *
     * @param request 输入输出请求
     * @param config 模型和批次配置
     * @return 自动标注结果
     */
    public AutoAnnotationResult autoLabel(AutoAnnotationRequest request,
                                          AutoAnnotationConfig config) {
        if (request == null || config == null) {
            throw new IllegalArgumentException("自动标注请求和配置不能为空");
        }
        if (!config.isEnabled()) {
            return AutoAnnotationResult.disabled();
        }
        long startedAt = clock.millis();
        AnnotationWorkbookData workbook;
        List<AutoAnnotationBatch> batches;
        try {
            config.validateEnabled();
            workbook = workbookAdapter.read(request.getInputWorkbook());
            batches = new AutoAnnotationBatchBuilder(promptBuilder).build(
                    workbook, request.getDatasetId(), request.getSampleBatchId(),
                    request.getSensitiveColumns(), config);
        } catch (RuntimeException exception) {
            LOGGER.error("自动标注初始化失败，datasetId={}，sampleBatchId={}，failPolicy={}",
                    request.getDatasetId(), request.getSampleBatchId(),
                    config.getFailPolicy(), exception);
            return handleFatal(request, config, 0,
                    Collections.<AutoAnnotationBatchResult>emptyList(),
                    Collections.<AutoAnnotationDecision>emptyList(),
                    message(exception), startedAt);
        }
        LOGGER.info("开始自动标注，datasetId={}，sampleBatchId={}，recordCount={}，batchCount={}，maxParallel={}，capacityProfile={}，contextTokens={}，maxCharsPerBatch={}，maxRowsPerBatch={}，maxColumnsPerBatch={}",
                request.getDatasetId(), request.getSampleBatchId(),
                workbook.getRows().size(), batches.size(),
                config.getMaxParallelBatches(),
                config.getModelCapacityProfile(),
                config.getContextWindowTokens(),
                config.getMaxCharsPerBatch(),
                config.getMaxRowsPerBatch(),
                config.getMaxColumnsPerBatch());
        LlmClient client = suppliedClient == null
                ? new OpenCompatibleLlmClient(config) : suppliedClient;
        List<AutoAnnotationBatchResult> batchResults;
        try {
            batchResults = executeBatches(request, workbook, batches, config,
                    client);
        } catch (RuntimeException exception) {
            LOGGER.error("自动标注批次编排失败，sampleBatchId={}",
                    request.getSampleBatchId(), exception);
            return handleFatal(request, config, workbook.getRows().size(),
                    Collections.<AutoAnnotationBatchResult>emptyList(),
                    Collections.<AutoAnnotationDecision>emptyList(),
                    message(exception), startedAt);
        }
        boolean hasBatchFailure = false;
        for (AutoAnnotationBatchResult result : batchResults) {
            hasBatchFailure |= !result.isSucceeded();
        }
        List<AutoAnnotationDecision> decisions = mergeService.merge(
                workbook, batchResults);
        if (hasBatchFailure
                && config.getFailPolicy() == AutoAnnotationFailPolicy.WARN_ONLY) {
            return failedWithReports(request, config, workbook.getRows().size(),
                    batchResults, Collections.<AutoAnnotationDecision>emptyList(),
                    "存在模型批次失败，已按 WARN_ONLY 保留原始模板", startedAt);
        }
        if (hasBatchFailure
                && config.getFailPolicy() == AutoAnnotationFailPolicy.FAIL) {
            AutoAnnotationResult failed = failedWithReports(request, config,
                    workbook.getRows().size(), batchResults, decisions,
                    "存在模型批次失败，已按 FAIL 终止采集", startedAt);
            throw new IllegalStateException(failed.getErrorMessage());
        }
        try {
            workbookWriter.write(request.getInputWorkbook(),
                    request.getOutputWorkbook(), decisions);
        } catch (RuntimeException exception) {
            LOGGER.error("自动标注工作簿回写失败，sampleBatchId={}",
                    request.getSampleBatchId(), exception);
            return handleFatal(request, config, workbook.getRows().size(),
                    batchResults, decisions, message(exception), startedAt);
        }
        int failedCount = Math.max(0,
                workbook.getRows().size() - decisions.size());
        AutoAnnotationStatus status = hasBatchFailure || failedCount > 0
                ? AutoAnnotationStatus.PARTIAL : AutoAnnotationStatus.SUCCEEDED;
        AutoAnnotationResult result = writeReports(request, config, status,
                workbook.getRows().size(), decisions.size(), failedCount,
                batchResults, decisions, request.getOutputWorkbook(), null,
                startedAt);
        LOGGER.info("自动标注完成，sampleBatchId={}，status={}，labeledCount={}，failedCount={}，batchCount={}",
                request.getSampleBatchId(), result.getStatus(),
                result.getLabeledCount(), result.getFailedCount(),
                result.getBatchCount());
        return result;
    }

    private List<AutoAnnotationBatchResult> executeBatches(
            AutoAnnotationRequest request, AnnotationWorkbookData workbook,
            List<AutoAnnotationBatch> batches, AutoAnnotationConfig config,
            LlmClient client) {
        if (batches.isEmpty()) {
            return Collections.emptyList();
        }
        ExecutorService executor = Executors.newFixedThreadPool(
                Math.min(config.getMaxParallelBatches(), batches.size()));
        try {
            List<Future<AutoAnnotationBatchResult>> futures =
                    new ArrayList<Future<AutoAnnotationBatchResult>>();
            for (final AutoAnnotationBatch batch : batches) {
                futures.add(executor.submit(new Callable<AutoAnnotationBatchResult>() {
                    @Override
                    public AutoAnnotationBatchResult call() {
                        return executeBatch(request, workbook, batch, config,
                                client);
                    }
                }));
            }
            List<AutoAnnotationBatchResult> results =
                    new ArrayList<AutoAnnotationBatchResult>();
            for (Future<AutoAnnotationBatchResult> future : futures) {
                try {
                    results.add(future.get());
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("等待自动标注批次时被中断", exception);
                } catch (ExecutionException exception) {
                    throw new IllegalStateException("执行自动标注批次失败",
                            exception.getCause());
                }
            }
            return Collections.unmodifiableList(results);
        } finally {
            executor.shutdownNow();
        }
    }

    private AutoAnnotationBatchResult executeBatch(
            AutoAnnotationRequest request, AnnotationWorkbookData workbook,
            AutoAnnotationBatch batch, AutoAnnotationConfig config,
            LlmClient client) {
        long startedAt = System.currentTimeMillis();
        RuntimeException last = null;
        int maximumAttempts = config.getMaxRetryCount() + 1;
        for (int attempt = 1; attempt <= maximumAttempts; attempt++) {
            try {
                LOGGER.debug("开始调用自动标注批次，batchId={}，attempt={}，rowCount={}，columnCount={}，estimatedChars={}",
                        batch.getBatchId(), attempt, batch.getRows().size(),
                        batch.getDetectableColumns().size(),
                        batch.getEstimatedChars());
                String userPrompt = promptBuilder.buildUserPrompt(
                        request.getDatasetId(), request.getSampleBatchId(), batch,
                        config.isMaskSensitiveColumns(),
                        request.getSensitiveColumns(), config.getMaxValueChars());
                String content = client.complete(promptBuilder.getSystemPrompt(),
                        userPrompt);
                List<AutoAnnotationDecision> decisions =
                        responseValidator.validate(content, batch);
                long elapsed = System.currentTimeMillis() - startedAt;
                LOGGER.info("自动标注批次完成，batchId={}，attempt={}，rowCount={}，elapsedMillis={}",
                        batch.getBatchId(), attempt, decisions.size(), elapsed);
                return new AutoAnnotationBatchResult(batch, true, attempt,
                        elapsed, decisions, null);
            } catch (RuntimeException exception) {
                last = exception;
                // 模型调用或结构校验失败均允许按配置重试，最终失败不泄露请求正文。
                LOGGER.warn("自动标注批次失败，batchId={}，attempt={}，maximumAttempts={}，error={}",
                        batch.getBatchId(), attempt, maximumAttempts,
                        message(exception), exception);
            }
        }
        return new AutoAnnotationBatchResult(batch, false, maximumAttempts,
                System.currentTimeMillis() - startedAt,
                Collections.<AutoAnnotationDecision>emptyList(), message(last));
    }

    private AutoAnnotationResult handleFatal(AutoAnnotationRequest request,
                                             AutoAnnotationConfig config,
                                             int recordCount,
                                             List<AutoAnnotationBatchResult> batches,
                                             List<AutoAnnotationDecision> decisions,
                                             String error, long startedAt) {
        AutoAnnotationResult result = failedWithReports(request, config,
                recordCount, batches, decisions, error, startedAt);
        if (config.getFailPolicy() == AutoAnnotationFailPolicy.FAIL) {
            throw new IllegalStateException(error);
        }
        return result;
    }

    private AutoAnnotationResult failedWithReports(
            AutoAnnotationRequest request, AutoAnnotationConfig config,
            int recordCount, List<AutoAnnotationBatchResult> batches,
            List<AutoAnnotationDecision> decisions, String error,
            long startedAt) {
        LOGGER.error("自动标注失败，sampleBatchId={}，failPolicy={}，error={}",
                request.getSampleBatchId(), config.getFailPolicy(), error);
        return writeReports(request, config, AutoAnnotationStatus.FAILED,
                recordCount, 0, recordCount, batches, decisions, null, error,
                startedAt);
    }

    private AutoAnnotationResult writeReports(
            AutoAnnotationRequest request, AutoAnnotationConfig config,
            AutoAnnotationStatus status, int recordCount, int labeledCount,
            int failedCount, List<AutoAnnotationBatchResult> batches,
            List<AutoAnnotationDecision> decisions, Path workbookPath,
            String error, long startedAt) {
        Path summaryPath = request.getReportDirectory().resolve("summary.json");
        Path decisionsPath = request.getReportDirectory().resolve(
                "decisions.jsonl");
        Path batchesPath = request.getReportDirectory().resolve("batches.jsonl");
        List<String> decisionLines = new ArrayList<String>();
        for (AutoAnnotationDecision decision : decisions) {
            decisionLines.add(FmdbJsonCodec.write(decision.toMap()));
        }
        List<String> batchLines = new ArrayList<String>();
        for (AutoAnnotationBatchResult batch : batches) {
            batchLines.add(FmdbJsonCodec.write(batch.toMap()));
        }
        writeLines(decisionsPath, decisionLines);
        writeLines(batchesPath, batchLines);
        Map<String, Object> summary = new java.util.LinkedHashMap<String, Object>();
        summary.put("status", status.name());
        summary.put("sampleBatchId", request.getSampleBatchId());
        summary.put("datasetId", request.getDatasetId());
        summary.put("rowCount", Integer.valueOf(recordCount));
        summary.put("labeledCount", Integer.valueOf(labeledCount));
        summary.put("failedCount", Integer.valueOf(failedCount));
        summary.put("batchCount", Integer.valueOf(batches.size()));
        summary.put("modelUrlHash", sha256(config.getModelUrl()));
        summary.put("model", config.getModel());
        summary.put("modelCapacityProfile",
                config.getModelCapacityProfile());
        summary.put("contextWindowTokens",
                Integer.valueOf(config.getContextWindowTokens()));
        summary.put("maxOutputTokens",
                Integer.valueOf(config.getMaxOutputTokens()));
        summary.put("maxCharsPerBatch",
                Integer.valueOf(config.getMaxCharsPerBatch()));
        summary.put("maxRowsPerBatch",
                Integer.valueOf(config.getMaxRowsPerBatch()));
        summary.put("maxColumnsPerBatch",
                Integer.valueOf(config.getMaxColumnsPerBatch()));
        summary.put("startedAt", Instant.ofEpochMilli(startedAt).toString());
        summary.put("finishedAt", Instant.ofEpochMilli(clock.millis()).toString());
        summary.put("promptVersion", LlmPromptBuilder.PROMPT_VERSION);
        summary.put("errorMessage", error);
        writeLines(summaryPath, Collections.singletonList(
                FmdbJsonCodec.write(summary)));
        return new AutoAnnotationResult(status, recordCount, labeledCount,
                failedCount, batches.size(), workbookPath, summaryPath,
                decisionsPath, batchesPath, error);
    }

    private static void writeLines(Path path, List<String> lines) {
        try {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            // JSONL 空结果也创建空文件，便于 ZIP 消费方按固定路径读取。
            Files.write(path, lines, StandardCharsets.UTF_8);
        } catch (IOException exception) {
            LOGGER.error("写入自动标注审计文件失败，fileName={}",
                    path.getFileName(), exception);
            throw new IllegalStateException("写入自动标注审计文件失败", exception);
        }
    }

    private static String sha256(String value) {
        if (value == null) {
            return null;
        }
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder();
            for (byte item : digest) {
                result.append(String.format(Locale.ROOT, "%02x", item & 0xff));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("运行环境缺少 SHA-256", exception);
        }
    }

    private static String message(Throwable exception) {
        if (exception == null) {
            return "未知错误";
        }
        String value = exception.getMessage();
        if (value == null || value.trim().isEmpty()) {
            value = exception.getClass().getSimpleName();
        }
        value = value.replaceAll("[\\r\\n\\t]+", " ").trim();
        return value.length() <= 500 ? value : value.substring(0, 500);
    }
}
