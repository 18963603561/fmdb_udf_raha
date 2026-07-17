package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.config.RahaDefaultConfigProvider;
import com.fiberhome.ml.raha.util.FormDataCodec;
import com.fiberhome.ml.raha.util.HashUtils;
import com.fiberhome.ml.raha.util.ValueUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 将独立注册 UDF 收到的请求幂等写入共享目录，供文件工作器异步消费。
 */
public final class FileRahaUdfJobSubmitter
        implements RahaUdfJobSubmitter, Serializable {

    /** Java 序列化版本。 */
    private static final long serialVersionUID = 1L;
    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            FileRahaUdfJobSubmitter.class);
    /** 可安全用于共享目录文件名的任务标识格式。 */
    private static final Pattern FILE_TOKEN = Pattern.compile(
            "[A-Za-z0-9][A-Za-z0-9._-]{0,127}");
    /** 所有 Spark 节点均可读写的任务目录。 */
    private final String queueDirectory;

    public FileRahaUdfJobSubmitter(String queueDirectory) {
        String validated = ValueUtils.requireNotBlank(
                queueDirectory, "UDF 共享任务目录");
        this.queueDirectory = Paths.get(validated).toAbsolutePath()
                .normalize().toString();
    }

    /**
     * 根据统一 UDF 配置创建独立文件提交器，未配置目录时返回空。
     */
    static FileRahaUdfJobSubmitter fromConfiguration() {
        String configured = System.getProperty("raha.udf.queue-directory");
        if (configured == null || configured.trim().isEmpty()) {
            configured = RahaDefaultConfigProvider.factory().udfConfig()
                    .getQueueDirectory();
        }
        return configured == null || configured.trim().isEmpty()
                ? null : new FileRahaUdfJobSubmitter(configured);
    }

    @Override
    public RahaUdfSubmissionResult submit(RahaUdfRequest request) {
        if (request == null) {
            throw new RahaUdfException("INVALID_UDF_ARGUMENT", "UDF 请求不能为空");
        }
        long submittedAt = Math.max(1L, System.currentTimeMillis());
        String jobId = request.getIdempotencyKey();
        String configVersion = HashUtils.sha256Hex(
                request.toCanonicalConfiguration());
        Path requestPath = requestPath(request);
        Path receiptPath = requestPath.resolveSibling(
                requestPath.getFileName().toString() + ".receipt");
        Map<String, String> receipt = receipt(request, configVersion, submittedAt);
        LOGGER.info("开始写入独立 UDF 文件任务，jobId={}，taskType={}，requestPath={}",
                jobId, request.getTaskType(), requestPath);
        boolean receiptCreated = false;
        try {
            Files.createDirectories(requestPath.getParent());
            Files.write(receiptPath, FormDataCodec.encode(receipt).getBytes(
                            StandardCharsets.UTF_8), StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE);
            receiptCreated = true;
            Files.write(requestPath, request.toEncodedRequest().getBytes(
                            StandardCharsets.UTF_8), StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE);
            LOGGER.info("独立 UDF 文件任务写入完成，jobId={}，taskType={}，requestPath={}",
                    jobId, request.getTaskType(), requestPath);
            return RahaUdfSubmissionResult.accepted(jobId, request.getTaskType(),
                    requestPath.toUri().toString(), configVersion, submittedAt);
        } catch (FileAlreadyExistsException exception) {
            if (receiptCreated) {
                deleteReceiptQuietly(receiptPath, jobId);
                throw new IllegalStateException("UDF 请求文件已存在但提交回执缺失", exception);
            }
            validateExistingReceipt(receiptPath, request, configVersion);
            LOGGER.info("独立 UDF 文件任务重复提交，jobId={}，taskType={}",
                    jobId, request.getTaskType());
            return RahaUdfSubmissionResult.duplicate(jobId, request.getTaskType(),
                    requestPath.toUri().toString(), configVersion, submittedAt);
        } catch (IOException exception) {
            if (receiptCreated) {
                deleteReceiptQuietly(receiptPath, jobId);
            }
            LOGGER.error("写入独立 UDF 文件任务失败，jobId={}，requestPath={}",
                    jobId, requestPath, exception);
            throw new IllegalStateException("无法写入独立 UDF 文件任务", exception);
        }
    }

    private Path requestPath(RahaUdfRequest request) {
        String fileToken = request.getIdempotencyKey();
        if (!FILE_TOKEN.matcher(fileToken).matches()) {
            throw new RahaUdfException("INVALID_UDF_ARGUMENT",
                    "文件提交模式的 idempotencyKey 格式非法");
        }
        return Paths.get(queueDirectory, fileToken + "-"
                + request.getTaskType().name().toLowerCase(java.util.Locale.ROOT)
                + ".request");
    }

    private static Map<String, String> receipt(RahaUdfRequest request,
                                                String configVersion,
                                                long submittedAt) {
        Map<String, String> values = new LinkedHashMap<String, String>();
        values.put("jobId", request.getIdempotencyKey());
        values.put("taskType", request.getTaskType().name());
        values.put("datasetId", request.getDatasetId());
        values.put("configVersion", configVersion);
        values.put("submittedAt", String.valueOf(submittedAt));
        return values;
    }

    private static void validateExistingReceipt(Path receiptPath,
                                                RahaUdfRequest request,
                                                String configVersion) {
        try {
            Map<String, String> existing = FormDataCodec.decode(new String(
                    Files.readAllBytes(receiptPath), StandardCharsets.UTF_8));
            boolean same = request.getIdempotencyKey().equals(existing.get("jobId"))
                    && request.getTaskType().name().equals(existing.get("taskType"))
                    && request.getDatasetId().equals(existing.get("datasetId"))
                    && configVersion.equals(existing.get("configVersion"));
            if (!same) {
                throw new RahaUdfException("IDEMPOTENCY_CONFLICT",
                        "相同文件任务标识已用于其他任务配置");
            }
        } catch (RahaUdfException exception) {
            throw exception;
        } catch (IOException | IllegalArgumentException exception) {
            throw new IllegalStateException("无法校验已有 UDF 文件任务回执", exception);
        }
    }

    private static void deleteReceiptQuietly(Path receiptPath, String jobId) {
        try {
            Files.deleteIfExists(receiptPath);
        } catch (IOException exception) {
            LOGGER.warn("清理失败的 UDF 文件任务回执异常，jobId={}，receiptPath={}",
                    jobId, receiptPath, exception);
        }
    }
}
