package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.service.RahaTaskType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * 从共享目录原子认领 UDF 请求并调度执行，供容器黑盒验收和文件队列部署使用。
 */
public final class FileRahaUdfJobWorker {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            FileRahaUdfJobWorker.class);
    /** 共享任务目录。 */
    private final Path queueDirectory;
    /** 任务请求解析器。 */
    private final RahaUdfRequestParser parser;
    /** 核心任务分发器。 */
    private final RahaUdfTaskDispatcher dispatcher;
    /** 提供可测试完成时间的时钟。 */
    private final Clock clock;
    /** 运行中任务租约超时时间。 */
    private final long leaseTimeoutMillis;

    public FileRahaUdfJobWorker(Path queueDirectory,
                                RahaUdfTaskDispatcher dispatcher,
                                Clock clock) {
        this(queueDirectory, dispatcher, clock, 300000L);
    }

    public FileRahaUdfJobWorker(Path queueDirectory,
                                RahaUdfTaskDispatcher dispatcher,
                                Clock clock,
                                long leaseTimeoutMillis) {
        if (queueDirectory == null || dispatcher == null || clock == null) {
            throw new IllegalArgumentException("文件任务消费者依赖不能为空");
        }
        if (leaseTimeoutMillis <= 0L) {
            throw new IllegalArgumentException("文件任务租约超时必须大于零");
        }
        this.queueDirectory = queueDirectory;
        this.parser = new RahaUdfRequestParser();
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.leaseTimeoutMillis = leaseTimeoutMillis;
    }

    /**
     * 扫描并执行当前可见任务，每个请求只允许一个消费者原子认领。
     *
     * @return 本轮成功处理的任务数量
     */
    public int runOnce() {
        recoverStaleTasks();
        List<Path> requests = requests();
        int completed = 0;
        for (Path requestPath : requests) {
            Path runningPath = sibling(requestPath, ".running");
            Path leasePath = sibling(requestPath, ".lease");
            if (!claim(requestPath, runningPath, leasePath)) {
                continue;
            }
            try {
                if (execute(runningPath)) {
                    completed++;
                }
            } finally {
                releaseLease(leasePath);
            }
        }
        return completed;
    }

    private void recoverStaleTasks() {
        try {
            Files.createDirectories(queueDirectory);
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(
                    queueDirectory, "*.running")) {
                for (Path runningPath : stream) {
                    Path leasePath = sibling(runningPath, ".lease");
                    boolean leaseMissing = !Files.exists(leasePath);
                    boolean leaseExpired = !leaseMissing
                            && Files.getLastModifiedTime(leasePath).toMillis()
                            + leaseTimeoutMillis <= clock.millis();
                    if (!leaseMissing && !leaseExpired) {
                        continue;
                    }
                    Path requestPath = sibling(runningPath, ".request");
                    // 只有租约缺失或超时才回收任务，仍活跃的消费者不会被抢占。
                    Files.move(runningPath, requestPath,
                            StandardCopyOption.REPLACE_EXISTING);
                    Files.deleteIfExists(leasePath);
                    LOGGER.warn("回收超时 UDF 文件任务，runningPath={}，requestPath={}",
                            runningPath, requestPath);
                }
            }
        } catch (IOException exception) {
            LOGGER.error("回收超时 UDF 文件任务失败，queueDirectory={}",
                    queueDirectory, exception);
            throw new IllegalStateException("无法回收超时 UDF 文件任务", exception);
        }
    }

    private List<Path> requests() {
        try {
            Files.createDirectories(queueDirectory);
            List<Path> requests = new ArrayList<Path>();
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(
                    queueDirectory, "*.request")) {
                for (Path path : stream) {
                    if (isPendingRequest(path.getFileName().toString())) {
                        requests.add(path);
                    }
                }
            }
            Collections.sort(requests, Comparator.comparing(
                    path -> path.getFileName().toString()));
            return requests;
        } catch (IOException exception) {
            LOGGER.error("扫描 UDF 文件任务失败，queueDirectory={}",
                    queueDirectory, exception);
            throw new IllegalStateException("无法扫描 UDF 文件任务", exception);
        }
    }

    private boolean claim(Path requestPath, Path runningPath, Path leasePath) {
        try {
            // 独占创建租约文件后再移动请求，避免 Windows 原子移动的目标替换语义导致重复认领。
            Files.createFile(leasePath);
        } catch (FileAlreadyExistsException exception) {
            return false;
        } catch (IOException exception) {
            LOGGER.error("创建 UDF 文件任务租约失败，requestPath={}",
                    requestPath, exception);
            throw new IllegalStateException("无法创建 UDF 文件任务租约", exception);
        }
        try {
            Files.move(requestPath, runningPath);
            LOGGER.info("UDF 文件任务认领成功，requestPath={}，runningPath={}",
                    requestPath, runningPath);
            return true;
        } catch (FileAlreadyExistsException | NoSuchFileException exception) {
            releaseLease(leasePath);
            return false;
        } catch (IOException exception) {
            releaseLease(leasePath);
            LOGGER.error("UDF 文件任务认领失败，requestPath={}", requestPath, exception);
            throw new IllegalStateException("无法认领 UDF 文件任务", exception);
        }
    }

    private void releaseLease(Path leasePath) {
        try {
            Files.deleteIfExists(leasePath);
        } catch (IOException exception) {
            LOGGER.error("释放 UDF 文件任务租约失败，leasePath={}", leasePath, exception);
        }
    }

    private boolean execute(Path runningPath) {
        RahaTaskType taskType = taskType(runningPath.getFileName().toString());
        try {
            String encoded = new String(Files.readAllBytes(runningPath),
                    StandardCharsets.UTF_8);
            RahaUdfRequest request = parser.parse(taskType, encoded);
            LOGGER.info("开始执行 UDF 文件任务，jobId={}，taskType={}",
                    request.getIdempotencyKey(), taskType);
            String summary = dispatcher.dispatch(request);
            Path completedPath = sibling(runningPath, ".completed.request");
            Files.move(runningPath, completedPath, StandardCopyOption.REPLACE_EXISTING);
            Files.write(sibling(completedPath, ".succeeded"),
                    completionText(request, summary, "SUCCEEDED").getBytes(
                            StandardCharsets.UTF_8), StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
            LOGGER.info("UDF 文件任务执行成功，jobId={}，taskType={}",
                    request.getIdempotencyKey(), taskType);
            return true;
        } catch (RuntimeException | IOException exception) {
            // 失败请求保留独立文件和脱敏错误类型，便于人工恢复且不会被重复扫描。
            Path failedPath = sibling(runningPath, ".failed.request");
            try {
                if (Files.exists(runningPath)) {
                    Files.move(runningPath, failedPath,
                            StandardCopyOption.REPLACE_EXISTING);
                }
                Files.write(sibling(failedPath, ".failed"),
                        ("status=FAILED&errorType="
                                + exception.getClass().getSimpleName()).getBytes(
                                StandardCharsets.UTF_8), StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING);
            } catch (IOException persistenceException) {
                LOGGER.error("保存 UDF 文件任务失败状态异常，runningPath={}",
                        runningPath, persistenceException);
            }
            LOGGER.error("UDF 文件任务执行失败，runningPath={}，taskType={}",
                    runningPath, taskType, exception);
            return false;
        }
    }

    private String completionText(RahaUdfRequest request,
                                  String summary,
                                  String status) {
        return "jobId=" + request.getIdempotencyKey()
                + "&taskType=" + request.getTaskType().name()
                + "&status=" + status
                + "&completedAt=" + Math.max(1L, clock.millis())
                + "&summary=" + (summary == null ? "" : summary);
    }

    private static RahaTaskType taskType(String fileName) {
        String normalized = fileName.toLowerCase(java.util.Locale.ROOT);
        if (normalized.contains("-train.")) {
            return RahaTaskType.TRAIN;
        }
        if (normalized.contains("-detect.")) {
            return RahaTaskType.DETECT;
        }
        if (normalized.contains("-sample.")) {
            return RahaTaskType.SAMPLE;
        }
        throw new IllegalArgumentException("无法从文件名识别 UDF 任务类型：" + fileName);
    }

    private static boolean isPendingRequest(String fileName) {
        String normalized = fileName.toLowerCase(java.util.Locale.ROOT);
        return normalized.endsWith("-train.request")
                || normalized.endsWith("-detect.request")
                || normalized.endsWith("-sample.request");
    }

    private static Path sibling(Path path, String suffix) {
        String name = path.getFileName().toString();
        int requestIndex = name.indexOf(".request");
        int runningIndex = name.indexOf(".running");
        int completedIndex = name.indexOf(".completed.request");
        int failedIndex = name.indexOf(".failed.request");
        int end = completedIndex >= 0 ? completedIndex
                : failedIndex >= 0 ? failedIndex
                : requestIndex >= 0 ? requestIndex
                : runningIndex >= 0 ? runningIndex : name.length();
        return path.resolveSibling(name.substring(0, end) + suffix);
    }
}
