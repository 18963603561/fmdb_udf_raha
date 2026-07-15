package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.service.RahaTaskType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证文件任务消费者的原子认领、完整请求解析和成功状态落盘。
 */
class FileRahaUdfJobWorkerTest {

    /** JUnit 提供的隔离任务目录。 */
    @TempDir
    Path queueDirectory;

    @Test
    void shouldExecuteRequestOnlyOnceWithConcurrentWorkers() throws Exception {
        RahaUdfRequest request = new RahaUdfRequestParser().parse(
                RahaTaskType.TRAIN, requestText());
        Files.write(queueDirectory.resolve("job-1-train.request"),
                request.toEncodedRequest().getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE_NEW);
        AtomicInteger executions = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        FileRahaUdfJobWorker first = worker(executions, start);
        FileRahaUdfJobWorker second = worker(executions, start);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.submit(() -> {
            await(start);
            first.runOnce();
        });
        executor.submit(() -> {
            await(start);
            second.runOnce();
        });

        start.countDown();
        executor.shutdown();
        assertTrue(executor.awaitTermination(10L, TimeUnit.SECONDS));

        assertEquals(1, executions.get());
        assertTrue(Files.exists(queueDirectory.resolve("job-1-train.completed.request")));
        assertTrue(Files.exists(queueDirectory.resolve("job-1-train.succeeded")));
    }

    @Test
    void shouldRecoverExpiredRunningRequest() throws Exception {
        RahaUdfRequest request = new RahaUdfRequestParser().parse(
                RahaTaskType.TRAIN, requestText());
        Path running = queueDirectory.resolve("job-1-train.running");
        Path lease = queueDirectory.resolve("job-1-train.lease");
        Files.write(running, request.toEncodedRequest().getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE_NEW);
        Files.createFile(lease);
        Files.setLastModifiedTime(lease, FileTime.fromMillis(1000L));
        AtomicInteger executions = new AtomicInteger();
        FileRahaUdfJobWorker worker = new FileRahaUdfJobWorker(queueDirectory,
                ignored -> {
                    executions.incrementAndGet();
                    return "recovered";
                }, Clock.fixed(Instant.ofEpochMilli(10000L), ZoneOffset.UTC), 1000L);

        assertEquals(1, worker.runOnce());
        assertEquals(1, executions.get());
        assertTrue(Files.exists(queueDirectory.resolve(
                "job-1-train.completed.request")));
    }

    private FileRahaUdfJobWorker worker(AtomicInteger executions,
                                        CountDownLatch start) {
        return new FileRahaUdfJobWorker(queueDirectory, request -> {
            executions.incrementAndGet();
            return "completed";
        }, Clock.fixed(Instant.ofEpochMilli(2000L), ZoneOffset.UTC));
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("等待并发任务启动时被中断", exception);
        }
    }

    private static String requestText() {
        return "annotationReference=labels_table"
                + "&caller=tester"
                + "&datasetId=dataset"
                + "&idempotencyKey=job-1"
                + "&inputReference=dirty_table"
                + "&resultTable=result_table"
                + "&rowIdColumn=id"
                + "&snapshotId=snapshot-v1"
                + "&sourceType=TABLE";
    }
}
