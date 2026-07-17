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
 *
 * <p>一个任务在正常执行过程中依次经历以下文件状态：</p>
 * <ol>
 *     <li>{@code <jobId>-<taskType>.request}：等待消费者认领；</li>
 *     <li>{@code <jobId>-<taskType>.lease}：消费者持有的独占租约；</li>
 *     <li>{@code <jobId>-<taskType>.running}：已经认领、正在执行；</li>
 *     <li>{@code <jobId>-<taskType>.completed.request} 和 {@code .succeeded}：执行成功；</li>
 *     <li>{@code <jobId>-<taskType>.failed.request} 和 {@code .failed}：执行失败。</li>
 * </ol>
 *
 * <p>例如，{@code job-1-train.request} 被认领后会变为
 * {@code job-1-train.running}，成功后最终保留为
 * {@code job-1-train.completed.request}，并额外生成
 * {@code job-1-train.succeeded} 结果摘要文件。</p>
 *
 * <p>该类允许多个工作器扫描同一个共享目录。独占创建租约文件和移动请求文件共同保证
 * 同一时刻只有一个工作器能够取得任务；如果工作器异常退出，后续扫描会按照租约时间回收
 * 遗留的运行中文件。一次 {@link #runOnce()} 只处理调用开始时能够扫描到的任务，不负责常驻轮询。</p>
 */
public final class FileRahaUdfJobWorker {

    /** 记录任务扫描、认领、执行、恢复以及文件系统异常的日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            FileRahaUdfJobWorker.class);
    /** 保存请求文件、租约文件和终态结果文件的共享任务目录。 */
    private final Path queueDirectory;
    /** 将请求文件中的表单编码文本转换为强类型请求的解析器。 */
    private final RahaUdfRequestParser parser;
    /** 将校验通过的请求交给训练、检测或采样核心流程的任务分发器。 */
    private final RahaUdfTaskDispatcher dispatcher;
    /** 提供租约过期判断和任务完成时间，允许测试注入固定时间。 */
    private final Clock clock;
    /** 运行中任务租约的最大存活毫秒数，超过后允许其他工作器回收任务。 */
    private final long leaseTimeoutMillis;

    /**
     * 使用默认五分钟租约超时创建文件任务工作器。
     *
     * @param queueDirectory 共享任务目录；不存在时会在首次扫描时创建
     * @param dispatcher 已解析任务的核心分发器，返回值会写入成功摘要
     * @param clock 用于租约判断和完成时间记录的时钟
     * @throws IllegalArgumentException 任一依赖为空时抛出
     */
    public FileRahaUdfJobWorker(Path queueDirectory,
                                RahaUdfTaskDispatcher dispatcher,
                                Clock clock) {
        this(queueDirectory, dispatcher, clock, 300000L);
    }

    /**
     * 使用指定租约超时创建文件任务工作器。
     *
     * <p>例如，测试可传入固定时钟和 {@code 1000L}，使最后修改时间早于当前时间
     * 一秒以上的租约在下一次扫描时被回收。</p>
     *
     * @param queueDirectory 共享任务目录；必须非空
     * @param dispatcher 已解析任务的核心分发器；必须非空
     * @param clock 用于租约判断和完成时间记录的时钟；必须非空
     * @param leaseTimeoutMillis 租约超时毫秒数；必须大于零
     * @throws IllegalArgumentException 依赖为空或租约超时不为正数时抛出
     */
    public FileRahaUdfJobWorker(Path queueDirectory,
                                RahaUdfTaskDispatcher dispatcher,
                                Clock clock,
                                long leaseTimeoutMillis) {
        // 工作器无法在缺少目录、任务实现或时间来源时可靠执行，构造阶段直接拒绝无效依赖。
        if (queueDirectory == null || dispatcher == null || clock == null) {
            throw new IllegalArgumentException("文件任务消费者依赖不能为空");
        }
        // 非正数超时会使刚创建的租约立即失效，可能导致活跃任务被其他工作器错误回收。
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
        LOGGER.info("开始扫描 UDF 文件任务，queueDirectory={}", queueDirectory);
        // 扫描新请求前先恢复异常退出留下的任务，使其能在本轮重新参与排序和认领。
        recoverStaleTasks();
        List<Path> requests = requests();
        int completed = 0;
        for (Path requestPath : requests) {
            Path runningPath = sibling(requestPath, ".running");
            Path leasePath = sibling(requestPath, ".lease");
            // 共享目录可能同时被多个工作器扫描；认领失败表示任务已被其他工作器取得。
            if (!claim(requestPath, runningPath, leasePath)) {
                continue;
            }
            try {
                // 仅统计真正执行成功并已写出成功终态的任务，失败任务不计入返回值。
                if (execute(runningPath)) {
                    completed++;
                }
            } finally {
                // 无论执行成功、失败或出现未预期异常，都尽量释放本工作器创建的租约。
                releaseLease(leasePath);
            }
        }
        LOGGER.info("结束扫描 UDF 文件任务，queueDirectory={}，requestCount={}，completedCount={}",
                queueDirectory, requests.size(), completed);
        return completed;
    }

    /**
     * 回收租约缺失或已经超时的运行中任务。
     *
     * <p>恢复操作把 {@code .running} 改回 {@code .request}，使任务能够由本轮或后续工作器
     * 重新认领。租约仍在有效期内时不会移动文件，以免抢占仍然活跃的消费者。</p>
     *
     * @throws IllegalStateException 创建目录、读取租约时间或移动文件失败时抛出
     */
    private void recoverStaleTasks() {
        try {
            // 文件队列目录由工作器按需创建，调用方无需预先准备目录。
            Files.createDirectories(queueDirectory);
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(
                    queueDirectory, "*.running")) {
                for (Path runningPath : stream) {
                    Path leasePath = sibling(runningPath, ".lease");
                    boolean leaseMissing = !Files.exists(leasePath);
                    boolean leaseExpired = !leaseMissing
                            && Files.getLastModifiedTime(leasePath).toMillis()
                            + leaseTimeoutMillis <= clock.millis();
                    // 租约存在且未到期表示任务仍可能由其他消费者执行，当前工作器必须跳过。
                    if (!leaseMissing && !leaseExpired) {
                        continue;
                    }
                    Path requestPath = sibling(runningPath, ".request");
                    // 回收时覆盖同名等待文件，确保一个任务标识只留下一个可重新认领的请求版本。
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

    /**
     * 扫描并按文件名升序返回当前合法的等待请求。
     *
     * <p>只有以 {@code -train.request}、{@code -detect.request} 或
     * {@code -sample.request} 结尾的文件会进入结果。例如，成功归档文件
     * {@code job-1-train.completed.request} 虽然也以 {@code .request} 结尾，
     * 但不会被再次执行。</p>
     *
     * @return 文件名排序后的请求路径列表；没有任务时返回空列表
     * @throws IllegalStateException 创建或扫描任务目录失败时抛出
     */
    private List<Path> requests() {
        try {
            // 通过目录流读取文件，避免一次性加载目录中所有非请求文件的元数据。
            Files.createDirectories(queueDirectory);
            List<Path> requests = new ArrayList<Path>();
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(
                    queueDirectory, "*.request")) {
                for (Path path : stream) {
                    // 目录通配符会匹配已完成和已失败的归档文件，需要按业务命名规则二次过滤。
                    if (isPendingRequest(path.getFileName().toString())) {
                        requests.add(path);
                    }
                }
            }
            // 固定处理顺序便于复现问题，但并不承诺跨消费者的全局严格顺序。
            Collections.sort(requests, Comparator.comparing(
                    path -> path.getFileName().toString()));
            return requests;
        } catch (IOException exception) {
            LOGGER.error("扫描 UDF 文件任务失败，queueDirectory={}",
                    queueDirectory, exception);
            throw new IllegalStateException("无法扫描 UDF 文件任务", exception);
        }
    }

    /**
     * 通过独占租约和文件移动尝试认领单个请求。
     *
     * <p>例如两个工作器同时认领 {@code job-1-train.request} 时，只有一个能够创建
     * {@code job-1-train.lease}；另一个收到文件已存在异常后返回 {@code false}，
     * 不会调用任务分发器。</p>
     *
     * @param requestPath 等待认领的请求文件
     * @param runningPath 认领后使用的运行中文件
     * @param leasePath 用于互斥和超时恢复的租约文件
     * @return 认领成功返回 {@code true}；已被其他工作器认领返回 {@code false}
     * @throws IllegalStateException 遇到非并发竞争类文件系统异常时抛出
     */
    private boolean claim(Path requestPath, Path runningPath, Path leasePath) {
        try {
            // 独占创建租约文件后再移动请求，避免 Windows 原子移动的目标替换语义导致重复认领。
            Files.createFile(leasePath);
        } catch (FileAlreadyExistsException exception) {
            // 同名租约已经存在，说明另一个工作器先完成了独占认领。
            return false;
        } catch (IOException exception) {
            LOGGER.error("创建 UDF 文件任务租约失败，requestPath={}",
                    requestPath, exception);
            throw new IllegalStateException("无法创建 UDF 文件任务租约", exception);
        }
        try {
            // 移动成功才代表任务正式进入运行态，原请求文件不再会被后续扫描发现。
            Files.move(requestPath, runningPath);
            LOGGER.info("UDF 文件任务认领成功，requestPath={}，runningPath={}",
                    requestPath, runningPath);
            return true;
        } catch (FileAlreadyExistsException | NoSuchFileException exception) {
            // 请求消失或运行文件已存在属于消费者之间的正常竞争，清理本次租约后静默跳过。
            releaseLease(leasePath);
            return false;
        } catch (IOException exception) {
            // 非竞争类移动异常不能继续执行，同时应尽力撤销刚创建的租约。
            releaseLease(leasePath);
            LOGGER.error("UDF 文件任务认领失败，requestPath={}", requestPath, exception);
            throw new IllegalStateException("无法认领 UDF 文件任务", exception);
        }
    }

    /**
     * 尽力删除任务租约。
     *
     * <p>释放失败只记录日志而不覆盖原有执行结果；遗留租约可在超时后由恢复流程清理。</p>
     *
     * @param leasePath 待删除的租约文件
     */
    private void releaseLease(Path leasePath) {
        try {
            // 删除操作具备幂等性，租约已不存在时也视为释放成功。
            Files.deleteIfExists(leasePath);
        } catch (IOException exception) {
            // 租约清理失败不改变已落盘的成功或失败终态，只保留上下文供运维排查。
            LOGGER.error("释放 UDF 文件任务租约失败，leasePath={}", leasePath, exception);
        }
    }

    /**
     * 解析、分发一个已经认领的任务，并将请求归档到成功或失败终态。
     *
     * <p>成功示例：{@code job-1-train.running} 被归档为
     * {@code job-1-train.completed.request}，执行摘要写入
     * {@code job-1-train.succeeded}。失败示例：请求无法解析时归档为
     * {@code job-1-train.failed.request}，异常类型写入 {@code job-1-train.failed}，
     * 原始异常消息不会写入状态文件。</p>
     *
     * @param runningPath 已经由当前工作器认领的运行中文件
     * @return 请求已执行且成功状态落盘时返回 {@code true}，否则返回 {@code false}
     */
    private boolean execute(Path runningPath) {
        // 任务类型属于文件队列协议的一部分，在读取正文前由文件名确定解析规则。
        RahaTaskType taskType = taskType(runningPath.getFileName().toString());
        try {
            // 请求文件固定按无 BOM 的 UTF-8 文本读取，正文应为严格表单编码字符串。
            String encoded = new String(Files.readAllBytes(runningPath),
                    StandardCharsets.UTF_8);
            RahaUdfRequest request = parser.parse(taskType, encoded);
            LOGGER.info("开始执行 UDF 文件任务，jobId={}，taskType={}",
                    request.getIdempotencyKey(), taskType);
            // 分发器是核心业务边界，可能调用训练、检测、采样以及下游数据存储实现。
            String summary = dispatcher.dispatch(request);
            Path completedPath = sibling(runningPath, ".completed.request");
            // 先归档原请求，再写成功摘要，使请求正文和执行结果都可供审计。
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
                // 成功归档后若摘要写入失败，运行文件已不存在，此处只补写失败状态文件。
                if (Files.exists(runningPath)) {
                    Files.move(runningPath, failedPath,
                            StandardCopyOption.REPLACE_EXISTING);
                }
                // 状态文件仅记录异常类型，避免将请求正文、表名或下游异常详情意外泄露。
                Files.write(sibling(failedPath, ".failed"),
                        ("status=FAILED&errorType="
                                + exception.getClass().getSimpleName()).getBytes(
                                StandardCharsets.UTF_8), StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING);
            } catch (IOException persistenceException) {
                // 失败终态落盘异常不能掩盖最初的任务异常，因此分别记录完整堆栈。
                LOGGER.error("保存 UDF 文件任务失败状态异常，runningPath={}",
                        runningPath, persistenceException);
            }
            LOGGER.error("UDF 文件任务执行失败，runningPath={}，taskType={}",
                    runningPath, taskType, exception);
            return false;
        }
    }

    /**
     * 生成成功状态文件正文。
     *
     * <p>示例结果：
     * {@code jobId=job-1&taskType=TRAIN&status=SUCCEEDED&completedAt=2000&summary=completed}。
     * 分发器返回 {@code null} 时摘要写为空字符串；时间最小写为 {@code 1}，避免产生
     * 容易被调用方视为“未完成”的零时间戳。</p>
     *
     * @param request 已执行的完整任务请求
     * @param summary 分发器返回的不含原始数据的结果摘要，可以为空
     * @param status 要写入的任务状态
     * @return 使用键值对拼接的状态文本
     */
    private String completionText(RahaUdfRequest request,
                                  String summary,
                                  String status) {
        return "jobId=" + request.getIdempotencyKey()
                + "&taskType=" + request.getTaskType().name()
                + "&status=" + status
                + "&completedAt=" + Math.max(1L, clock.millis())
                + "&summary=" + (summary == null ? "" : summary);
    }

    /**
     * 从文件名中的任务标记识别任务类型，匹配时忽略大小写。
     *
     * @param fileName 请求或运行中文件名，例如 {@code job-1-train.running}
     * @return 与 {@code -train.}、{@code -detect.} 或 {@code -sample.} 对应的任务类型
     * @throws IllegalArgumentException 文件名不包含受支持的任务标记时抛出
     */
    private static RahaTaskType taskType(String fileName) {
        String normalized = fileName.toLowerCase(java.util.Locale.ROOT);
        // 文件名是任务类型的可信协议字段，正文中的参数不能覆盖该类型。
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

    /**
     * 判断文件名是否表示可执行的等待请求，匹配时忽略大小写。
     *
     * @param fileName 待检查的文件名
     * @return 是三类等待请求之一时返回 {@code true}，终态归档或其他文件返回 {@code false}
     */
    private static boolean isPendingRequest(String fileName) {
        String normalized = fileName.toLowerCase(java.util.Locale.ROOT);
        return normalized.endsWith("-train.request")
                || normalized.endsWith("-detect.request")
                || normalized.endsWith("-sample.request");
    }

    /**
     * 根据任务基础名生成同目录的另一个状态文件路径。
     *
     * <p>该方法会先剥离已知状态后缀，再追加目标后缀。例如：
     * {@code sibling(job-1-train.completed.request, ".succeeded")} 返回
     * {@code job-1-train.succeeded}，不会生成重复的状态后缀。</p>
     *
     * @param path 任意请求、运行、成功或失败状态路径
     * @param suffix 要追加到任务基础名后的目标后缀
     * @return 与输入路径同目录的新状态路径
     */
    private static Path sibling(Path path, String suffix) {
        String name = path.getFileName().toString();
        int requestIndex = name.indexOf(".request");
        int runningIndex = name.indexOf(".running");
        int completedIndex = name.indexOf(".completed.request");
        int failedIndex = name.indexOf(".failed.request");
        // 优先匹配较长的终态后缀，避免把其中的 .request 提前当作普通请求后缀截断。
        int end = completedIndex >= 0 ? completedIndex
                : failedIndex >= 0 ? failedIndex
                : requestIndex >= 0 ? requestIndex
                : runningIndex >= 0 ? runningIndex : name.length();
        return path.resolveSibling(name.substring(0, end) + suffix);
    }
}
