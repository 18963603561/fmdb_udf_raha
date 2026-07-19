package com.fiberhome.ml.raha.annotation.service;

import com.fiberhome.ml.raha.annotation.excel.AnnotationWorkbookAdapter;
import com.fiberhome.ml.raha.repository.port.SampleRecordRepository;
import com.fiberhome.ml.raha.sampling.domain.SampleAnnotationRow;
import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 从不可变 c1 批次导出受保护的 Excel 标注模板。
 */
public final class AnnotationTemplateService {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            AnnotationTemplateService.class);
    /** c1 采样记录仓储。 */
    private final SampleRecordRepository sampleRepository;
    /** Excel 工作簿适配器。 */
    private final AnnotationWorkbookAdapter workbookAdapter;
    /** 提供可测试导出时间的时钟。 */
    private final Clock clock;

    public AnnotationTemplateService(SampleRecordRepository sampleRepository,
                                     AnnotationWorkbookAdapter workbookAdapter,
                                     Clock clock) {
        if (sampleRepository == null || workbookAdapter == null || clock == null) {
            throw new IllegalArgumentException("标注模板服务依赖不能为空");
        }
        this.sampleRepository = sampleRepository;
        this.workbookAdapter = workbookAdapter;
        this.clock = clock;
    }

    /**
     * 按数据集、分区和采样批次导出模板。
     *
     * @param request 模板导出请求
     * @return 已生成的规范化文件路径
     */
    public Path exportTemplate(AnnotationTemplateRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("标注模板请求不能为空");
        }
        LOGGER.info("开始读取 c1 并导出标注模板，datasetId={}，sampleBatchId={}",
                request.getDatasetId(), request.getSampleBatchId());
        List<SampleAnnotationRow> rows = sampleRepository.findForAnnotation(
                request.getDatasetId(), request.getSamplePartitionMonth(),
                request.getSampleBatchId());
        if (rows.isEmpty()) {
            throw new IllegalStateException("指定 c1 采样批次不存在或没有记录");
        }
        return workbookAdapter.exportTemplate(request.getOutputPath(),
                request.getDatasetId(), request.getSamplePartitionMonth(),
                request.getSampleBatchId(), rows,
                Math.max(1L, clock.millis()));
    }
}
