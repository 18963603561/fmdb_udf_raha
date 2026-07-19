package com.fiberhome.ml.raha.annotation.service;

import com.fiberhome.ml.raha.annotation.domain.AnnotationBatch;
import com.fiberhome.ml.raha.annotation.domain.AnnotationBatchStatus;
import com.fiberhome.ml.raha.annotation.domain.AnnotationRowError;
import com.fiberhome.ml.raha.label.CellLabel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 返回标注批次状态、有效直接标签、错误行和可选错误工作簿。
 */
public final class AnnotationImportResult {

    /** 导入终态。 */
    private final AnnotationBatchStatus status;
    /** 成功入库批次，拒绝或重复时为空。 */
    private final AnnotationBatch batch;
    /** 所有有效行展开后的直接标签。 */
    private final List<CellLabel> cellLabels;
    /** 所有无效 Excel 行。 */
    private final List<AnnotationRowError> errors;
    /** 有错误时生成的校验工作簿。 */
    private final Path validationWorkbook;

    public AnnotationImportResult(AnnotationBatchStatus status,
                                  AnnotationBatch batch,
                                  List<CellLabel> cellLabels,
                                  List<AnnotationRowError> errors,
                                  Path validationWorkbook) {
        if (status == null || cellLabels == null || errors == null) {
            throw new IllegalArgumentException("标注导入结果参数不能为空");
        }
        if ((status == AnnotationBatchStatus.IMPORTED
                || status == AnnotationBatchStatus.PARTIAL) && batch == null) {
            throw new IllegalArgumentException("成功标注结果必须包含物理批次");
        }
        this.status = status;
        this.batch = batch;
        this.cellLabels = Collections.unmodifiableList(
                new ArrayList<CellLabel>(cellLabels));
        this.errors = Collections.unmodifiableList(
                new ArrayList<AnnotationRowError>(errors));
        this.validationWorkbook = validationWorkbook;
    }

    public AnnotationBatchStatus getStatus() { return status; }
    public AnnotationBatch getBatch() { return batch; }
    public List<CellLabel> getCellLabels() { return cellLabels; }
    public List<AnnotationRowError> getErrors() { return errors; }
    public Path getValidationWorkbook() { return validationWorkbook; }
}
