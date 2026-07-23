package com.fiberhome.ml.raha.annotation.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * 标注上传文件定位规则测试。
 */
class AnnotationUploadFileLocatorTest {

    @Test
    void shouldGenerateNameThatMatchesTrainingLookup() {
        String sampleBatchId = "sample_dw.person_info@20260721090041.862";

        String fileName = AnnotationUploadFileLocator.annotationExcelName(
                sampleBatchId, "20260721171443");

        assertEquals("raha-annotation_sample_dw.person_info_20260721090041.862_20260721171443.xls",
                fileName);
        assertTrue(AnnotationUploadFileLocator.matches(fileName, sampleBatchId));
    }

    @Test
    void shouldGenerateCollectZipNameWithSameToken() {
        String sampleBatchId = "sample_dw.person_info@20260721090041.862";

        String fileName = AnnotationUploadFileLocator.collectZipName(
                sampleBatchId, "20260721171443");

        assertEquals("raha-collect_sample_dw.person_info_20260721090041.862_20260721171443.zip",
                fileName);
    }

    @Test
    void shouldRejectLegacyRawSampleBatchName() {
        String sampleBatchId = "sample_dw.person_info@20260721090041.862";
        String legacyName = "raha-annotation_"
                + sampleBatchId + "_20260721171443.xls";

        assertFalse(AnnotationUploadFileLocator.matches(legacyName,
                sampleBatchId));
    }

    @Test
    void shouldRejectValidationFile() {
        String sampleBatchId = "sample_dw.person_info@20260721090041.862";
        String fileName = "validation_raha-annotation_sample_dw.person_info_"
                + "20260721090041.862_20260721171443.xls";

        assertFalse(AnnotationUploadFileLocator.matches(fileName,
                sampleBatchId));
    }

    @Test
    void shouldAcceptAutoAndReviewedAnnotationFiles() {
        String sampleBatchId = "sample_dw.person_info@20260721090041.862";
        String prefix = "raha-annotation_sample_dw.person_info_"
                + "20260721090041.862_20260721171443";

        assertTrue(AnnotationUploadFileLocator.matches(prefix + "_auto.xls",
                sampleBatchId));
        assertTrue(AnnotationUploadFileLocator.matches(prefix + "_reviewed.xls",
                sampleBatchId));
    }
}
