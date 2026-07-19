package com.fiberhome.ml.raha.service.task;

import com.fiberhome.ml.raha.config.dto.RahaJobConfig;
import com.fiberhome.ml.raha.data.loader.DataFormat;
import com.fiberhome.ml.raha.data.loader.DataLoadRequest;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationConfig;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationMethod;
import com.fiberhome.ml.raha.model.training.LogisticRegressionTrainingConfig;
import java.util.Collections;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证统一任务请求的简化创建入口和默认值。
 */
class RahaTaskExecutionRequestTest {

    @Test
    void shouldCreateTrainingRequestWithAllDefaults() {
        RahaTaskExecutionRequest request = RahaTaskExecutionRequest.training(
                jobConfig(JobType.TRAINING), loadRequest(),
                Collections.emptyList());

        assertEquals(LabelPropagationMethod.HOMOGENEITY,
                request.getPropagationMethod());
        assertNotNull(request.getPropagationConfig());
        assertNotNull(request.getTrainingConfig());
        assertEquals("raha", request.getModelNamePrefix());
        assertTrue(request.getLabels().isEmpty());
    }

    @Test
    void shouldCreateTrainingRequestWithTheFiveArgumentOrder() {
        LogisticRegressionTrainingConfig trainingConfig =
                LogisticRegressionTrainingConfig.defaults();
        LabelPropagationConfig propagationConfig =
                LabelPropagationConfig.defaults();

        RahaTaskExecutionRequest request = RahaTaskExecutionRequest.training(
                jobConfig(JobType.TRAINING), loadRequest(), trainingConfig,
                propagationConfig, Collections.emptyList());

        assertEquals(trainingConfig, request.getTrainingConfig());
        assertEquals(propagationConfig, request.getPropagationConfig());
        assertEquals(LabelPropagationMethod.HOMOGENEITY,
                request.getPropagationMethod());
        assertEquals("raha", request.getModelNamePrefix());
    }

    @Test
    void shouldCreateSamplingRequestWithTheFirstRoundByDefault() {
        RahaTaskExecutionRequest request = RahaTaskExecutionRequest.sampling(
                jobConfig(JobType.SAMPLING), loadRequest());

        assertEquals(1, request.getSamplingRound());
        assertTrue(request.getLabels().isEmpty());
    }

    private static RahaJobConfig jobConfig(JobType jobType) {
        return RahaJobConfig.defaults(jobType, "dataset", "input.csv",
                RowIdentityConfig.contentHash());
    }

    private static DataLoadRequest loadRequest() {
        return new DataLoadRequest("dataset", "input.csv", "dataset",
                RowIdentityConfig.contentHash(), DataFormat.CSV,
                Collections.<String, String>emptyMap(),
                Collections.<String>emptySet(),
                Collections.<String>emptySet(),
                Collections.<String>emptySet(), null, null);
    }
}
