package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.api.DetectRequest;
import com.fiberhome.ml.raha.api.SampleRequest;
import com.fiberhome.ml.raha.api.TrainRequest;
import com.fiberhome.ml.raha.support.FormCodec;
import com.fiberhome.ml.raha.support.RahaException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 三个 UDF 请求解析测试。
 */
class RahaRequestParserTest {

    /** 固定请求解析器。 */
    private final RahaRequestParser parser = new RahaRequestParser();

    @Test
    void shouldParseSampleTrainAndDetectRequests() {
        SampleRequest sample = parser.parseSample("inputReference="
                + FormCodec.encode("csv:/tmp/toy.csv")
                + "&rowKeyColumns=ID&targetColumns=Lord,Kingdom&labelingBudget=20");
        assertEquals(Arrays.asList("Lord", "Kingdom"), sample.getTargetColumns());
        assertEquals(20, sample.getLabelingBudget());

        TrainRequest train = parser.parseTrain(
                "sampleBatchIds=s1,s2&targetColumns=Kingdom&baseModelSetVersion=m1");
        assertEquals(Arrays.asList("s1", "s2"), train.getSampleBatchIds());
        assertEquals("m1", train.getBaseModelSetVersion());

        DetectRequest detect = parser.parseDetect(
                "inputReference=dw.toy&modelSetVersion=m2&errorsOnly=false");
        assertEquals("m2", detect.getModelSetVersion());
        assertTrue(!detect.isErrorsOnly());
    }

    @Test
    void shouldRejectUnknownAndDuplicateParameters() {
        assertThrows(RahaException.class, () -> parser.parseSample(
                "inputReference=dw.toy&unknown=1"));
        assertThrows(RahaException.class, () -> parser.parseSample(
                "inputReference=dw.a&inputReference=dw.b"));
    }
}
