package com.fiberhome.ml.raha.api;

/**
 * Raha 三条同步业务用例的唯一 Java 门面。
 */
public interface RahaFacade {

    SampleResult sample(SampleRequest request);

    TrainResult train(TrainRequest request);

    DetectResult detect(DetectRequest request);
}
