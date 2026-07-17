package com.fiberhome.ml.raha.api;

import com.fiberhome.ml.raha.config.RahaConfig;
import com.fiberhome.ml.raha.detect.DetectionExplainer;
import com.fiberhome.ml.raha.detect.RahaDetectService;
import com.fiberhome.ml.raha.feature.FeatureVectorizer;
import com.fiberhome.ml.raha.fmdb.FmdbDatasetLoader;
import com.fiberhome.ml.raha.fmdb.FmdbDetectionStore;
import com.fiberhome.ml.raha.fmdb.FmdbLabelStore;
import com.fiberhome.ml.raha.fmdb.FmdbModelStore;
import com.fiberhome.ml.raha.fmdb.FmdbSampleStore;
import com.fiberhome.ml.raha.fmdb.FmdbTableGateway;
import com.fiberhome.ml.raha.model.LogisticRegressionColumnModelTrainer;
import com.fiberhome.ml.raha.model.ModelScoreExpression;
import com.fiberhome.ml.raha.profile.ColumnProfileService;
import com.fiberhome.ml.raha.sample.RahaSampleService;
import com.fiberhome.ml.raha.sample.TupleSampler;
import com.fiberhome.ml.raha.strategy.StrategyPlanner;
import com.fiberhome.ml.raha.train.IncrementalTrainingDatasetBuilder;
import com.fiberhome.ml.raha.train.RahaTrainService;
import com.fiberhome.ml.raha.train.TrainingDatasetBuilder;
import org.apache.spark.sql.SparkSession;

/**
 * 默认同步门面，只聚合采样、训练和检测三个用例服务。
 */
public final class DefaultRahaFacade implements RahaFacade {

    /** 采样用例。 */
    private final RahaSampleService sampleService;
    /** 训练用例。 */
    private final RahaTrainService trainService;
    /** 检测用例。 */
    private final RahaDetectService detectService;

    public DefaultRahaFacade(RahaSampleService sampleService,
                             RahaTrainService trainService,
                             RahaDetectService detectService) {
        this.sampleService = sampleService;
        this.trainService = trainService;
        this.detectService = detectService;
    }

    /**
     * 基于当前 Spark 会话创建一次调用使用的门面，不保存跨调用业务状态。
     *
     * @param sparkSession 当前驱动进程 Spark 会话
     * @return 默认门面
     */
    public static DefaultRahaFacade create(SparkSession sparkSession) {
        RahaConfig config = RahaConfig.defaults();
        FmdbTableGateway gateway = new FmdbTableGateway(sparkSession, config);
        FmdbDatasetLoader loader = new FmdbDatasetLoader(sparkSession);
        FmdbSampleStore sampleStore = new FmdbSampleStore(gateway);
        FmdbLabelStore labelStore = new FmdbLabelStore(gateway);
        FmdbModelStore modelStore = new FmdbModelStore(gateway);
        FmdbDetectionStore detectionStore = new FmdbDetectionStore(gateway);
        ColumnProfileService profiler = new ColumnProfileService();
        StrategyPlanner planner = new StrategyPlanner();
        RahaSampleService sample = new RahaSampleService(loader, profiler, planner,
                new TupleSampler(), sampleStore, config);
        RahaTrainService train = new RahaTrainService(sampleStore, labelStore, modelStore,
                loader, profiler, planner, new TrainingDatasetBuilder(),
                new IncrementalTrainingDatasetBuilder(),
                new LogisticRegressionColumnModelTrainer(sparkSession,
                        config.getModelConfig()), new FeatureVectorizer(), config);
        RahaDetectService detect = new RahaDetectService(loader, modelStore,
                detectionStore, new ModelScoreExpression(), new DetectionExplainer(), config);
        return new DefaultRahaFacade(sample, train, detect);
    }

    @Override
    public SampleResult sample(SampleRequest request) {
        return sampleService.sample(request);
    }

    @Override
    public TrainResult train(TrainRequest request) {
        return trainService.train(request);
    }

    @Override
    public DetectResult detect(DetectRequest request) {
        return detectService.detect(request);
    }
}
