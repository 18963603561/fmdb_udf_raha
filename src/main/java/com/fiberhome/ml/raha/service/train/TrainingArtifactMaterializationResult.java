package com.fiberhome.ml.raha.service.train;

/** 返回训练派生产物实际物化和写入数量。 */
public final class TrainingArtifactMaterializationResult {

    /** 列级产物写入数量。 */
    private final long columnArtifactWrittenCount;
    /** 训练单元格写入数量。 */
    private final long trainingCellWrittenCount;
    /** 最终训练样本写入数量。 */
    private final long trainingExampleWrittenCount;
    /** 训练单元格内存物化数量。 */
    private final int materializedCellCount;
    /** 实际入模样本数量。 */
    private final int materializedExampleCount;
    /** 冻结训练样本所在月分区。 */
    private final String partitionMonth;
    /** 冻结训练样本的模型集合版本。 */
    private final String modelSetVersion;

    public TrainingArtifactMaterializationResult(long columnArtifactWrittenCount,
                                                long trainingCellWrittenCount,
                                                long trainingExampleWrittenCount,
                                                int materializedCellCount,
                                                int materializedExampleCount) {
        this(columnArtifactWrittenCount, trainingCellWrittenCount,
                trainingExampleWrittenCount, materializedCellCount,
                materializedExampleCount, null, null);
    }

    public TrainingArtifactMaterializationResult(long columnArtifactWrittenCount,
                                                long trainingCellWrittenCount,
                                                long trainingExampleWrittenCount,
                                                int materializedCellCount,
                                                int materializedExampleCount,
                                                String partitionMonth,
                                                String modelSetVersion) {
        if (columnArtifactWrittenCount < 0L || trainingCellWrittenCount < 0L
                || trainingExampleWrittenCount < 0L || materializedCellCount < 0
                || materializedExampleCount < 0
                || materializedExampleCount > materializedCellCount) {
            throw new IllegalArgumentException("训练物化结果数量非法");
        }
        this.columnArtifactWrittenCount = columnArtifactWrittenCount;
        this.trainingCellWrittenCount = trainingCellWrittenCount;
        this.trainingExampleWrittenCount = trainingExampleWrittenCount;
        this.materializedCellCount = materializedCellCount;
        this.materializedExampleCount = materializedExampleCount;
        this.partitionMonth = partitionMonth;
        this.modelSetVersion = modelSetVersion;
    }

    public long getColumnArtifactWrittenCount() { return columnArtifactWrittenCount; }
    public long getTrainingCellWrittenCount() { return trainingCellWrittenCount; }
    public long getTrainingExampleWrittenCount() { return trainingExampleWrittenCount; }
    public int getMaterializedCellCount() { return materializedCellCount; }
    public int getMaterializedExampleCount() { return materializedExampleCount; }
    public String getPartitionMonth() { return partitionMonth; }
    public String getModelSetVersion() { return modelSetVersion; }
}
