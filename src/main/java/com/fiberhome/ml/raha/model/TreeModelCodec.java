package com.fiberhome.ml.raha.model;

import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.GBTClassificationModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.ml.tree.CategoricalSplit;
import org.apache.spark.ml.tree.ContinuousSplit;
import org.apache.spark.ml.tree.InternalNode;
import org.apache.spark.ml.tree.LeafNode;
import org.apache.spark.ml.tree.Node;
import org.apache.spark.ml.tree.Split;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;

/**
 * 编码和执行可移植的 Spark 树模型结构。
 *
 * <p>模型只保存分裂特征、分裂条件、叶子预测值和 GBT 树权重，避免把 Spark
 * 训练对象或执行器依赖写入 FMDB 模型参数。</p>
 */
public final class TreeModelCodec {

    /** 编码格式标识。 */
    private static final int MAGIC = 0x52485431;
    /** 决策树模型类型。 */
    private static final int DECISION_TREE = 1;
    /** 梯度提升树模型类型。 */
    private static final int GBT = 2;
    /** 叶子节点标识。 */
    private static final int LEAF = 0;
    /** 内部节点标识。 */
    private static final int INTERNAL = 1;
    /** 连续特征分裂标识。 */
    private static final int CONTINUOUS = 0;
    /** 离散特征分裂标识。 */
    private static final int CATEGORICAL = 1;

    private TreeModelCodec() {
    }

    /**
     * 将 Spark 决策树分类模型编码为 Base64 文本。
     */
    public static String encode(DecisionTreeClassificationModel model) {
        if (model == null) {
            throw new IllegalArgumentException("决策树模型不能为空");
        }
        return encodeModel(DECISION_TREE, model.numFeatures(),
                new Node[]{model.rootNode()}, new double[]{1.0d});
    }

    /**
     * 将 Spark 梯度提升树分类模型编码为 Base64 文本。
     */
    public static String encode(GBTClassificationModel model) {
        if (model == null || model.trees() == null || model.treeWeights() == null
                || model.trees().length != model.treeWeights().length) {
            throw new IllegalArgumentException("梯度提升树模型不能为空且树权重必须匹配");
        }
        DecisionTreeRegressionModel[] trees = model.trees();
        Node[] roots = new Node[trees.length];
        for (int index = 0; index < trees.length; index++) {
            roots[index] = trees[index].rootNode();
        }
        return encodeModel(GBT, model.numFeatures(), roots, model.treeWeights());
    }

    /**
     * 从编码文本创建可复用的模型执行器。
     */
    public static DecodedModel decode(String encoded) {
        if (encoded == null || encoded.trim().isEmpty()) {
            throw new IllegalArgumentException("树模型编码不能为空");
        }
        try {
            byte[] bytes = Base64.getDecoder().decode(encoded.getBytes(StandardCharsets.US_ASCII));
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes));
            int magic = input.readInt();
            if (magic != MAGIC) {
                throw new IllegalArgumentException("树模型编码格式不受支持");
            }
            int modelType = input.readInt();
            int dimension = input.readInt();
            int treeCount = input.readInt();
            if ((modelType != DECISION_TREE && modelType != GBT)
                    || dimension <= 0 || treeCount <= 0 || treeCount > 100000) {
                throw new IllegalArgumentException("树模型编码头非法");
            }
            double[] weights = new double[treeCount];
            for (int index = 0; index < treeCount; index++) {
                weights[index] = input.readDouble();
                if (Double.isNaN(weights[index]) || Double.isInfinite(weights[index])) {
                    throw new IllegalArgumentException("树模型权重非法");
                }
            }
            PortableNode[] roots = new PortableNode[treeCount];
            for (int index = 0; index < treeCount; index++) {
                roots[index] = readNode(input, dimension);
            }
            if (input.read() != -1) {
                throw new IllegalArgumentException("树模型编码包含多余内容");
            }
            return new DecodedModel(modelType, dimension, roots, weights);
        } catch (IOException | IllegalArgumentException exception) {
            throw new IllegalArgumentException("树模型编码解析失败", exception);
        }
    }

    private static String encodeModel(int modelType,
                                      int dimension,
                                      Node[] roots,
                                      double[] weights) {
        if (dimension <= 0 || roots == null || roots.length == 0
                || weights == null || roots.length != weights.length) {
            throw new IllegalArgumentException("树模型编码参数非法");
        }
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(bytes);
            output.writeInt(MAGIC);
            output.writeInt(modelType);
            output.writeInt(dimension);
            output.writeInt(roots.length);
            for (double weight : weights) {
                output.writeDouble(weight);
            }
            for (Node root : roots) {
                writeNode(output, root, dimension);
            }
            output.flush();
            return Base64.getEncoder().encodeToString(bytes.toByteArray());
        } catch (IOException exception) {
            throw new IllegalStateException("树模型编码失败", exception);
        }
    }

    private static void writeNode(DataOutputStream output,
                                  Node node,
                                  int dimension) throws IOException {
        if (node instanceof LeafNode) {
            double prediction = ((LeafNode) node).prediction();
            if (Double.isNaN(prediction) || Double.isInfinite(prediction)) {
                throw new IllegalArgumentException("树模型叶子预测值非法");
            }
            output.writeByte(LEAF);
            output.writeDouble(prediction);
            return;
        }
        if (!(node instanceof InternalNode)) {
            throw new IllegalArgumentException("树模型节点类型不受支持");
        }
        InternalNode internal = (InternalNode) node;
        Split split = internal.split();
        if (split.featureIndex() < 0 || split.featureIndex() >= dimension) {
            throw new IllegalArgumentException("树模型分裂特征编号非法");
        }
        output.writeByte(INTERNAL);
        output.writeInt(split.featureIndex());
        if (split instanceof ContinuousSplit) {
            output.writeByte(CONTINUOUS);
            output.writeDouble(((ContinuousSplit) split).threshold());
        } else if (split instanceof CategoricalSplit) {
            CategoricalSplit categorical = (CategoricalSplit) split;
            double[] categories = categorical.leftCategories().clone();
            Arrays.sort(categories);
            output.writeByte(CATEGORICAL);
            output.writeInt(categories.length);
            for (double category : categories) {
                output.writeDouble(category);
            }
        } else {
            throw new IllegalArgumentException("树模型分裂类型不受支持");
        }
        writeNode(output, internal.leftChild(), dimension);
        writeNode(output, internal.rightChild(), dimension);
    }

    private static PortableNode readNode(DataInputStream input,
                                         int dimension) throws IOException {
        int nodeType = input.readUnsignedByte();
        if (nodeType == LEAF) {
            double prediction = input.readDouble();
            if (Double.isNaN(prediction) || Double.isInfinite(prediction)) {
                throw new IllegalArgumentException("树模型叶子预测值非法");
            }
            return PortableNode.leaf(prediction);
        }
        if (nodeType != INTERNAL) {
            throw new IllegalArgumentException("树模型节点标识非法");
        }
        int featureIndex = input.readInt();
        int splitType = input.readUnsignedByte();
        if (featureIndex < 0 || featureIndex >= dimension) {
            throw new IllegalArgumentException("树模型分裂特征编号非法");
        }
        double threshold = 0.0d;
        double[] categories = null;
        if (splitType == CONTINUOUS) {
            threshold = input.readDouble();
        } else if (splitType == CATEGORICAL) {
            int count = input.readInt();
            if (count < 0 || count > dimension * 100000) {
                throw new IllegalArgumentException("树模型离散类别数量非法");
            }
            categories = new double[count];
            for (int index = 0; index < count; index++) {
                categories[index] = input.readDouble();
            }
            Arrays.sort(categories);
        } else {
            throw new IllegalArgumentException("树模型分裂类型非法");
        }
        return PortableNode.branch(featureIndex, splitType, threshold, categories,
                readNode(input, dimension), readNode(input, dimension));
    }

    /**
     * 已解析的不可变树模型执行器。
     */
    public static final class DecodedModel {

        /** 模型类型。 */
        private final int modelType;
        /** 特征维度。 */
        private final int dimension;
        /** 模型树根节点。 */
        private final PortableNode[] roots;
        /** GBT 树权重。 */
        private final double[] weights;

        private DecodedModel(int modelType,
                             int dimension,
                             PortableNode[] roots,
                             double[] weights) {
            this.modelType = modelType;
            this.dimension = dimension;
            this.roots = roots;
            this.weights = weights;
        }

        public int getDimension() { return dimension; }

        public double score(Map<Integer, Double> values) {
            if (values == null) {
                throw new IllegalArgumentException("树模型特征不能为空");
            }
            double[] vector = new double[dimension];
            for (Map.Entry<Integer, Double> entry : values.entrySet()) {
                if (entry.getKey() == null || entry.getKey() < 0
                        || entry.getKey() >= dimension || entry.getValue() == null
                        || Double.isNaN(entry.getValue())
                        || Double.isInfinite(entry.getValue())) {
                    throw new IllegalArgumentException("树模型特征编号或值非法");
                }
                vector[entry.getKey()] = entry.getValue();
            }
            return score(vector);
        }

        public double score(Vector vector) {
            if (vector == null || vector.size() != dimension) {
                throw new IllegalArgumentException("树模型向量维度不一致");
            }
            double[] values = new double[dimension];
            for (int index = 0; index < dimension; index++) {
                values[index] = vector.apply(index);
            }
            return score(values);
        }

        private double score(double[] values) {
            if (modelType == DECISION_TREE) {
                return bounded(roots[0].predict(values));
            }
            double margin = 0.0d;
            for (int index = 0; index < roots.length; index++) {
                margin += weights[index] * roots[index].predict(values);
            }
            if (margin >= 35.0d) {
                return 1.0d;
            }
            if (margin <= -35.0d) {
                return 0.0d;
            }
            return 1.0d / (1.0d + Math.exp(-2.0d * margin));
        }

        private static double bounded(double value) {
            return Math.max(0.0d, Math.min(1.0d, value));
        }
    }

    /** 可移植树节点。 */
    private static final class PortableNode {

        /** 叶子预测值。 */
        private final double prediction;
        /** 分裂特征编号。 */
        private final int featureIndex;
        /** 分裂类型。 */
        private final int splitType;
        /** 连续分裂阈值。 */
        private final double threshold;
        /** 离散分裂的左侧类别集合。 */
        private final double[] categories;
        /** 左子节点。 */
        private final PortableNode left;
        /** 右子节点。 */
        private final PortableNode right;

        private PortableNode(double prediction,
                             int featureIndex,
                             int splitType,
                             double threshold,
                             double[] categories,
                             PortableNode left,
                             PortableNode right) {
            this.prediction = prediction;
            this.featureIndex = featureIndex;
            this.splitType = splitType;
            this.threshold = threshold;
            this.categories = categories;
            this.left = left;
            this.right = right;
        }

        private static PortableNode leaf(double prediction) {
            return new PortableNode(prediction, -1, -1, 0.0d,
                    null, null, null);
        }

        private static PortableNode branch(int featureIndex,
                                           int splitType,
                                           double threshold,
                                           double[] categories,
                                           PortableNode left,
                                           PortableNode right) {
            return new PortableNode(0.0d, featureIndex, splitType, threshold,
                    categories, left, right);
        }

        private double predict(double[] values) {
            if (left == null) {
                return prediction;
            }
            boolean goLeft;
            if (splitType == CONTINUOUS) {
                goLeft = values[featureIndex] <= threshold;
            } else {
                goLeft = Arrays.binarySearch(categories, values[featureIndex]) >= 0;
            }
            return (goLeft ? left : right).predict(values);
        }
    }
}
