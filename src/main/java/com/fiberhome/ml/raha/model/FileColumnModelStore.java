package com.fiberhome.ml.raha.model;

import com.fiberhome.ml.raha.data.type.ClassifierType;
import com.fiberhome.ml.raha.model.domain.ColumnModelArtifact;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 使用 UTF-8 属性文件原子保存列级模型参数，不保存训练原始数据。
 */
public final class FileColumnModelStore implements ColumnModelStore {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(FileColumnModelStore.class);
    /** 模型文件根目录。 */
    private final Path rootDirectory;

    public FileColumnModelStore(Path rootDirectory) {
        if (rootDirectory == null) {
            throw new IllegalArgumentException("模型文件根目录不能为空");
        }
        this.rootDirectory = rootDirectory.toAbsolutePath().normalize();
    }

    @Override
    public String save(ColumnModelArtifact artifact) {
        if (artifact == null || !artifact.getModelVersion().matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("模型参数和版本必须有效");
        }
        Path target = rootDirectory.resolve(artifact.getModelVersion() + ".properties").normalize();
        ensureInsideRoot(target);
        Path temporary = rootDirectory.resolve(artifact.getModelVersion() + ".tmp").normalize();
        LOGGER.info("开始保存列级模型文件，modelVersion={}，columnName={}",
                artifact.getModelVersion(), artifact.getColumnName());
        try {
            Files.createDirectories(rootDirectory);
            write(temporary, artifact);
            try {
                Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException exception) {
                Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
            }
            LOGGER.info("列级模型文件保存完成，modelVersion={}，columnName={}",
                    artifact.getModelVersion(), artifact.getColumnName());
            return target.toString();
        } catch (IOException exception) {
            // 文件写入失败必须保留模型版本和异常堆栈，且不记录任何训练值。
            LOGGER.error("列级模型文件保存失败，modelVersion={}，columnName={}",
                    artifact.getModelVersion(), artifact.getColumnName(), exception);
            throw new IllegalStateException("列级模型文件保存失败", exception);
        }
    }

    @Override
    public ColumnModelArtifact load(String modelPath) {
        if (modelPath == null || modelPath.trim().isEmpty()) {
            throw new IllegalArgumentException("模型文件路径不能为空");
        }
        Path path = java.nio.file.Paths.get(modelPath).toAbsolutePath().normalize();
        ensureInsideRoot(path);
        LOGGER.info("开始加载列级模型文件，fileName={}", path.getFileName());
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            Properties properties = new Properties();
            properties.load(reader);
            ColumnModelArtifact artifact = fromProperties(properties);
            LOGGER.info("列级模型文件加载完成，modelVersion={}，columnName={}",
                    artifact.getModelVersion(), artifact.getColumnName());
            return artifact;
        } catch (IOException | RuntimeException exception) {
            // 文件损坏或缺失时拒绝加载，避免生产预测使用不完整参数。
            LOGGER.error("列级模型文件加载失败，fileName={}", path.getFileName(), exception);
            throw new IllegalStateException("列级模型文件加载失败", exception);
        }
    }

    private static void write(Path path, ColumnModelArtifact artifact) throws IOException {
        Map<String, String> values = new LinkedHashMap<String, String>();
        values.put("modelName", artifact.getModelName());
        values.put("modelVersion", artifact.getModelVersion());
        values.put("columnName", artifact.getColumnName());
        values.put("classifierType", artifact.getClassifierType().name());
        values.put("featureDictionaryVersion", artifact.getFeatureDictionaryVersion());
        values.put("featureDimension", String.valueOf(artifact.getFeatureDimension()));
        values.put("threshold", String.valueOf(artifact.getThreshold()));
        values.put("intercept", String.valueOf(artifact.getIntercept()));
        values.put("trainingMode", artifact.getTrainingMode());
        for (Map.Entry<Integer, Double> entry : artifact.getCoefficients().entrySet()) {
            values.put("coefficient." + entry.getKey(), String.valueOf(entry.getValue()));
        }
        List<String> keys = new ArrayList<String>(values.keySet());
        Collections.sort(keys);
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            for (String key : keys) {
                writer.write(key);
                writer.write('=');
                writer.write(values.get(key));
                writer.newLine();
            }
        }
    }

    private static ColumnModelArtifact fromProperties(Properties properties) {
        int dimension = Integer.parseInt(required(properties, "featureDimension"));
        Map<Integer, Double> coefficients = new LinkedHashMap<Integer, Double>();
        for (String name : properties.stringPropertyNames()) {
            if (name.startsWith("coefficient.")) {
                int index = Integer.parseInt(name.substring("coefficient.".length()));
                coefficients.put(index, Double.parseDouble(properties.getProperty(name)));
            }
        }
        return new ColumnModelArtifact(required(properties, "modelName"),
                required(properties, "modelVersion"), required(properties, "columnName"),
                ClassifierType.valueOf(required(properties, "classifierType")),
                required(properties, "featureDictionaryVersion"), dimension,
                Double.parseDouble(required(properties, "threshold")),
                Double.parseDouble(required(properties, "intercept")), coefficients,
                required(properties, "trainingMode"));
    }

    private void ensureInsideRoot(Path path) {
        if (!path.startsWith(rootDirectory)) {
            throw new IllegalArgumentException("模型文件路径超出配置根目录");
        }
    }

    private static String required(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("模型文件缺少字段：" + key);
        }
        return value;
    }
}
