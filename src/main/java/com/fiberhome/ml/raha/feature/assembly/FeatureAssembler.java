package com.fiberhome.ml.raha.feature.assembly;

import com.fiberhome.ml.raha.config.dto.FeatureConfig;
import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.data.domain.ColumnMetadata;
import com.fiberhome.ml.raha.data.domain.ColumnProfile;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.type.FeatureType;
import com.fiberhome.ml.raha.data.type.StrategyFamily;
import com.fiberhome.ml.raha.error.RahaErrorCode;
import com.fiberhome.ml.raha.error.RahaException;
import com.fiberhome.ml.raha.feature.domain.FeatureDefinition;
import com.fiberhome.ml.raha.feature.domain.FeatureDictionary;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import com.fiberhome.ml.raha.feature.FeatureDictionaryVersioner;
import com.fiberhome.ml.raha.strategy.api.StrategyConfigurationKeys;
import com.fiberhome.ml.raha.strategy.domain.StrategyHit;
import com.fiberhome.ml.raha.strategy.execution.SparkStrategySupport;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
import java.text.Normalizer;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sha2;
import static org.apache.spark.sql.functions.when;

/**
 * 将策略命中和单元格上下文转换成按列冻结的稀疏特征。
 */
public final class FeatureAssembler {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(FeatureAssembler.class);
    /** 特征字典版本生成器。 */
    private final FeatureDictionaryVersioner versioner;
    /** 提供可测试字典创建时间的时钟。 */
    private final Clock clock;
    public FeatureAssembler(FeatureDictionaryVersioner versioner, Clock clock) {
        if (versioner == null || clock == null) {
            throw new IllegalArgumentException("特征组装器依赖不能为空");
        }
        this.versioner = versioner;
        this.clock = clock;
    }

    /**
     * 为全部可检测字段生成特征字典和单元格稀疏向量。
     *
     * @param dataset 已画像只读数据集
     * @param plans 当前快照策略计划
     * @param hits 当前任务成功策略命中
     * @param config 特征配置
     * @return 特征组装结果
     */
    public FeatureAssemblyResult assemble(RahaDataset dataset,
                                          List<StrategyPlan> plans,
                                          List<StrategyHit> hits,
                                          FeatureConfig config) {
        if (dataset == null || dataset.getDataFrame() == null
                || plans == null || hits == null || config == null) {
            throw new IllegalArgumentException("特征组装参数不能为空");
        }
        Map<String, List<StrategyHit>> hitsByCell = indexHits(hits);
        Map<String, FeatureDictionary> dictionaries = new LinkedHashMap<String, FeatureDictionary>();
        List<SparseFeatureRow> sparseRows = new ArrayList<SparseFeatureRow>();
        long cellCount = 0L;
        long candidateFeatureCount = 0L;
        long retainedFeatureCount = 0L;
        long removedConstantFeatureCount = 0L;
        Map<String, List<Row>> rowsByColumn = buildCellRows(dataset);
        LOGGER.info("开始生成单元格特征，datasetId={}，snapshotId={}，planCount={}，hitCount={}",
                dataset.getDatasetId(), dataset.getSnapshotId(), plans.size(), hits.size());
        for (ColumnMetadata column : dataset.getColumns()) {
            if (!column.isDetectable()) {
                continue;
            }
            LinkedHashMap<String, FeatureSpec> specs = featureSpecs(column.getName(), plans, config);
            List<MutableCellFeatures> cells = buildCells(
                    dataset, column, specs, hitsByCell, config,
                    rowsByColumn.get(column.getName()));
            cellCount += cells.size();
            candidateFeatureCount += specs.size();
            List<FeatureSpec> retained = retainFeatures(specs, cells, config);
            int constantRemoved = specs.size() - countVariableFeatures(specs, cells, config);
            removedConstantFeatureCount += Math.max(0, constantRemoved);
            Map<Integer, FeatureDefinition> definitions = definitions(retained);
            List<FeatureDefinition> definitionList = new ArrayList<FeatureDefinition>(definitions.values());
            String dictionaryVersion = versioner.versionOf(column.getName(), definitionList, config);
            FeatureDictionary dictionary = new FeatureDictionary(dictionaryVersion,
                    column.getName(), definitions, clock.millis());
            dictionaries.put(column.getName(), dictionary);
            retainedFeatureCount += retained.size();
            sparseRows.addAll(toSparseRows(cells, retained, dictionaryVersion));
            LOGGER.debug("字段特征生成完成，columnName={}，cellCount={}，featureCount={}",
                    column.getName(), cells.size(), retained.size());
        }
        FeatureAssemblyMetrics metrics = new FeatureAssemblyMetrics(cellCount,
                candidateFeatureCount, retainedFeatureCount, removedConstantFeatureCount);
        LOGGER.info("单元格特征生成完成，cellCount={}，dictionaryCount={}，featureCount={}，removedConstantCount={}",
                metrics.getCellCount(), dictionaries.size(), metrics.getRetainedFeatureCount(),
                metrics.getRemovedConstantFeatureCount());
        return new FeatureAssemblyResult(dictionaries, sparseRows, metrics);
    }

    private static LinkedHashMap<String, FeatureSpec> featureSpecs(String columnName,
                                                                   List<StrategyPlan> plans,
                                                                   FeatureConfig config) {
        LinkedHashMap<String, FeatureSpec> specs = new LinkedHashMap<String, FeatureSpec>();
        EnumSet<StrategyFamily> families = EnumSet.noneOf(StrategyFamily.class);
        for (StrategyPlan plan : plans) {
            if (!plan.getTargetColumns().contains(columnName)) {
                continue;
            }
            String strategyType = plan.getConfiguration().get(
                    StrategyConfigurationKeys.STRATEGY_TYPE).toLowerCase(Locale.ROOT);
            String name = "strategy." + plan.getStrategyFamily().name().toLowerCase(Locale.ROOT)
                    + "." + strategyType + "."
                    + plan.getConfigurationHash().substring(0, 12) + ".hit";
            specs.put(name, new FeatureSpec(name, FeatureType.BINARY, plan.getStrategyId()));
            families.add(plan.getStrategyFamily());
        }
        for (StrategyFamily family : families) {
            String name = "summary.strategy." + family.name().toLowerCase(Locale.ROOT) + ".hit_count";
            specs.put(name, new FeatureSpec(name, FeatureType.NUMERIC, "strategy_summary"));
        }
        specs.put("summary.strategy.max_score", new FeatureSpec(
                "summary.strategy.max_score", FeatureType.NUMERIC, "strategy_summary"));
        if (config.isContextFeaturesEnabled()) {
            addContextSpecs(specs);
        }
        return specs;
    }

    private static void addContextSpecs(Map<String, FeatureSpec> specs) {
        add(specs, "context.value.length", FeatureType.NUMERIC);
        add(specs, "context.value.is_null", FeatureType.BINARY);
        add(specs, "context.value.is_blank", FeatureType.BINARY);
        add(specs, "context.value.has_digit", FeatureType.BINARY);
        add(specs, "context.value.has_letter", FeatureType.BINARY);
        add(specs, "context.value.has_chinese", FeatureType.BINARY);
        add(specs, "context.value.has_symbol", FeatureType.BINARY);
        add(specs, "context.value.type.numeric", FeatureType.BINARY);
        add(specs, "context.value.type.letter", FeatureType.BINARY);
        add(specs, "context.value.type.chinese", FeatureType.BINARY);
        add(specs, "context.value.type.alphanumeric", FeatureType.BINARY);
        add(specs, "context.value.type.mixed", FeatureType.BINARY);
        add(specs, "context.column.frequency", FeatureType.NUMERIC);
        add(specs, "context.column.frequency_ratio", FeatureType.NUMERIC);
        add(specs, "context.column.frequency_bucket.rare", FeatureType.BINARY);
        add(specs, "context.neighbor.rvd.conflict_count", FeatureType.NUMERIC);
    }

    private static void add(Map<String, FeatureSpec> specs, String name, FeatureType type) {
        specs.put(name, new FeatureSpec(name, type, "value_context"));
    }

    private List<MutableCellFeatures> buildCells(RahaDataset dataset,
                                                 ColumnMetadata column,
                                                 Map<String, FeatureSpec> specs,
                                                 Map<String, List<StrategyHit>> hitsByCell,
                                                 FeatureConfig config,
                                                 List<Row> preparedRows) {
        List<Row> rows = preparedRows == null
                ? Collections.<Row>emptyList() : preparedRows;
        ColumnProfile profile = dataset.getProfiles().get(column.getName());
        long totalCount = profile == null ? rows.size() : profile.getTotalCount();
        long rareThreshold = Math.max(1L, (long) Math.floor(
                totalCount * config.getRareValueRatio()));
        List<MutableCellFeatures> cells = new ArrayList<MutableCellFeatures>(rows.size());
        for (Row row : rows) {
            String rowId = row.getAs("row_id");
            String valueHash = row.getAs("value_hash");
            String originalText = row.getAs("text_value");
            String normalized = normalize(originalText, config);
            long frequency = ((Number) row.getAs("value_frequency")).longValue();
            CellCoordinate coordinate = new CellCoordinate(dataset.getDatasetId(),
                    dataset.getSnapshotId(), rowId, column.getName());
            MutableCellFeatures cell = new MutableCellFeatures(
                    coordinate, valueHash, null);
            List<StrategyHit> cellHits = hitsByCell.containsKey(coordinate.toCellId())
                    ? hitsByCell.get(coordinate.toCellId()) : Collections.<StrategyHit>emptyList();
            addStrategyValues(cell, cellHits, specs, valueHash);
            if (config.isContextFeaturesEnabled()) {
                addContextValues(cell, normalized, originalText == null,
                        frequency, totalCount, rareThreshold);
            }
            cells.add(cell);
        }
        return cells;
    }

    private static Map<String, List<Row>> buildCellRows(RahaDataset dataset) {
        List<String> detectableColumns = new ArrayList<String>();
        for (ColumnMetadata column : dataset.getColumns()) {
            if (column.isDetectable()) {
                detectableColumns.add(column.getName());
            }
        }
        if (detectableColumns.isEmpty()) {
            return Collections.emptyMap();
        }
        StringBuilder stack = new StringBuilder("stack(")
                .append(detectableColumns.size());
        for (String columnName : detectableColumns) {
            stack.append(", '").append(columnName.replace("'", "''"))
                    .append("', cast(").append(quoted(columnName))
                    .append(" as string)");
        }
        stack.append(") as (column_name, text_value)");
        Dataset<Row> values = dataset.getDataFrame().selectExpr(
                "cast(" + quoted(dataset.getRowIdColumn()) + " as string) as row_id",
                stack.toString())
                .withColumn("value_hash", sha2(when(col("text_value").isNull(),
                        lit("<null>")).otherwise(col("text_value")), 256));
        Dataset<Row> frequencies = values.groupBy("column_name", "value_hash").agg(
                count(lit(1)).alias("value_frequency"));
        Dataset<Row> valueRows = values.alias("v");
        Dataset<Row> frequencyRows = frequencies.alias("f");
        List<Row> rows = valueRows.join(frequencyRows,
                        col("v.column_name").equalTo(col("f.column_name"))
                                .and(col("v.value_hash").equalTo(col("f.value_hash"))))
                .select(col("v.column_name").alias("column_name"),
                        col("v.row_id").alias("row_id"),
                        col("v.text_value").alias("text_value"),
                        col("v.value_hash").alias("value_hash"),
                        col("f.value_frequency").alias("value_frequency"))
                .collectAsList();
        Map<String, List<Row>> rowsByColumn = new LinkedHashMap<String, List<Row>>();
        for (String columnName : detectableColumns) {
            rowsByColumn.put(columnName, new ArrayList<Row>());
        }
        for (Row row : rows) {
            rowsByColumn.get((String) row.getAs("column_name")).add(row);
        }
        return rowsByColumn;
    }

    private static String quoted(String columnName) {
        if (columnName == null || columnName.trim().isEmpty()) {
            throw new IllegalArgumentException("字段名称不能为空");
        }
        return "`" + columnName.replace("`", "``") + "`";
    }

    private static void addStrategyValues(MutableCellFeatures cell,
                                          List<StrategyHit> hits,
                                          Map<String, FeatureSpec> specs,
                                          String valueHash) {
        Map<StrategyFamily, Set<String>> familyStrategies =
                new EnumMap<StrategyFamily, Set<String>>(StrategyFamily.class);
        double maxScore = 0.0d;
        int rvdConflictCount = 0;
        for (StrategyHit hit : hits) {
            if (!hit.getValueHash().equals(valueHash)) {
                throw new RahaException(RahaErrorCode.FEATURE_SNAPSHOT_CONFLICT,
                        "策略命中值哈希与特征输入快照不一致", false);
            }
            String prefix = "strategy." + hit.getStrategyFamily().name().toLowerCase(Locale.ROOT) + ".";
            for (String featureName : specs.keySet()) {
                if (featureName.startsWith(prefix)
                        && specs.get(featureName).source.equals(hit.getStrategyId())) {
                    cell.values.put(featureName, 1.0d);
                }
            }
            if (!familyStrategies.containsKey(hit.getStrategyFamily())) {
                familyStrategies.put(hit.getStrategyFamily(), new LinkedHashSet<String>());
            }
            familyStrategies.get(hit.getStrategyFamily()).add(hit.getStrategyId());
            maxScore = Math.max(maxScore,
                    hit.getStrategyScore() == null ? 1.0d : hit.getStrategyScore());
            if (hit.getStrategyFamily() == StrategyFamily.RVD) {
                rvdConflictCount++;
            }
        }
        for (Map.Entry<StrategyFamily, Set<String>> entry : familyStrategies.entrySet()) {
            cell.values.put("summary.strategy."
                    + entry.getKey().name().toLowerCase(Locale.ROOT) + ".hit_count",
                    (double) entry.getValue().size());
        }
        cell.values.put("summary.strategy.max_score", maxScore);
        cell.values.put("context.neighbor.rvd.conflict_count", (double) rvdConflictCount);
        cell.summary.put("strategyHitCount", String.valueOf(hits.size()));
        cell.summary.put("strategyMaxScore", String.valueOf(maxScore));
        cell.summary.put("rvdConflictCount", String.valueOf(rvdConflictCount));
    }

    private static void addContextValues(MutableCellFeatures cell,
                                         String normalized,
                                         boolean nullValue,
                                         long frequency,
                                         long totalCount,
                                         long rareThreshold) {
        boolean blank = !nullValue && (normalized == null || normalized.isEmpty());
        String type = valueType(normalized, nullValue, blank);
        cell.values.put("context.value.length",
                normalized == null ? 0.0d : (double) normalized.length());
        cell.values.put("context.value.is_null", nullValue ? 1.0d : 0.0d);
        cell.values.put("context.value.is_blank", blank ? 1.0d : 0.0d);
        cell.values.put("context.value.has_digit", hasDigit(normalized) ? 1.0d : 0.0d);
        cell.values.put("context.value.has_letter", hasLetter(normalized) ? 1.0d : 0.0d);
        cell.values.put("context.value.has_chinese", hasChinese(normalized) ? 1.0d : 0.0d);
        cell.values.put("context.value.has_symbol", hasSymbol(normalized) ? 1.0d : 0.0d);
        cell.values.put("context.value.type.numeric", "NUMERIC".equals(type) ? 1.0d : 0.0d);
        cell.values.put("context.value.type.letter", "LETTER".equals(type) ? 1.0d : 0.0d);
        cell.values.put("context.value.type.chinese", "CHINESE".equals(type) ? 1.0d : 0.0d);
        cell.values.put("context.value.type.alphanumeric", "ALPHANUMERIC".equals(type) ? 1.0d : 0.0d);
        cell.values.put("context.value.type.mixed", "MIXED".equals(type) ? 1.0d : 0.0d);
        cell.values.put("context.column.frequency", (double) frequency);
        cell.values.put("context.column.frequency_ratio",
                totalCount == 0L ? 0.0d : (double) frequency / totalCount);
        cell.values.put("context.column.frequency_bucket.rare",
                frequency <= rareThreshold ? 1.0d : 0.0d);
        cell.summary.put("valueLength", String.valueOf(normalized == null ? 0 : normalized.length()));
        cell.summary.put("valueFrequency", String.valueOf(frequency));
        cell.summary.put("valueType", type);
    }

    private static List<FeatureSpec> retainFeatures(LinkedHashMap<String, FeatureSpec> specs,
                                                    List<MutableCellFeatures> cells,
                                                    FeatureConfig config) {
        List<FeatureSpec> retained = new ArrayList<FeatureSpec>();
        for (FeatureSpec spec : specs.values()) {
            if (!config.isRemoveConstantFeatures() || isVariable(spec.name, cells)) {
                retained.add(spec);
                if (retained.size() >= config.getMaxFeatureCount()) {
                    break;
                }
            }
        }
        return retained;
    }

    private static int countVariableFeatures(Map<String, FeatureSpec> specs,
                                             List<MutableCellFeatures> cells,
                                             FeatureConfig config) {
        if (!config.isRemoveConstantFeatures()) {
            return specs.size();
        }
        int variable = 0;
        for (FeatureSpec spec : specs.values()) {
            if (isVariable(spec.name, cells)) {
                variable++;
            }
        }
        return variable;
    }

    private static boolean isVariable(String featureName, List<MutableCellFeatures> cells) {
        if (cells.isEmpty()) {
            return false;
        }
        double first = cells.get(0).values.containsKey(featureName)
                ? cells.get(0).values.get(featureName) : 0.0d;
        for (int index = 1; index < cells.size(); index++) {
            double value = cells.get(index).values.containsKey(featureName)
                    ? cells.get(index).values.get(featureName) : 0.0d;
            if (Double.compare(first, value) != 0) {
                return true;
            }
        }
        return false;
    }

    private static Map<Integer, FeatureDefinition> definitions(List<FeatureSpec> specs) {
        Map<Integer, FeatureDefinition> definitions = new LinkedHashMap<Integer, FeatureDefinition>();
        for (int index = 0; index < specs.size(); index++) {
            FeatureSpec spec = specs.get(index);
            definitions.put(index, new FeatureDefinition(index, spec.name,
                    spec.type, spec.source, 0.0d));
        }
        return definitions;
    }

    private static List<SparseFeatureRow> toSparseRows(List<MutableCellFeatures> cells,
                                                       List<FeatureSpec> specs,
                                                       String dictionaryVersion) {
        List<SparseFeatureRow> rows = new ArrayList<SparseFeatureRow>(cells.size());
        for (MutableCellFeatures cell : cells) {
            Map<Integer, Double> values = new LinkedHashMap<Integer, Double>();
            for (int index = 0; index < specs.size(); index++) {
                Double value = cell.values.get(specs.get(index).name);
                if (value != null && Double.compare(value, 0.0d) != 0) {
                    values.put(index, value);
                }
            }
            rows.add(new SparseFeatureRow(cell.coordinate.toCellId(),
                    cell.coordinate.getColumnName(), cell.coordinate, cell.valueHash,
                    cell.maskedValue, dictionaryVersion, values, cell.summary));
        }
        return rows;
    }

    private static Map<String, List<StrategyHit>> indexHits(List<StrategyHit> hits) {
        Map<String, List<StrategyHit>> index = new HashMap<String, List<StrategyHit>>();
        for (StrategyHit hit : hits) {
            String cellId = hit.getCoordinate().toCellId();
            if (!index.containsKey(cellId)) {
                index.put(cellId, new ArrayList<StrategyHit>());
            }
            index.get(cellId).add(hit);
        }
        return index;
    }

    private static String normalize(String value, FeatureConfig config) {
        if (value == null) {
            return null;
        }
        String normalized = config.isNormalizeWidth()
                ? Normalizer.normalize(value, Normalizer.Form.NFKC) : value;
        if (config.isTrimValue()) {
            normalized = normalized.trim();
        }
        if (config.isLowerCaseValue()) {
            normalized = normalized.toLowerCase(Locale.ROOT);
        }
        return normalized;
    }

    private static String valueType(String value, boolean nullValue, boolean blank) {
        if (nullValue) {
            return "NULL";
        }
        if (blank) {
            return "BLANK";
        }
        if (value.matches(SparkStrategySupport.NUMERIC_PATTERN)) {
            return "NUMERIC";
        }
        if (value.matches("[A-Za-z]+")) {
            return "LETTER";
        }
        if (hasChinese(value) && value.codePoints().allMatch(
                codePoint -> codePoint >= 0x4E00 && codePoint <= 0x9FFF)) {
            return "CHINESE";
        }
        if (value.codePoints().allMatch(Character::isLetterOrDigit)) {
            return "ALPHANUMERIC";
        }
        return "MIXED";
    }

    private static boolean hasDigit(String value) {
        return value != null && value.codePoints().anyMatch(Character::isDigit);
    }

    private static boolean hasLetter(String value) {
        return value != null && value.codePoints().anyMatch(Character::isLetter);
    }

    private static boolean hasChinese(String value) {
        return value != null && value.codePoints().anyMatch(
                codePoint -> codePoint >= 0x4E00 && codePoint <= 0x9FFF);
    }

    private static boolean hasSymbol(String value) {
        return value != null && value.codePoints().anyMatch(
                codePoint -> !Character.isLetterOrDigit(codePoint)
                        && !Character.isWhitespace(codePoint));
    }

    private static final class FeatureSpec {
        /** 稳定特征名称。 */
        private final String name;
        /** 特征类型。 */
        private final FeatureType type;
        /** 特征来源。 */
        private final String source;

        private FeatureSpec(String name, FeatureType type, String source) {
            this.name = name;
            this.type = type;
            this.source = source;
        }
    }

    private static final class MutableCellFeatures {
        /** 单元格坐标。 */
        private final CellCoordinate coordinate;
        /** 原始值哈希。 */
        private final String valueHash;
        /** 可选脱敏值。 */
        private final String maskedValue;
        /** 按特征名称索引的非冻结数值。 */
        private final Map<String, Double> values = new LinkedHashMap<String, Double>();
        /** 不包含原始值的解释摘要。 */
        private final Map<String, String> summary = new LinkedHashMap<String, String>();

        private MutableCellFeatures(CellCoordinate coordinate,
                                    String valueHash,
                                    String maskedValue) {
            this.coordinate = coordinate;
            this.valueHash = valueHash;
            this.maskedValue = maskedValue;
        }
    }
}
