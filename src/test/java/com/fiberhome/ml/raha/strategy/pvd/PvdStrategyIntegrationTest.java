package com.fiberhome.ml.raha.strategy.pvd;

import com.fiberhome.ml.raha.data.ColumnMetadata;
import com.fiberhome.ml.raha.data.RahaDataset;
import com.fiberhome.ml.raha.data.StrategyFamily;
import com.fiberhome.ml.raha.data.StrategyStatus;
import com.fiberhome.ml.raha.strategy.DetectionStrategy;
import com.fiberhome.ml.raha.strategy.StrategyCandidate;
import com.fiberhome.ml.raha.strategy.StrategyConfigurationKeys;
import com.fiberhome.ml.raha.strategy.StrategyExecutionContext;
import com.fiberhome.ml.raha.strategy.StrategyIdentityGenerator;
import com.fiberhome.ml.raha.strategy.StrategyPlan;
import com.fiberhome.ml.raha.strategy.StrategyTypes;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 使用 Spark 本地模式验证 PVD 多字符类型、长度、空值和格式边界。
 */
class PvdStrategyIntegrationTest {

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldDetectMinorCharacterSetAndLength() {
        List<StrategyCandidate> characterCandidates = execute(new CharacterSetStrategy(),
                plan("code", StrategyTypes.PVD_CHARACTER_SET,
                        StrategyConfigurationKeys.MINORITY_RATIO, "0.1"));
        Map<String, String> lengthConfiguration = base(StrategyTypes.PVD_LENGTH);
        lengthConfiguration.put(StrategyConfigurationKeys.MINORITY_RATIO, "0.1");
        lengthConfiguration.put(StrategyConfigurationKeys.IQR_MULTIPLIER, "1.5");
        List<StrategyCandidate> lengthCandidates = execute(new LengthAnomalyStrategy(),
                plan("serial", lengthConfiguration));

        assertEquals(Collections.singletonList("10"), rowIds(characterCandidates));
        assertEquals("PVD_MINOR_CHARACTER_SET", characterCandidates.get(0).getReasonCode());
        assertEquals(Collections.singletonList("10"), rowIds(lengthCandidates));
        assertEquals("PVD_LENGTH_OUTLIER", lengthCandidates.get(0).getReasonCode());
    }

    @Test
    void shouldDistinguishNullBlankAndPlaceholderValues() {
        List<StrategyCandidate> candidates = execute(new NullPlaceholderStrategy(),
                plan("status", StrategyTypes.PVD_NULL_PLACEHOLDER,
                        StrategyConfigurationKeys.PLACEHOLDERS, "N/A,NULL,-"));

        assertEquals(Arrays.asList("10", "11", "12"), rowIds(candidates));
        assertTrue(reasons(candidates).contains("PVD_PLACEHOLDER_VALUE"));
        assertTrue(reasons(candidates).contains("PVD_NULL_VALUE"));
        assertTrue(reasons(candidates).contains("PVD_BLANK_VALUE"));
    }

    @Test
    void shouldDetectMinorTypeAndConfigurableFormats() {
        Map<String, String> languageConfiguration = typeFormatConfiguration("AUTO");
        List<StrategyCandidate> languageCandidates = execute(new TypeFormatStrategy(),
                plan("language", languageConfiguration));
        List<StrategyCandidate> emailCandidates = execute(new TypeFormatStrategy(),
                plan("email", typeFormatConfiguration("AUTO")));
        List<StrategyCandidate> phoneCandidates = execute(new TypeFormatStrategy(),
                plan("phone", typeFormatConfiguration("PHONE")));

        assertEquals(Collections.singletonList("10"), rowIds(languageCandidates));
        assertEquals("PVD_TYPE_MISMATCH", languageCandidates.get(0).getReasonCode());
        assertTrue(rowIds(emailCandidates).contains("10"));
        assertTrue(reasons(emailCandidates).contains("PVD_FORMAT_MISMATCH"));
        assertTrue(rowIds(phoneCandidates).contains("10"));
        assertTrue(reasons(phoneCandidates).contains("PVD_FORMAT_MISMATCH"));
    }

    @Test
    void shouldSupportDateTimeAndIdentifierFormats() {
        List<StrategyCandidate> dateCandidates = execute(new TypeFormatStrategy(),
                plan("event_date", typeFormatConfiguration("DATE")));
        List<StrategyCandidate> timeCandidates = execute(new TypeFormatStrategy(),
                plan("event_time", typeFormatConfiguration("TIME")));
        List<StrategyCandidate> identifierCandidates = execute(new TypeFormatStrategy(),
                plan("business_id", typeFormatConfiguration("IDENTIFIER")));

        assertTrue(rowIds(dateCandidates).contains("10"));
        assertTrue(rowIds(timeCandidates).contains("10"));
        assertTrue(rowIds(identifierCandidates).contains("10"));
        assertTrue(reasons(dateCandidates).contains("PVD_FORMAT_MISMATCH"));
        assertTrue(reasons(timeCandidates).contains("PVD_FORMAT_MISMATCH"));
        assertTrue(reasons(identifierCandidates).contains("PVD_FORMAT_MISMATCH"));
    }

    private static Map<String, String> typeFormatConfiguration(String formatType) {
        Map<String, String> configuration = base(StrategyTypes.PVD_TYPE_FORMAT);
        configuration.put(StrategyConfigurationKeys.MINORITY_RATIO, "0.1");
        configuration.put(StrategyConfigurationKeys.FORMAT_TYPE, formatType);
        configuration.put(StrategyConfigurationKeys.FORMAT_MIN_RATIO, "0.8");
        return configuration;
    }

    private static List<StrategyCandidate> execute(DetectionStrategy strategy, StrategyPlan plan) {
        return strategy.detect(new StrategyExecutionContext("job", "stage", dataset(), plan));
    }

    private static StrategyPlan plan(String column,
                                     String strategyType,
                                     String key,
                                     String value) {
        Map<String, String> configuration = base(strategyType);
        configuration.put(key, value);
        return plan(column, configuration);
    }

    private static StrategyPlan plan(String column, Map<String, String> configuration) {
        List<String> columns = Collections.singletonList(column);
        return new StrategyPlan(StrategyIdentityGenerator.strategyId(
                StrategyFamily.PVD, columns, configuration), StrategyFamily.PVD,
                columns, configuration, 1, StrategyStatus.PLANNED);
    }

    private static Map<String, String> base(String strategyType) {
        Map<String, String> configuration = new LinkedHashMap<String, String>();
        configuration.put(StrategyConfigurationKeys.STRATEGY_TYPE, strategyType);
        return configuration;
    }

    private static RahaDataset dataset() {
        List<Row> rows = new ArrayList<Row>();
        for (int index = 1; index <= 12; index++) {
            String code = index == 10 ? "A1#" : "ABC";
            String serial = index == 10 ? "TOO-LONG" : String.format("A%02d", index);
            String status = index == 10 ? "N/A" : index == 11 ? null : index == 12 ? "" : "OK";
            String email = index == 10 ? "bad-address" : "user" + index + "@example.com";
            String language = index == 10 ? "English" : "中文";
            String phone = index == 10 ? "bad-phone" : "13800000001";
            String eventDate = index == 10 ? "14/99" : "2026-07-14";
            String eventTime = index == 10 ? "99:99" : "12:30";
            String businessId = index == 10 ? "bad" : "AB-" + String.format("%03d", index);
            rows.add(RowFactory.create(String.valueOf(index), code, serial,
                    status, email, language, phone, eventDate, eventTime, businessId));
        }
        StructType schema = new StructType()
                .add("id", DataTypes.StringType, false)
                .add("code", DataTypes.StringType, true)
                .add("serial", DataTypes.StringType, true)
                .add("status", DataTypes.StringType, true)
                .add("email", DataTypes.StringType, true)
                .add("language", DataTypes.StringType, true)
                .add("phone", DataTypes.StringType, true)
                .add("event_date", DataTypes.StringType, true)
                .add("event_time", DataTypes.StringType, true)
                .add("business_id", DataTypes.StringType, true);
        Dataset<Row> dataFrame = SparkTestSession.get().createDataFrame(rows, schema);
        List<ColumnMetadata> columns = new ArrayList<ColumnMetadata>();
        for (int index = 0; index < schema.size(); index++) {
            String name = schema.fields()[index].name();
            columns.add(new ColumnMetadata(name, index, "string", index > 0,
                    index > 0, false));
        }
        return new RahaDataset("dataset", "snapshot", "table", "id",
                columns, dataFrame, "schema-hash", Collections.emptyMap());
    }

    private static List<String> rowIds(List<StrategyCandidate> candidates) {
        List<String> ids = new ArrayList<String>();
        for (StrategyCandidate candidate : candidates) {
            ids.add(candidate.getRowId());
        }
        Collections.sort(ids);
        return ids;
    }

    private static List<String> reasons(List<StrategyCandidate> candidates) {
        List<String> reasons = new ArrayList<String>();
        for (StrategyCandidate candidate : candidates) {
            reasons.add(candidate.getReasonCode());
        }
        return reasons;
    }
}
