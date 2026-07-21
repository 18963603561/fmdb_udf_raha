package com.fiberhome.ml.raha.data.loader;

import com.fiberhome.ml.raha.data.loader.identity.RowIdentityColumns;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityResult;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityService;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证联合键、内容哈希、分区变化和确定性逻辑去重。
 */
class RowIdentityServiceIntegrationTest {

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldGenerateUnambiguousJointKeyAndChooseStableRepresentative() {
        SparkSession spark = SparkTestSession.get();
        StructType schema = new StructType()
                .add("key_a", DataTypes.StringType, false)
                .add("key_b", DataTypes.StringType, false)
                .add("value", DataTypes.StringType, true);
        Dataset<Row> source = spark.createDataFrame(Arrays.asList(
                RowFactory.create("a|b", "c", "first"),
                RowFactory.create("a", "b|c", "second"),
                RowFactory.create("same", "key", "Z"),
                RowFactory.create("same", "key", "A")), schema);
        RowIdentityConfig config = RowIdentityConfig.sourceKey("key_a", "key_b");
        RowIdentityService service = new RowIdentityService();

        RowIdentityResult onePartition = service.identify(source.repartition(1), config);
        RowIdentityResult threePartitions = service.identify(source.repartition(3), config);
        List<String> firstRows = normalizedRows(onePartition.getDataFrame());
        List<String> secondRows = normalizedRows(threePartitions.getDataFrame());

        assertEquals(3L, onePartition.getMetrics().getLogicalRowCount());
        assertEquals(1L, onePartition.getMetrics().getDiscardedDuplicateCount());
        assertEquals(1L, onePartition.getMetrics().getKeyConflictCount());
        assertEquals(firstRows, secondRows);
        List<Row> delimiterRows = onePartition.getDataFrame()
                .filter("key_a = 'a|b' OR key_b = 'b|c'").collectAsList();
        assertEquals(2, delimiterRows.size());
        String firstId = delimiterRows.get(0).getAs(RowIdentityColumns.ROW_ID);
        String secondId = delimiterRows.get(1).getAs(RowIdentityColumns.ROW_ID);
        assertNotEquals(firstId, secondId);
    }

    @Test
    void shouldFoldContentDuplicatesButKeepNullAndEmptySeparate() {
        SparkSession spark = SparkTestSession.get();
        StructType schema = new StructType()
                .add("value", DataTypes.StringType, true);
        Dataset<Row> source = spark.createDataFrame(Arrays.asList(
                RowFactory.create(new Object[] {null}),
                RowFactory.create(new Object[] {null}),
                RowFactory.create(""),
                RowFactory.create("中文"),
                RowFactory.create("中文")), schema);

        RowIdentityResult result = new RowIdentityService().identify(
                source, RowIdentityConfig.contentHash());
        List<Row> rows = result.getDataFrame().collectAsList();

        assertEquals(3L, result.getMetrics().getLogicalRowCount());
        assertEquals(2L, result.getMetrics().getDiscardedDuplicateCount());
        assertEquals(0L, result.getMetrics().getKeyConflictCount());
        for (Row row : rows) {
            String rowId = row.getAs(RowIdentityColumns.ROW_ID);
            assertEquals(32, rowId.length());
            assertTrue(rowId.matches("[0-9a-f]{32}"));
            assertEquals(rowId,
                    row.getAs(RowIdentityColumns.ROW_CONTENT_HASH));
        }
        Row nullRow = result.getDataFrame().filter("value IS NULL").head();
        Row chineseRow = result.getDataFrame().filter("value = '中文'").head();
        assertEquals(2L, (Long) nullRow.getAs(
                RowIdentityColumns.DUPLICATE_COUNT));
        assertEquals(2L, (Long) chineseRow.getAs(
                RowIdentityColumns.DUPLICATE_COUNT));
    }

    @Test
    void shouldKeepHashesStableWhenInputProjectionOrderChanges() {
        SparkSession spark = SparkTestSession.get();
        StructType schema = new StructType()
                .add("z_value", DataTypes.StringType, false)
                .add("a_key", DataTypes.StringType, false);
        Dataset<Row> source = spark.createDataFrame(Collections.singletonList(
                RowFactory.create("内容", "key-1")), schema);
        RowIdentityService service = new RowIdentityService();

        Row first = service.identify(source,
                RowIdentityConfig.sourceKey("a_key")).getDataFrame().head();
        Row second = service.identify(source.select("a_key", "z_value"),
                RowIdentityConfig.sourceKey("a_key")).getDataFrame().head();

        assertEquals((String) first.getAs(RowIdentityColumns.ROW_ID),
                (String) second.getAs(RowIdentityColumns.ROW_ID));
        assertEquals((String) first.getAs(RowIdentityColumns.ROW_CONTENT_HASH),
                (String) second.getAs(RowIdentityColumns.ROW_CONTENT_HASH));
    }

    private static List<String> normalizedRows(Dataset<Row> rows) {
        List<String> values = new ArrayList<String>();
        for (Row row : rows.select("key_a", "key_b", "value",
                RowIdentityColumns.ROW_ID,
                RowIdentityColumns.DUPLICATE_COUNT).collectAsList()) {
            values.add(row.mkString("|"));
        }
        Collections.sort(values);
        return values;
    }
}
