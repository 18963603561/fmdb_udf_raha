package com.fiberhome.ml.raha.data.loader.metadata;

import com.fiberhome.ml.raha.data.loader.identity.RowIdentityColumns;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import java.util.Arrays;
import java.util.Collections;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证逻辑数据集内容指纹与行顺序无关，并保留物理重复数量语义。
 */
class DatasetContentFingerprinterTest {

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldGenerateStableFingerprintAndDetectDuplicateCountChange() {
        StructType schema = new StructType()
                .add(RowIdentityColumns.ROW_CONTENT_HASH,
                        DataTypes.StringType, false)
                .add(RowIdentityColumns.DUPLICATE_COUNT,
                        DataTypes.LongType, false);
        Dataset<Row> first = SparkTestSession.get().createDataFrame(Arrays.asList(
                RowFactory.create("hash-b", 2L),
                RowFactory.create("hash-a", 1L)), schema);
        Dataset<Row> reordered = SparkTestSession.get().createDataFrame(Arrays.asList(
                RowFactory.create("hash-a", 1L),
                RowFactory.create("hash-b", 2L)), schema);
        Dataset<Row> changed = SparkTestSession.get().createDataFrame(Arrays.asList(
                RowFactory.create("hash-a", 1L),
                RowFactory.create("hash-b", 3L)), schema);
        DatasetContentFingerprinter fingerprinter =
                new DatasetContentFingerprinter();

        assertEquals(fingerprinter.fingerprint(first),
                fingerprinter.fingerprint(reordered));
        assertNotEquals(fingerprinter.fingerprint(first),
                fingerprinter.fingerprint(changed));
    }

    @Test
    void shouldRejectDatasetWithoutIdentityColumns() {
        StructType schema = new StructType()
                .add("value", DataTypes.StringType, true);
        Dataset<Row> frame = SparkTestSession.get().createDataFrame(
                Collections.singletonList(RowFactory.create("value")), schema);

        assertThrows(IllegalArgumentException.class,
                () -> new DatasetContentFingerprinter().fingerprint(frame));
    }
}
