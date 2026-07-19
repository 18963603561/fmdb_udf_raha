package com.fiberhome.ml.raha.data.loader;

import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import com.fiberhome.ml.raha.data.loader.metadata.SchemaHasher;
import java.util.Collections;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证模式哈希对相同模式稳定，并能识别字段顺序和类型变化。
 */
class SchemaHasherTest {

    @Test
    void shouldGenerateStableSchemaHash() {
        StructType first = new StructType()
                .add("id", DataTypes.StringType, false)
                .add("amount", DataTypes.DoubleType, true);
        StructType same = new StructType()
                .add("id", DataTypes.StringType, false)
                .add("amount", DataTypes.DoubleType, true);
        StructType reordered = new StructType()
                .add("amount", DataTypes.DoubleType, true)
                .add("id", DataTypes.StringType, false);
        StructType changedType = new StructType()
                .add("id", DataTypes.StringType, false)
                .add("amount", DataTypes.StringType, true);
        SchemaHasher hasher = new SchemaHasher();

        assertEquals(hasher.hash(first), hasher.hash(same));
        assertNotEquals(hasher.hash(first), hasher.hash(reordered));
        assertNotEquals(hasher.hash(first), hasher.hash(changedType));
    }

    @Test
    void shouldRejectConflictingColumnFilters() {
        assertThrows(IllegalArgumentException.class, () -> new DataLoadRequest(
                "dataset", "input", "table", RowIdentityConfig.sourceKey("id"),
                DataFormat.CSV,
                Collections.<String, String>emptyMap(),
                Collections.singleton("name"),
                Collections.singleton("name"),
                Collections.<String>emptySet(), null, "v1"));
    }
}
