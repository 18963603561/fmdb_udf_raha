package com.fiberhome.ml.raha.data.loader.metadata;

import com.fiberhome.ml.raha.data.domain.DatasetSnapshot;
import com.fiberhome.ml.raha.data.loader.DataFormat;
import com.fiberhome.ml.raha.data.loader.DataLoadRequest;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import java.util.Collections;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * 验证平台未提供来源版本时快照使用确定性内容指纹。
 */
class SnapshotMetadataFactoryTest {

    @Test
    void shouldUseContentFingerprintInsteadOfReadTime() {
        SnapshotMetadataFactory factory = new SnapshotMetadataFactory();
        DataLoadRequest request = request();

        DatasetSnapshot first = factory.create(request, "schema-v1",
                10L, 3, 1000L, "content-v1");
        DatasetSnapshot replay = factory.create(request, "schema-v1",
                10L, 3, 9000L, "content-v1");
        DatasetSnapshot changed = factory.create(request, "schema-v1",
                10L, 3, 9000L, "content-v2");

        assertEquals(first.getSnapshotId(), replay.getSnapshotId());
        assertNotEquals(first.getSnapshotId(), changed.getSnapshotId());
        assertEquals("snapshot_dw.orders@content-content-v1",
                first.getSnapshotId());
    }

    private static DataLoadRequest request() {
        return new DataLoadRequest("dataset", "dw.orders", "dw.orders",
                RowIdentityConfig.contentHash(), DataFormat.FMDB_TABLE,
                Collections.<String, String>emptyMap(),
                Collections.<String>emptySet(), Collections.<String>emptySet(),
                Collections.<String>emptySet(), null, null);
    }
}
