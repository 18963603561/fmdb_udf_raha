package com.fiberhome.ml.raha.data.loader;

import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证路由型数据加载器会根据请求格式选择具体加载器。
 */
class RoutingRahaDatasetLoaderTest {

    @Test
    void shouldRouteFileFormatToFileLoader() {
        AtomicInteger fileCalls = new AtomicInteger();
        AtomicInteger fmdbCalls = new AtomicInteger();
        RoutingRahaDatasetLoader loader = new RoutingRahaDatasetLoader(
                request -> {
                    fileCalls.incrementAndGet();
                    return null;
                },
                request -> {
                    fmdbCalls.incrementAndGet();
                    return null;
                });

        loader.load(request(DataFormat.CSV));

        assertEquals(1, fileCalls.get());
        assertEquals(0, fmdbCalls.get());
    }

    @Test
    void shouldRouteFmdbFormatsToFmdbLoader() {
        AtomicInteger fileCalls = new AtomicInteger();
        AtomicInteger fmdbCalls = new AtomicInteger();
        RoutingRahaDatasetLoader loader = new RoutingRahaDatasetLoader(
                request -> {
                    fileCalls.incrementAndGet();
                    return null;
                },
                request -> {
                    fmdbCalls.incrementAndGet();
                    return null;
                });

        loader.load(request(DataFormat.FMDB_TABLE));
        loader.load(request(DataFormat.FMDB_SQL));

        assertEquals(0, fileCalls.get());
        assertEquals(2, fmdbCalls.get());
    }

    @Test
    void shouldRejectNullRequest() {
        RoutingRahaDatasetLoader loader = new RoutingRahaDatasetLoader(
                request -> null, request -> null);

        assertThrows(IllegalArgumentException.class, () -> loader.load(null));
    }

    private static DataLoadRequest request(DataFormat format) {
        return new DataLoadRequest("dataset", "input", "dataset",
                RowIdentityConfig.contentHash(), format,
                Collections.<String, String>emptyMap(),
                Collections.<String>emptySet(),
                Collections.<String>emptySet(),
                Collections.<String>emptySet(), null, null);
    }
}
