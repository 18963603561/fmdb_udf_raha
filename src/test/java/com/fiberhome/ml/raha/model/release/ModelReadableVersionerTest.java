package com.fiberhome.ml.raha.model.release;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证模型可读版本和安全文件名规则。
 */
class ModelReadableVersionerTest {

    @Test
    void shouldGenerateReadableModelSetAndColumnVersions() {
        String modelSetVersion = ModelReadableVersioner.modelSetVersion(
                "`DW`.`ORDERS`", 1000L);
        String modelVersion = ModelReadableVersioner.columnModelVersion(
                modelSetVersion, "Order Code");

        assertEquals("dw.orders@19700101000001.000", modelSetVersion);
        assertEquals("dw.orders.order_code@19700101000001.000",
                modelVersion);
        assertEquals("raha-dw.orders-order_code",
                ModelReadableVersioner.modelName("raha", "dw.orders",
                        "Order Code"));
    }

    @Test
    void shouldAppendReadableUniquenessToModelSetVersion() {
        String modelSetVersion = ModelReadableVersioner.modelSetVersion(
                "dw.orders", 1000L, "train-job-1");
        String modelVersion = ModelReadableVersioner.columnModelVersion(
                modelSetVersion, "code");

        assertEquals("dw.orders@19700101000001.000-train-job-1",
                modelSetVersion);
        assertEquals("dw.orders.code@19700101000001.000-train-job-1",
                modelVersion);
    }

    @Test
    void shouldCreateSafeModelFileToken() {
        String token = ModelReadableVersioner.safeFileToken(
                "dw.orders.order_code@19700101000001.000");

        assertEquals("dw.orders.order_code_19700101000001.000", token);
        assertTrue(token.endsWith(".000"));
    }
}
