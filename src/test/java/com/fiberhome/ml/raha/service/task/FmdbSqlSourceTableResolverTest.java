package com.fiberhome.ml.raha.service.task;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证 SQL 输入按首个来源表生成默认模型来源。
 */
class FmdbSqlSourceTableResolverTest {

    @Test
    void shouldResolveFirstTableFromSimpleSql() {
        assertEquals("dw.orders", FmdbSqlSourceTableResolver.firstSourceTable(
                "select * from `DW`.`ORDERS` where dt = '20260721'"));
    }

    @Test
    void shouldResolveOnlyFirstTableWhenJoinHasMoreTables() {
        assertEquals("dw.orders", FmdbSqlSourceTableResolver.firstSourceTable(
                "select * from dw.orders o join dw.customer c on o.id = c.id"));
    }

    @Test
    void shouldResolveFirstPhysicalTableInsideSubQuery() {
        assertEquals("dw.orders", FmdbSqlSourceTableResolver.firstSourceTable(
                "select * from (select * from dw.orders) t"));
    }

    @Test
    void shouldUseDefaultDatabaseForUnqualifiedTable() {
        assertEquals("default.orders", FmdbSqlSourceTableResolver.firstSourceTable(
                "select * from orders"));
    }

    @Test
    void shouldRejectMutatingSql() {
        assertThrows(IllegalArgumentException.class,
                () -> FmdbSqlSourceTableResolver.firstSourceTable(
                        "delete from dw.orders"));
    }
}
