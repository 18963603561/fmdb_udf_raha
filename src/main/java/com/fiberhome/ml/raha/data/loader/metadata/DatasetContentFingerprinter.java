package com.fiberhome.ml.raha.data.loader.metadata;

import com.fiberhome.ml.raha.data.loader.identity.RowIdentityColumns;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * 对已经生成稳定行身份的逻辑数据集计算确定性内容指纹。
 *
 * <p>实现按行内容哈希和物理重复计数稳定排序，并通过本地迭代器流式更新 SHA-256，
 * 避免一次性把全部业务行收集到驱动端内存。</p>
 */
public final class DatasetContentFingerprinter {

    /**
     * 计算包含重复行数量语义的稳定内容指纹。
     *
     * @param dataFrame 已包含 Raha 行身份技术字段的逻辑数据集
     * @return 小写十六进制 SHA-256
     */
    public String fingerprint(Dataset<Row> dataFrame) {
        if (dataFrame == null
                || !hasColumn(dataFrame, RowIdentityColumns.ROW_CONTENT_HASH)
                || !hasColumn(dataFrame, RowIdentityColumns.DUPLICATE_COUNT)) {
            throw new IllegalArgumentException("内容指纹输入缺少行哈希或重复计数字段");
        }
        MessageDigest digest = sha256();
        Iterator<Row> rows = dataFrame.select(
                        functions.col(RowIdentityColumns.ROW_CONTENT_HASH),
                        functions.col(RowIdentityColumns.DUPLICATE_COUNT))
                .orderBy(functions.col(RowIdentityColumns.ROW_CONTENT_HASH).asc(),
                        functions.col(RowIdentityColumns.DUPLICATE_COUNT).asc())
                .toLocalIterator();
        long count = 0L;
        while (rows.hasNext()) {
            Row row = rows.next();
            updateToken(digest, row.getString(0));
            updateToken(digest, String.valueOf(
                    ((Number) row.get(1)).longValue()));
            count++;
        }
        updateToken(digest, String.valueOf(count));
        return toHex(digest.digest());
    }

    private static boolean hasColumn(Dataset<Row> frame, String columnName) {
        for (String name : frame.columns()) {
            if (columnName.equals(name)) {
                return true;
            }
        }
        return false;
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("当前 JVM 不支持 SHA-256", exception);
        }
    }

    private static void updateToken(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(String.valueOf(bytes.length).getBytes(StandardCharsets.US_ASCII));
        digest.update((byte) ':');
        digest.update(bytes);
    }

    private static String toHex(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            result.append(String.format("%02x", value & 0xff));
        }
        return result.toString();
    }
}
