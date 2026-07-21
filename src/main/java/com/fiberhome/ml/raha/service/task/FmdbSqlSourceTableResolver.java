package com.fiberhome.ml.raha.service.task;

import com.fiberhome.ml.raha.util.ReadableIdUtils;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * 从 FMDB 只读 SQL 中提取第一个真实来源表。
 */
public final class FmdbSqlSourceTableResolver {

    /** 默认解析器实例。 */
    private static final FmdbSqlSourceTableResolver DEFAULT =
            new FmdbSqlSourceTableResolver();

    /**
     * 使用默认规则解析第一个来源表。
     *
     * @param sql 只读 SQL
     * @return 规范化后的库名和表名
     */
    public static String firstSourceTable(String sql) {
        return DEFAULT.resolve(sql);
    }

    /**
     * 解析第一个来源表，多个表或多条 SQL 只取扫描到的第一个表。
     *
     * @param sql 只读 SQL
     * @return 规范化后的库名和表名
     */
    public String resolve(String sql) {
        String text = ValueUtils.requireNotBlank(sql, "FMDB SQL").trim();
        String lower = text.toLowerCase(Locale.ROOT);
        if (!lower.startsWith("select ") && !lower.startsWith("with ")) {
            throw new IllegalArgumentException("FMDB SQL 只允许 SELECT 或 WITH 查询");
        }
        List<String> tokens = tokenize(text);
        for (int index = 0; index < tokens.size(); index++) {
            String token = tokens.get(index).toLowerCase(Locale.ROOT);
            if (!"from".equals(token) && !"join".equals(token)) {
                continue;
            }
            String table = nextTable(tokens, index + 1);
            if (table != null) {
                return ReadableIdUtils.normalizeSourceName(table);
            }
        }
        throw new IllegalArgumentException("FMDB SQL 未解析到来源表");
    }

    private static String nextTable(List<String> tokens, int start) {
        for (int index = start; index < tokens.size(); index++) {
            String token = tokens.get(index);
            String lower = token.toLowerCase(Locale.ROOT);
            if ("(".equals(token) || ",".equals(token)) {
                continue;
            }
            // 表生成函数、横向视图或值列表不是可绑定模型的数据表。
            if ("select".equals(lower) || "with".equals(lower)
                    || "lateral".equals(lower) || "values".equals(lower)) {
                return null;
            }
            if (isClauseBoundary(lower) || ")".equals(token)
                    || ";".equals(token)) {
                return null;
            }
            return token;
        }
        return null;
    }

    private static boolean isClauseBoundary(String token) {
        return "where".equals(token) || "group".equals(token)
                || "order".equals(token) || "having".equals(token)
                || "limit".equals(token) || "union".equals(token)
                || "on".equals(token);
    }

    private static List<String> tokenize(String sql) {
        List<String> tokens = new ArrayList<String>();
        StringBuilder current = new StringBuilder();
        boolean quotedIdentifier = false;
        char quote = 0;
        for (int index = 0; index < sql.length(); index++) {
            char ch = sql.charAt(index);
            if (!quotedIdentifier && ch == '-' && index + 1 < sql.length()
                    && sql.charAt(index + 1) == '-') {
                index = skipLineComment(sql, index + 2);
                continue;
            }
            if (!quotedIdentifier && ch == '/' && index + 1 < sql.length()
                    && sql.charAt(index + 1) == '*') {
                index = skipBlockComment(sql, index + 2);
                continue;
            }
            if (!quotedIdentifier && ch == '\'') {
                index = skipString(sql, index, ch);
                continue;
            }
            if (quotedIdentifier) {
                current.append(ch);
                if (ch == quote) {
                    quotedIdentifier = false;
                    quote = 0;
                }
                continue;
            }
            if (ch == '`' || ch == '[' || ch == '"') {
                if (current.length() > 0
                        && current.charAt(current.length() - 1) != '.') {
                    flush(tokens, current);
                }
                quotedIdentifier = true;
                quote = ch == '[' ? ']' : ch;
                current.append(ch);
                continue;
            }
            if (Character.isWhitespace(ch) || ch == ',' || ch == '('
                    || ch == ')' || ch == ';') {
                flush(tokens, current);
                if (ch == ',' || ch == '(' || ch == ')' || ch == ';') {
                    tokens.add(String.valueOf(ch));
                }
                continue;
            }
            current.append(ch);
        }
        flush(tokens, current);
        return tokens;
    }

    private static int skipLineComment(String sql, int index) {
        int current = index;
        while (current < sql.length() && sql.charAt(current) != '\n') {
            current++;
        }
        return current;
    }

    private static int skipBlockComment(String sql, int index) {
        int current = index;
        while (current + 1 < sql.length()) {
            if (sql.charAt(current) == '*' && sql.charAt(current + 1) == '/') {
                return current + 1;
            }
            current++;
        }
        return sql.length() - 1;
    }

    private static int skipString(String sql, int index, char quote) {
        int current = index + 1;
        while (current < sql.length()) {
            char ch = sql.charAt(current);
            if (ch == quote) {
                if (current + 1 < sql.length()
                        && sql.charAt(current + 1) == quote) {
                    current += 2;
                    continue;
                }
                return current;
            }
            current++;
        }
        return sql.length() - 1;
    }

    private static void flush(List<String> tokens, StringBuilder current) {
        if (current.length() == 0) {
            return;
        }
        tokens.add(current.toString());
        current.setLength(0);
    }
}
