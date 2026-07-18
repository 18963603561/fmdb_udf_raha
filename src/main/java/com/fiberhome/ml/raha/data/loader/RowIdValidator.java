package com.fiberhome.ml.raha.data.loader;

import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.Arrays;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.trim;

/**
 * 校验 Spark 数据集中的行标识字段存在、非空且唯一。
 */
public final class RowIdValidator {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(RowIdValidator.class);

    public RowIdValidationResult validate(Dataset<Row> dataFrame, String rowIdColumn) {
        if (dataFrame == null) {
            throw new IllegalArgumentException("Spark 数据集不能为空");
        }
        String validatedColumn = ValueUtils.requireNotBlank(rowIdColumn, "行标识字段");
        if (!Arrays.asList(dataFrame.columns()).contains(validatedColumn)) {
            throw new DataValidationException(DataValidationErrorCode.ROW_ID_COLUMN_MISSING,
                    "输入数据不存在行标识字段：" + validatedColumn);
        }
        long rowCount = dataFrame.count();
        if (rowCount <= 0L) {
            throw new DataValidationException(DataValidationErrorCode.EMPTY_DATASET, "输入数据不能为空");
        }

        Column rowId = quotedColumn(validatedColumn);
        long invalidCount = dataFrame.filter(rowId.isNull()
                .or(trim(rowId.cast("string")).equalTo(""))).limit(1).count();
        if (invalidCount > 0L) {
            throw new DataValidationException(DataValidationErrorCode.ROW_ID_NULL_OR_BLANK,
                    "行标识字段包含空值或空白值：" + validatedColumn);
        }

        long duplicateCount = dataFrame.groupBy(rowId).count()
                .filter(col("count").gt(1L)).limit(1).count();
        if (duplicateCount > 0L) {
            throw new DataValidationException(DataValidationErrorCode.ROW_ID_DUPLICATED,
                    "行标识字段存在重复值：" + validatedColumn);
        }
        LOGGER.info("行标识校验通过，rowIdColumn={}，rowCount={}", validatedColumn, rowCount);
        return new RowIdValidationResult(rowCount);
    }

    private static Column quotedColumn(String columnName) {
        return col("`" + columnName.replace("`", "``") + "`");
    }
}

