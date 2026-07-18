package com.fiberhome.ml.raha.data;

import com.fiberhome.ml.raha.support.RahaErrorCode;
import com.fiberhome.ml.raha.support.RahaException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 将可选目标字段展开为按输入模式排序的确定列表。
 */
public final class TargetColumnResolver {

    private TargetColumnResolver() {
    }

    /**
     * 解析目标字段。未指定时返回全部可用字段；显式字段保持可用字段顺序。
     *
     * @param requested 调用方字段
     * @param available 当前阶段可用字段
     * @return 确定目标字段
     */
    public static List<String> resolve(List<String> requested, List<String> available) {
        if (available == null || available.isEmpty()) {
            throw new RahaException(RahaErrorCode.INVALID_DATA, "没有可处理字段");
        }
        if (requested == null || requested.isEmpty()) {
            return new ArrayList<String>(available);
        }
        Set<String> requestedSet = new HashSet<String>();
        for (String column : requested) {
            if (column == null || column.trim().isEmpty()) {
                throw new RahaException(RahaErrorCode.INVALID_REQUEST, "目标字段不能为空");
            }
            if (!requestedSet.add(column)) {
                throw new RahaException(RahaErrorCode.INVALID_REQUEST,
                        "目标字段不能重复：" + column);
            }
        }
        List<String> resolved = new ArrayList<String>();
        for (String column : available) {
            if (requestedSet.remove(column)) {
                resolved.add(column);
            }
        }
        if (!requestedSet.isEmpty()) {
            throw new RahaException(RahaErrorCode.INVALID_REQUEST,
                    "目标字段不存在或当前阶段不可用：" + requestedSet);
        }
        return resolved;
    }
}
