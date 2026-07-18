package com.fiberhome.ml.raha.repository.adapter;

import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;

/**
 * 返回标签内容及其配置、快照、阶段和更新时间版本。
 */
public final class StoredCellLabel {

    /** 单元格标签。 */
    private final CellLabel label;
    /** 标签仓储业务版本。 */
    private final ArtifactVersion artifactVersion;
    /** 标签记录更新时间。 */
    private final long updatedAt;

    public StoredCellLabel(CellLabel label,
                           ArtifactVersion artifactVersion,
                           long updatedAt) {
        if (label == null || artifactVersion == null || updatedAt <= 0L) {
            throw new IllegalArgumentException("标签、业务版本和更新时间必须有效");
        }
        this.label = label;
        this.artifactVersion = artifactVersion;
        this.updatedAt = updatedAt;
    }

    public CellLabel getLabel() { return label; }
    public ArtifactVersion getArtifactVersion() { return artifactVersion; }
    public long getUpdatedAt() { return updatedAt; }
}
