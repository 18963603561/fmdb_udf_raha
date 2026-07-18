package com.fiberhome.ml.raha.profile;

import com.fiberhome.ml.raha.data.ColumnProfile;
import com.fiberhome.ml.raha.data.RahaDataset;

import java.util.List;

/**
 * 生成目标字段画像的算法接口。
 */
public interface ColumnProfiler {

    List<ColumnProfile> profile(RahaDataset dataset);
}
