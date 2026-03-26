# =============================================================================
# Yezhou Capital Limited  |  Proprietary & Confidential
# =============================================================================
# Copyright (c) 2026 Yezhou Capital Limited. All rights reserved.
#
# Project  : Yezhou Quantitative Trading System
# Author   : Yezhou Liu
# Contact  : yezhoucapital@gmail.com
#
# This source code is the exclusive property of Yezhou Capital Limited.
# Unauthorized copying, modification, distribution, or use of this file,
# via any medium, is strictly prohibited without prior written consent.
# =============================================================================
import pandas as pd


# ----------------------------------------------------------------------------------------------------------------------------------------
# 对 DataFrame 每列进行 z-score 标准化，忽略 NaN
# 等价于 scipy.stats.zscore(df, nan_policy="omit", axis=0)
# ----------------------------------------------------------------------------------------------------------------------------------------
def zscore_df(df: pd.DataFrame) -> pd.DataFrame:
    return (df - df.mean()) / df.std(ddof=0)


# ----------------------------------------------------------------------------------------------------------------------------------------
# 对一个 pandas Series 进行标准化（减去均值除以标准差），保留 NaN
# ----------------------------------------------------------------------------------------------------------------------------------------
def zscore_series(series: pd.Series) -> pd.Series:
    mean = series.mean()
    std = series.std()
    if std == 0 or pd.isna(std):
        return pd.Series([0] * len(series), index=series.index)
    return (series - mean) / std
