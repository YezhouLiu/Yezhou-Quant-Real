from typing import List
import pandas as pd


def align_series(series_list: List[pd.DataFrame]) -> List[pd.DataFrame]:
    """
    以所有序列日期的并集为主轴，缺失点用前向填充（ffill）。
    适用于 Portfolio（每月一点）与个股（每日）混合对比：
    portfolio 会在日线上呈阶梯形展示，个股保持完整每日曲线。
    """
    all_dates = series_list[0].index
    for df in series_list[1:]:
        all_dates = all_dates.union(df.index)
    all_dates = all_dates.sort_values()

    aligned = []
    for df in series_list:
        reindexed = df.reindex(all_dates).ffill().dropna()
        aligned.append(reindexed)

    # 取所有序列都有数据的共同起始日
    start = max(df.index[0] for df in aligned)
    aligned = [df.loc[start:] for df in aligned]

    return aligned
