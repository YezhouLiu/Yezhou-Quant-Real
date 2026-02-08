import numpy as np
import pandas as pd


def rank_normalize(
    values: pd.Series,
    ascending: bool = True,
    to_range: str = "minus1_1",
) -> pd.Series:
    """
    横截面 rank 标准化

    Parameters
    ----------
    values : pd.Series
        原始因子值（同一时间点的横截面）
    ascending : bool
        True  表示值越大越好
        False 表示值越小越好（如 volatility）
    to_range : {"minus1_1", "0_1"}

    Returns
    -------
    pd.Series
        rank-based score
    """
    pct_rank = values.rank(pct=True, ascending=ascending)

    if to_range == "0_1":
        return pct_rank

    if to_range == "minus1_1":
        return 2.0 * pct_rank - 1.0

    raise ValueError(f"Unknown to_range: {to_range}")


def magnitude_normalize(
    values: pd.Series,
    *,
    ascending: bool = True,
    clip_quantile: float | None = 0.01,
    activation: str = "tanh",
    z_clip: float = 6.0,
) -> pd.Series:
    """
    幅度型标准化（方向 + 强度）

    Parameters
    ----------
    values : pd.Series
        原始因子值（横截面）
    ascending : bool
        True  表示值越大越好
        False 表示值越小越好
    clip_quantile : float or None
        裁剪极端值的分位数（如 0.01）。若为 None 则不裁剪
    activation : {"tanh", "sigmoid"}
    z_clip : float
        对 robust z-score 做裁剪，防止 MAD 很小时输出爆表导致 tanh/sigmoid 饱和或 exp 溢出

    Returns
    -------
    pd.Series
        连续幅度分数
        tanh    -> (-1, 1)
        sigmoid -> (0, 1)
    """
    x = values.copy()

    # 方向统一：始终是“越大越好”
    if not ascending:
        x = -x

    # 裁剪极端值（非常重要）
    if clip_quantile is not None:
        if not (0.0 < clip_quantile < 0.5):
            raise ValueError("clip_quantile must be in (0, 0.5)")
        lo = x.quantile(clip_quantile)
        hi = x.quantile(1.0 - clip_quantile)
        x = x.clip(lo, hi)

    z = robust_zscore(x)

    if z_clip <= 0:
        raise ValueError("z_clip must be > 0")

    # 防爆表：避免 tanh/sigmoid 饱和成精确 1.0 / 0.0，或 sigmoid 的 exp 溢出
    z = z.clip(lower=-z_clip, upper=z_clip)

    if activation == "tanh":
        return tanh(z)

    if activation == "sigmoid":
        return sigmoid(z)

    raise ValueError(f"Unknown activation: {activation}")


def robust_zscore(values: pd.Series) -> pd.Series:
    """
    Robust Z-score using median and MAD
    """
    median = values.median()
    mad = (values - median).abs().median()

    if mad == 0 or pd.isna(mad):
        # 全部一样 or 全 NaN 等情况，直接返回 0（NaN 会自然保留在后续映射中）
        return pd.Series(0.0, index=values.index)

    return (values - median) / mad


def sigmoid(x: pd.Series) -> pd.Series:
    # x 已经被 z_clip 裁过了，这里不会 exp 溢出
    return 1.0 / (1.0 + np.exp(-x))


def tanh(x: pd.Series) -> pd.Series:
    return np.tanh(x)
