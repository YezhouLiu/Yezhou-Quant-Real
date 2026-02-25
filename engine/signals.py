from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable, Literal, Sequence

import pandas as pd
import psycopg

from engine.normalizer import rank_normalize, magnitude_normalize

log = logging.getLogger(__name__)


Method = Literal["rank", "mag"]


@dataclass(frozen=True)
class FactorSpec:
    """
    规定某个因子的“方向”和“需要生成哪些信号”。

    factor_name: DB 里 factor_name 的值
    ascending : True  表示值越大越好；False 表示值越小越好（风险类）
    methods   : 生成哪些列：rank / mag
    mag_activation: "tanh" 或 "sigmoid"
    mag_clip_quantile: 幅度分裁剪分位数
    mag_z_clip: robust z 之后的裁剪，防止饱和
    """
    factor_name: str
    ascending: bool = True
    methods: Sequence[Method] = ("rank", "mag")

    mag_activation: str = "tanh"
    mag_clip_quantile: float | None = 0.01
    mag_z_clip: float = 6.0


def normalize_cross_section(
    df_wide: pd.DataFrame,
    specs: Sequence[FactorSpec],
    *,
    id_col: str = "instrument_id",
) -> pd.DataFrame:
    """
    对某一交易日的横截面宽表进行标准化，生成 *_rank / *_mag 信号列。

    df_wide: 必须包含 instrument_id + 每个 factor_name 一列
    返回: instrument_id + 生成的信号列（原始因子列保留，方便 debug）
    
    注意：自动过滤掉任何因子数据缺失（NaN）的股票
    """
    if id_col not in df_wide.columns:
        raise KeyError(f"missing id_col: {id_col}")

    out = df_wide.copy()
    
    # 检查是否有数据
    if len(out) == 0:
        raise ValueError("df_wide is empty - no factor data available for this date")
    
    # 检查所有需要的因子列是否存在
    missing_factors = []
    for spec in specs:
        if spec.factor_name not in out.columns:
            missing_factors.append(spec.factor_name)
    
    if missing_factors:
        raise KeyError(f"missing factor columns in df_wide: {missing_factors}")
    
    if missing_factors:
        raise KeyError(f"missing factor columns in df_wide: {missing_factors}")
    
    # 过滤掉任何因子有 NaN 的行（数据不全的股票）
    factor_cols = [spec.factor_name for spec in specs]
    before_count = len(out)
    out = out.dropna(subset=factor_cols)
    after_count = len(out)

    if before_count > after_count:
        dropped = before_count - after_count
        log.debug(f"Filtered {dropped} instruments with incomplete factor data")

    if len(out) == 0:
        raise ValueError("All instruments have missing factor data")

    for spec in specs:
        for m in spec.methods:
            if m == "rank":
                out[f"{spec.factor_name}_rank"] = rank_normalize(
                    out[spec.factor_name],
                    ascending=spec.ascending,
                    to_range="minus1_1",
                )
            elif m == "mag":
                out[f"{spec.factor_name}_mag"] = magnitude_normalize(
                    out[spec.factor_name],
                    ascending=spec.ascending,
                    clip_quantile=spec.mag_clip_quantile,
                    activation=spec.mag_activation,
                    z_clip=spec.mag_z_clip,
                )
            else:
                raise ValueError(f"unknown method: {m}")

    return out


def fetch_factors_long_for_date(
    conn: psycopg.Connection,
    *,
    asof_date: str,  # 'YYYY-MM-DD'
    factor_names: Sequence[str],
    factor_version: str | None = None,
    universe_ids: Sequence[int] | None = None,
) -> pd.DataFrame:
    """
    从 DB 读某一天的因子长表。

    返回: DataFrame with columns [instrument_id, date, factor_name, factor_value]
    """
    from database.readwrite.rw_factor_values import get_factor_values
    
    if len(factor_names) == 0:
        raise ValueError("factor_names cannot be empty")

    if universe_ids is not None and len(universe_ids) == 0:
        raise ValueError("universe_ids provided but empty")

    df = get_factor_values(
        conn,
        factor_names=list(factor_names),
        factor_version=factor_version,
        instrument_ids=list(universe_ids) if universe_ids else None,
        date=asof_date,
    )

    # RW 返回的列名是 factor_value，需要重命名为 value 以保持兼容
    if df.empty:
        return pd.DataFrame(columns=["instrument_id", "date", "factor_name", "value"])
    
    # 只保留需要的列并重命名
    df = df[["instrument_id", "date", "factor_name", "factor_value"]].copy()
    df.rename(columns={"factor_value": "value"}, inplace=True)
    
    return df


def pivot_factors_long_to_wide(
    df_long: pd.DataFrame,
    *,
    id_col: str = "instrument_id",
    factor_col: str = "factor_name",
    value_col: str = "value",
) -> pd.DataFrame:
    """
    长表 -> 宽表：每个 factor_name 变成一列。
    """
    for c in (id_col, factor_col, value_col):
        if c not in df_long.columns:
            raise KeyError(f"missing column in df_long: {c}")

    if df_long.empty:
        # 返回空 DataFrame，但保留 instrument_id 列
        return pd.DataFrame(columns=[id_col])

    df_wide = (
        df_long.pivot(index=id_col, columns=factor_col, values=value_col)
        .reset_index()
    )

    # pivot 后 columns 可能是 Index，转成普通列名
    df_wide.columns.name = None
    return df_wide


def build_signals_for_date(
    conn: psycopg.Connection,
    *,
    asof_date: str,
    specs: Sequence[FactorSpec],
    factor_version: str | None = None,
    universe_ids: Sequence[int] | None = None,
) -> pd.DataFrame:
    """
    一步到位：
    DB -> (long) -> (wide) -> normalized signals
    """
    factor_names = [s.factor_name for s in specs]

    df_long = fetch_factors_long_for_date(
        conn,
        asof_date=asof_date,
        factor_names=factor_names,
        factor_version=factor_version,
        universe_ids=universe_ids,
    )

    df_wide = pivot_factors_long_to_wide(df_long)
    df_sig = normalize_cross_section(df_wide, specs)
    return df_sig
