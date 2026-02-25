import numpy as np
import pandas as pd
import pytest
from unittest.mock import MagicMock

from engine.signals import (
    FactorSpec, 
    normalize_cross_section, 
    pivot_factors_long_to_wide,
    fetch_factors_long_for_date,
)


def test_pivot_factors_long_to_wide_basic():
    df_long = pd.DataFrame(
        {
            "instrument_id": [1, 1, 2, 2],
            "factor_name": ["mom", "vol", "mom", "vol"],
            "value": [0.1, 0.3, 0.2, 0.5],
        }
    )
    df_wide = pivot_factors_long_to_wide(df_long)

    assert "instrument_id" in df_wide.columns
    assert "mom" in df_wide.columns
    assert "vol" in df_wide.columns
    assert df_wide.shape[0] == 2


def test_normalize_cross_section_creates_expected_columns():
    df_wide = pd.DataFrame(
        {
            "instrument_id": [1, 2, 3, 4],
            "mom": [0.05, -0.08, -0.081, -0.082],
            "vol": [0.10, 0.20, 0.15, 0.30],
        }
    )

    specs = [
        FactorSpec("mom", ascending=True, methods=("rank", "mag"), mag_activation="tanh", mag_clip_quantile=None),
        FactorSpec("vol", ascending=False, methods=("rank", "mag"), mag_activation="tanh", mag_clip_quantile=None),
    ]

    out = normalize_cross_section(df_wide, specs)

    for col in ("mom_rank", "mom_mag", "vol_rank", "vol_mag"):
        assert col in out.columns


def test_normalize_cross_section_directionality():
    df_wide = pd.DataFrame(
        {
            "instrument_id": [1, 2, 3],
            "mom": [0.2, 0.0, -0.1],     # 越大越好
            "vol": [0.3, 0.1, 0.2],      # 越小越好
        }
    )
    specs = [
        FactorSpec("mom", ascending=True, methods=("rank", "mag"), mag_activation="tanh", mag_clip_quantile=None),
        FactorSpec("vol", ascending=False, methods=("rank", "mag"), mag_activation="tanh", mag_clip_quantile=None),
    ]
    out = normalize_cross_section(df_wide, specs)

    # mom: 0.2 应该最高
    assert out.loc[out["instrument_id"] == 1, "mom_rank"].iloc[0] > out.loc[out["instrument_id"] == 2, "mom_rank"].iloc[0]
    assert out.loc[out["instrument_id"] == 2, "mom_rank"].iloc[0] > out.loc[out["instrument_id"] == 3, "mom_rank"].iloc[0]

    # vol: 0.1 最小，所以 rank 应该最高（ascending=False）
    v2 = out.loc[out["instrument_id"] == 2, "vol_rank"].iloc[0]
    v3 = out.loc[out["instrument_id"] == 3, "vol_rank"].iloc[0]
    v1 = out.loc[out["instrument_id"] == 1, "vol_rank"].iloc[0]
    assert v2 > v3 > v1


def test_normalize_cross_section_missing_factor_column_raises():
    df_wide = pd.DataFrame({"instrument_id": [1, 2], "mom": [0.1, 0.2]})
    specs = [FactorSpec("vol", ascending=False)]
    with pytest.raises(KeyError):
        normalize_cross_section(df_wide, specs)


def test_normalize_cross_section_missing_id_col_raises():
    df_wide = pd.DataFrame({"mom": [0.1, 0.2]})
    specs = [FactorSpec("mom", ascending=True)]
    with pytest.raises(KeyError):
        normalize_cross_section(df_wide, specs)


def test_pivot_missing_columns_raises():
    df_long = pd.DataFrame({"instrument_id": [1], "value": [0.1]})
    with pytest.raises(KeyError):
        pivot_factors_long_to_wide(df_long)


def test_fetch_factors_long_for_date_uses_rw_method(monkeypatch):
    """测试 fetch_factors_long_for_date 使用 RW 方法"""
    
    def fake_get_factor_values(conn, **kwargs):
        # 模拟返回 RW 方法的格式（含 factor_value 列）
        return pd.DataFrame({
            "instrument_id": [1, 2],
            "date": ["2026-01-01", "2026-01-01"],
            "factor_name": ["mom_21d", "mom_21d"],
            "factor_value": [0.1, 0.2],
            "factor_version": ["v1", "v1"],
        })
    
    # patch RW 模块中的函数（因为 signals 里是 import 后调用）
    monkeypatch.setattr(
        "database.readwrite.rw_factor_values.get_factor_values",
        fake_get_factor_values,
    )
    
    conn = MagicMock()
    df = fetch_factors_long_for_date(
        conn,
        asof_date="2026-01-01",
        factor_names=["mom_21d"],
        factor_version="v1",
        universe_ids=[1, 2],
    )
    
    # 验证返回的列名（应该是 value 而不是 factor_value）
    assert "value" in df.columns
    assert "factor_value" not in df.columns
    assert list(df.columns) == ["instrument_id", "date", "factor_name", "value"]
    assert len(df) == 2


def test_fetch_factors_long_for_date_empty_result(monkeypatch):
    """测试空结果返回正确的列结构"""
    
    def fake_get_factor_values(conn, **kwargs):
        return pd.DataFrame()
    
    monkeypatch.setattr(
        "database.readwrite.rw_factor_values.get_factor_values",
        fake_get_factor_values,
    )
    
    conn = MagicMock()
    df = fetch_factors_long_for_date(
        conn,
        asof_date="2026-01-01",
        factor_names=["mom_21d"],
    )
    
    assert df.empty
    assert list(df.columns) == ["instrument_id", "date", "factor_name", "value"]


def test_fetch_factors_long_for_date_validates_inputs():
    """测试输入验证"""
    conn = MagicMock()
    
    # 空 factor_names
    with pytest.raises(ValueError, match="cannot be empty"):
        fetch_factors_long_for_date(
            conn,
            asof_date="2026-01-01",
            factor_names=[],
        )
    
    # 空 universe_ids
    with pytest.raises(ValueError, match="universe_ids provided but empty"):
        fetch_factors_long_for_date(
            conn,
            asof_date="2026-01-01",
            factor_names=["mom_21d"],
            universe_ids=[],
        )

