import numpy as np
import pandas as pd
import pytest

from engine.normalizer import (
    rank_normalize,
    robust_zscore,
    magnitude_normalize,
)


def test_rank_normalize_minus1_1_range_and_monotonic():
    s = pd.Series([1.0, 2.0, 3.0], index=["a", "b", "c"])
    out = rank_normalize(s, ascending=True, to_range="minus1_1")

    assert out.min() >= -1.0
    assert out.max() <= 1.0
    assert out["c"] > out["b"] > out["a"]


def test_rank_normalize_0_1_range():
    s = pd.Series([10.0, 20.0, 30.0])
    out = rank_normalize(s, to_range="0_1")
    assert out.min() >= 0.0
    assert out.max() <= 1.0


def test_rank_normalize_ascending_false_reverses_order():
    s = pd.Series([1.0, 2.0, 3.0], index=["a", "b", "c"])
    out = rank_normalize(s, ascending=False, to_range="minus1_1")
    assert out["a"] > out["b"] > out["c"]


def test_rank_normalize_invalid_to_range_raises():
    s = pd.Series([1.0, 2.0, 3.0])
    with pytest.raises(ValueError):
        rank_normalize(s, to_range="nope")


def test_robust_zscore_constant_series_returns_zeros():
    s = pd.Series([5.0, 5.0, 5.0], index=["a", "b", "c"])
    out = robust_zscore(s)
    assert (out == 0.0).all()


def test_robust_zscore_median_is_zero():
    s = pd.Series([1.0, 2.0, 3.0])
    out = robust_zscore(s)
    assert out.iloc[1] == 0.0


def test_magnitude_normalize_tanh_range_and_direction():
    s = pd.Series([0.05, -0.08, -0.081, -0.082], index=["a", "b", "c", "d"])
    out = magnitude_normalize(
        s,
        ascending=True,
        activation="tanh",
        clip_quantile=None,
        z_clip=6.0,
    )

    assert out.min() >= -1.0
    assert out.max() <= 1.0
    assert out["a"] > out["b"] > out["c"] > out["d"]


def test_magnitude_normalize_sigmoid_range_and_direction():
    s = pd.Series([0.05, -0.08, -0.081, -0.082], index=["a", "b", "c", "d"])
    out = magnitude_normalize(
        s,
        ascending=True,
        activation="sigmoid",
        clip_quantile=None,
        z_clip=6.0,
    )

    # sigmoid 输出应在 [0,1]（严格 (0,1) 也通常成立，但不写死避免边界浮点问题）
    assert out.min() >= 0.0
    assert out.max() <= 1.0

    assert out["a"] > out["b"] > out["c"] > out["d"]


def test_magnitude_normalize_ascending_false_inverts_preference():
    s = pd.Series([0.1, -0.2, 0.0], index=["x", "y", "z"])
    out = magnitude_normalize(
        s,
        ascending=False,
        activation="tanh",
        clip_quantile=None,
        z_clip=6.0,
    )
    assert out["y"] > out["z"] > out["x"]


def test_magnitude_normalize_invalid_activation_raises():
    s = pd.Series([1.0, 2.0, 3.0])
    with pytest.raises(ValueError):
        magnitude_normalize(s, activation="relu")


def test_magnitude_normalize_invalid_clip_quantile_raises():
    s = pd.Series([1.0, 2.0, 3.0])
    with pytest.raises(ValueError):
        magnitude_normalize(s, clip_quantile=0.5)


def test_magnitude_normalize_invalid_z_clip_raises():
    s = pd.Series([1.0, 2.0, 3.0])
    with pytest.raises(ValueError):
        magnitude_normalize(s, z_clip=0.0)


def test_magnitude_normalize_preserves_nans():
    s = pd.Series([1.0, np.nan, 3.0], index=["a", "b", "c"])
    out = magnitude_normalize(s, activation="tanh", clip_quantile=None, z_clip=6.0)
    assert np.isnan(out["b"])
    assert not np.isnan(out["a"])
    assert not np.isnan(out["c"])
