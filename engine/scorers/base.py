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
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable, Tuple
import pandas as pd


@dataclass(frozen=True)
class ScoreResult:
    """
    打分结果：返回带 score 的 signals DF + score 列名
    """

    signals: pd.DataFrame
    score_col: str


@runtime_checkable
class Scorer(Protocol):
    """
    Scorer: 输入 signals DF，输出 ScoreResult
    """

    def score(self, signals: pd.DataFrame) -> ScoreResult: ...
