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
