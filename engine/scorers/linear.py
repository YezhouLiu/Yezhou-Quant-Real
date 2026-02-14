from __future__ import annotations

from dataclasses import dataclass
import pandas as pd
import numpy as np

from engine.scorers.base import ScoreResult


@dataclass(frozen=True)
class LinearTerm:
    col: str
    weight: float


@dataclass(frozen=True)
class LinearScorer:
    """
    线性打分器：
      score = sum_i (w_i * signals[col_i]) + bias

    可选 post_transform:
      - None: 原样输出
      - "tanh": tanh(score)
      - "sigmoid": sigmoid(score)
      - "rank": 横截面 rank 到 (-1,1)
    """

    terms: tuple[LinearTerm, ...]
    out_col: str = "_score"
    bias: float = 0.0
    post_transform: str | None = None

    def score(self, signals: pd.DataFrame) -> ScoreResult:
        if "instrument_id" not in signals.columns:
            raise KeyError("signals missing required column: instrument_id")

        if len(self.terms) == 0:
            raise ValueError("terms cannot be empty")

        df = signals.copy()

        for t in self.terms:
            if t.col not in df.columns:
                raise KeyError(f"signals missing term column: {t.col}")

        s = pd.Series(self.bias, index=df.index, dtype="float64")
        for t in self.terms:
            s = s + t.weight * df[t.col].astype("float64")

        if self.post_transform is None:
            pass
        elif self.post_transform == "tanh":
            s = np.tanh(s)
        elif self.post_transform == "sigmoid":
            s = 1.0 / (1.0 + np.exp(-s))
        elif self.post_transform == "rank":
            pct = s.rank(pct=True, ascending=True)
            s = 2.0 * pct - 1.0
        else:
            raise ValueError(f"unknown post_transform: {self.post_transform}")

        df[self.out_col] = s
        return ScoreResult(signals=df, score_col=self.out_col)
