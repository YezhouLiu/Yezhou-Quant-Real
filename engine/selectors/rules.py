from __future__ import annotations

from dataclasses import dataclass
import pandas as pd

from engine.selectors.base import SelectionResult


@dataclass(frozen=True)
class Rule:
    col: str
    op: str
    thr: float


@dataclass(frozen=True)
class RuleSelector:
    """
    Rule-based selector:
    - 先按 rules 过滤
    - 再按 rank_cols 做一个简单聚合分（默认均值）排序取 TopK

    “聚合”只是为了在多个 rank 列之间做一个一致的 tie-break / 排序。
    """

    k: int
    rules: tuple[Rule, ...]
    rank_cols: tuple[str, ...]  # 用于排序的列（建议用 *_rank）
    agg: str = "mean"  # "mean" 或 "sum"
    sort_ascending: bool = False

    def select(self, signals: pd.DataFrame) -> SelectionResult:
        if self.k <= 0:
            raise ValueError("k must be > 0")

        if "instrument_id" not in signals.columns:
            raise KeyError("signals missing required column: instrument_id")

        df = signals

        for r in self.rules:
            if r.col not in df.columns:
                raise KeyError(f"signals missing rule column: {r.col}")

            if r.op == ">":
                df = df[df[r.col] > r.thr]
            elif r.op == ">=":
                df = df[df[r.col] >= r.thr]
            elif r.op == "<":
                df = df[df[r.col] < r.thr]
            elif r.op == "<=":
                df = df[df[r.col] <= r.thr]
            elif r.op == "==":
                df = df[df[r.col] == r.thr]
            else:
                raise ValueError(f"unknown operator: {r.op}")

        if len(self.rank_cols) == 0:
            raise ValueError("rank_cols cannot be empty")

        for c in self.rank_cols:
            if c not in df.columns:
                raise KeyError(f"signals missing rank_cols column: {c}")

        # 简单聚合分：用于排序（不写回 DB，只在这一步产生）
        score_col = "_selector_score"
        if self.agg == "mean":
            df = df.assign(**{score_col: df[list(self.rank_cols)].mean(axis=1)})
        elif self.agg == "sum":
            df = df.assign(**{score_col: df[list(self.rank_cols)].sum(axis=1)})
        else:
            raise ValueError(f"unknown agg: {self.agg}")

        selected = (
            df.sort_values(score_col, ascending=self.sort_ascending).head(self.k).copy()
        )
        return SelectionResult(selected=selected, ranking_col=score_col)
