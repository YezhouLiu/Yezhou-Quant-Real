from __future__ import annotations

from dataclasses import dataclass
import pandas as pd

from engine.selectors.base import SelectionResult


@dataclass(frozen=True)
class TopKSelector:
    """
    - 可选：先按若干条件过滤
    - 再按某个列排序取 TopK

    用法示例：
      selector = TopKSelector(
          k=20,
          sort_by="momentum_60d_rank",
          sort_ascending=False,
          filters=[
              ("momentum_60d_mag", ">", 0.0),
              ("volatility_20d_rank", ">", 0.0),
          ],
      )
    """

    k: int
    sort_by: str
    sort_ascending: bool = False
    filters: tuple[tuple[str, str, float], ...] = ()

    def select(self, signals: pd.DataFrame) -> SelectionResult:
        if self.k <= 0:
            raise ValueError("k must be > 0")

        if "instrument_id" not in signals.columns:
            raise KeyError("signals missing required column: instrument_id")

        if self.sort_by not in signals.columns:
            raise KeyError(f"signals missing sort_by column: {self.sort_by}")

        df = signals

        for col, op, thr in self.filters:
            if col not in df.columns:
                raise KeyError(f"signals missing filter column: {col}")

            if op == ">":
                df = df[df[col] > thr]
            elif op == ">=":
                df = df[df[col] >= thr]
            elif op == "<":
                df = df[df[col] < thr]
            elif op == "<=":
                df = df[df[col] <= thr]
            elif op == "==":
                df = df[df[col] == thr]
            else:
                raise ValueError(f"unknown operator: {op}")

        selected = (
            df.sort_values(self.sort_by, ascending=self.sort_ascending)
            .head(self.k)
            .copy()
        )

        return SelectionResult(selected=selected, ranking_col=self.sort_by)
