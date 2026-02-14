from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence
import pandas as pd
import psycopg

from engine.scorers.base import Scorer, ScoreResult
from engine.signals import FactorSpec, build_signals_for_date


@dataclass(frozen=True)
class ScoringStrategy:
    """
    Strategy：只负责产出 score
      DB -> signals -> scorer -> score

    不负责：
      - 选股（selector）
      - 回测（rebalance/持仓/NAV）
    """

    factor_specs: tuple[FactorSpec, ...]
    scorer: Scorer
    factor_version: str | None = None
    table: str = "factor_values"

    def score_for_date(
        self,
        conn: psycopg.Connection,
        *,
        asof_date: str,
        universe_ids: Sequence[int] | None = None,
    ) -> ScoreResult:
        if len(self.factor_specs) == 0:
            raise ValueError("factor_specs cannot be empty")

        signals = build_signals_for_date(
            conn,
            asof_date=asof_date,
            specs=self.factor_specs,
            factor_version=self.factor_version,
            universe_ids=universe_ids,
            table=self.table,
        )

        return self.scorer.score(signals)

    def signals_for_date(
        self,
        conn: psycopg.Connection,
        *,
        asof_date: str,
        universe_ids: Sequence[int] | None = None,
    ) -> pd.DataFrame:
        """
        需要 debug 时用：只拿 signals，不打分
        """
        if len(self.factor_specs) == 0:
            raise ValueError("factor_specs cannot be empty")

        return build_signals_for_date(
            conn,
            asof_date=asof_date,
            specs=self.factor_specs,
            factor_version=self.factor_version,
            universe_ids=universe_ids,
            table=self.table,
        )
