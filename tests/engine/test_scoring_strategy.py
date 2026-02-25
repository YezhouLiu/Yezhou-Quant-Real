import pytest
import pandas as pd
from unittest.mock import MagicMock

from engine.scorers.linear import LinearScorer, LinearTerm
from engine.signals import FactorSpec
from engine.strategies.scoring_strategy import ScoringStrategy


def test_scoring_strategy_calls_pipeline(monkeypatch):
    fake_signals = pd.DataFrame(
        {
            "instrument_id": [1, 2],
            "mom_rank": [1.0, -1.0],
            "vol_rank": [0.5, 0.5],
        }
    )

    def fake_build_signals_for_date(*args, **kwargs):
        return fake_signals

    # patch 在 scoring_strategy 模块里引用到的函数名
    monkeypatch.setattr(
        "engine.strategies.scoring_strategy.build_signals_for_date",
        fake_build_signals_for_date,
    )

    scorer = LinearScorer(
        terms=(LinearTerm("mom_rank", 1.0), LinearTerm("vol_rank", -1.0)),
        out_col="_score",
    )

    strat = ScoringStrategy(
        factor_specs=(FactorSpec("mom", True),),
        scorer=scorer,
        factor_version="v1",
    )

    conn = MagicMock()

    res = strat.score_for_date(conn, asof_date="2024-01-31", universe_ids=[1, 2])

    assert "_score" in res.signals.columns
    assert res.score_col == "_score"
    assert len(res.signals) == 2


def test_scoring_strategy_empty_specs_raises():
    scorer = LinearScorer(terms=(LinearTerm("x", 1.0),))
    strat = ScoringStrategy(factor_specs=(), scorer=scorer)
    conn = MagicMock()

    with pytest.raises(ValueError):
        strat.score_for_date(conn, asof_date="2024-01-31")
