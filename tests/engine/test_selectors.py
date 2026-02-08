import pandas as pd
import pytest

from engine.selectors.topk import TopKSelector
from engine.selectors.rules import Rule, RuleSelector


def _sample_signals() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "instrument_id": [1, 2, 3, 4, 5],
            "mom_rank": [0.9, 0.7, 0.1, -0.2, 0.5],
            "mom_mag": [0.8, 0.2, -0.1, -0.7, 0.1],
            "vol_rank": [0.6, 0.2, 0.9, 0.1, 0.4],
        }
    )


def test_topk_selector_basic():
    df = _sample_signals()
    sel = TopKSelector(k=2, sort_by="mom_rank", sort_ascending=False)
    res = sel.select(df)

    assert res.selected.shape[0] == 2
    assert res.ranking_col == "mom_rank"
    # top2 should be instrument_id 1 (0.9) and 2 (0.7)
    assert res.selected["instrument_id"].tolist() == [1, 2]


def test_topk_selector_with_filter_blocks_negative_mag():
    df = _sample_signals()
    sel = TopKSelector(
        k=3,
        sort_by="mom_rank",
        sort_ascending=False,
        filters=(("mom_mag", ">", 0.0),),
    )
    res = sel.select(df)

    # ids with mom_mag > 0: 1,2,5
    assert res.selected["instrument_id"].tolist() == [1, 2, 5]


def test_topk_selector_missing_column_raises():
    df = _sample_signals().drop(columns=["mom_rank"])
    sel = TopKSelector(k=2, sort_by="mom_rank")
    with pytest.raises(KeyError):
        sel.select(df)


def test_rule_selector_filters_then_ranks():
    df = _sample_signals()
    sel = RuleSelector(
        k=2,
        rules=(Rule("mom_mag", ">", 0.0), Rule("vol_rank", ">", 0.1)),
        rank_cols=("mom_rank", "vol_rank"),
        agg="mean",
        sort_ascending=False,
    )
    res = sel.select(df)

    # eligible: 1 (mom_mag 0.8, vol 0.6), 2 (0.2,0.2), 5 (0.1,0.4)
    # mean scores: id1=0.75, id2=0.45, id5=0.45 -> top2 should start with 1, then tie among 2/5
    assert res.selected.iloc[0]["instrument_id"] == 1
    assert res.selected.shape[0] == 2
    assert res.ranking_col == "_selector_score"


def test_rule_selector_missing_rank_cols_raises():
    df = _sample_signals().drop(columns=["vol_rank"])
    sel = RuleSelector(
        k=2,
        rules=(Rule("mom_mag", ">", 0.0),),
        rank_cols=("mom_rank", "vol_rank"),
    )
    with pytest.raises(KeyError):
        sel.select(df)
