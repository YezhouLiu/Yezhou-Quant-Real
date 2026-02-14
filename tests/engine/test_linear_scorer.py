import pytest
import pandas as pd

from engine.scorers.linear import LinearScorer, LinearTerm


class TestLinearScorer:

    def test_linear_score_basic(self):
        df = pd.DataFrame(
            {
                "instrument_id": [1, 2, 3],
                "mom_rank": [1.0, 0.0, -1.0],
                "vol_rank": [-1.0, 0.0, 1.0],
            }
        )

        scorer = LinearScorer(
            terms=(LinearTerm("mom_rank", 0.6), LinearTerm("vol_rank", -0.4)),
            out_col="_score",
            bias=0.0,
            post_transform=None,
        )

        res = scorer.score(df)

        assert res.score_col == "_score"
        assert "_score" in res.signals.columns

        # score = 0.6*mom_rank -0.4*vol_rank
        s = res.signals["_score"].tolist()
        assert s[0] == pytest.approx(0.6 * 1.0 - 0.4 * (-1.0))
        assert s[1] == pytest.approx(0.0)
        assert s[2] == pytest.approx(0.6 * (-1.0) - 0.4 * (1.0))

    def test_missing_instrument_id_raises(self):
        df = pd.DataFrame({"mom_rank": [1.0]})
        scorer = LinearScorer(terms=(LinearTerm("mom_rank", 1.0),))
        with pytest.raises(KeyError):
            scorer.score(df)

    def test_missing_term_col_raises(self):
        df = pd.DataFrame({"instrument_id": [1], "mom_rank": [1.0]})
        scorer = LinearScorer(terms=(LinearTerm("nope", 1.0),))
        with pytest.raises(KeyError):
            scorer.score(df)

    def test_post_transform_rank(self):
        df = pd.DataFrame(
            {
                "instrument_id": [1, 2, 3],
                "x": [10.0, 20.0, 30.0],
            }
        )
        scorer = LinearScorer(
            terms=(LinearTerm("x", 1.0),), post_transform="rank", out_col="_score"
        )
        res = scorer.score(df)
        s = res.signals["_score"].tolist()
        # rank pct: [1/3,2/3,1] -> map to (-1,1): [-1/3, 1/3, 1]
        assert s[0] == pytest.approx(-1 / 3)
        assert s[1] == pytest.approx(1 / 3)
        assert s[2] == pytest.approx(1.0)
