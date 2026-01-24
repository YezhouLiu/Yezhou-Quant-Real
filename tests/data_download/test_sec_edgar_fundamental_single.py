import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pytest
from unittest.mock import MagicMock, patch

from data_download.input.sec_edgar_fundamental_single import download_one_ticker_fundamental_data


@pytest.fixture
def mock_conn():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


def _mk_resp(json_data, status=200):
    r = MagicMock()
    r.raise_for_status.return_value = None
    r.json.return_value = json_data
    r.status_code = status
    return r


class TestSecEdgarFundamentalSingle:
    def test_download_one_ticker_filters_and_dedupes(self, mock_conn):
        conn, cursor = mock_conn

        # instruments lookup: ticker+exchange -> instrument_id
        cursor.fetchone.return_value = (123,)

        # SEC mapping response
        mapping_json = {
            "0": {"ticker": "AAPL", "cik_str": 320193, "title": "Apple Inc."}
        }

        # companyfacts response (只塞一个 tag 足够测试)
        # Revenues:
        # - FY 2016: keep
        # - Q3 cumulative 9m: drop (duration ~ 270)
        # - Q1 single quarter: keep
        # - Q1 amended later filed: should win
        companyfacts_json = {
            "entityName": "Apple Inc.",
            "facts": {
                "us-gaap": {
                    "Revenues": {
                        "units": {
                            "USD": [
                                # FY keep
                                {"fy": 2016, "fp": "FY", "form": "10-K", "filed": "2016-10-26",
                                 "start": "2015-09-28", "end": "2016-09-24", "val": 215639000000},

                                # Q3 cumulative 9 months drop
                                {"fy": 2016, "fp": "Q3", "form": "10-Q", "filed": "2016-07-27",
                                 "start": "2015-09-28", "end": "2016-06-25", "val": 100000000000},

                                # Q1 single quarter keep
                                {"fy": 2016, "fp": "Q1", "form": "10-Q", "filed": "2016-01-27",
                                 "start": "2015-09-28", "end": "2015-12-26", "val": 75000000000},

                                # Q1 amended later filed, same end/fp should override
                                {"fy": 2016, "fp": "Q1", "form": "10-Q/A", "filed": "2016-02-15",
                                 "start": "2015-09-28", "end": "2015-12-26", "val": 76000000000},
                            ]
                        }
                    }
                },
                "dei": {}
            }
        }

        def requests_get_side_effect(url, headers=None, timeout=None):
            if "company_tickers.json" in url:
                return _mk_resp(mapping_json)
            if "companyfacts" in url:
                return _mk_resp(companyfacts_json)
            raise AssertionError(f"unexpected url: {url}")

        with patch("data_download.input.sec_edgar_fundamental_single.requests.get", side_effect=requests_get_side_effect):
            n = download_one_ticker_fundamental_data(conn, ticker="AAPL", exchange="NASDAQ",
                                                     tags=[("us-gaap", "Revenues", "USD")])

        # 应该写入两条：FY + Q1（Q3累计被过滤；Q1被修订覆盖后只剩一条）
        assert n == 2

        assert cursor.executemany.called
        sql = cursor.executemany.call_args[0][0]
        assert "INSERT INTO fundamental_data" in sql

        rows = cursor.executemany.call_args[0][1]
        assert len(rows) == 2

        # 找到 Q1 那条（Quarterly）
        q_rows = [r for r in rows if r["period_type"] == "Quarterly"]
        assert len(q_rows) == 1
        assert q_rows[0]["report_date"] == "2015-12-26"
        assert q_rows[0]["metric_value"] == 76000000000.0  # amended wins
        assert q_rows[0]["metric_name"] == "us-gaap.Revenues"
        assert q_rows[0]["currency"] == "USD"
        assert q_rows[0]["data_source"] == "sec_edgar"

        # FY 那条（Annual）
        a_rows = [r for r in rows if r["period_type"] == "Annual"]
        assert len(a_rows) == 1
        assert a_rows[0]["report_date"] == "2016-09-24"
        assert a_rows[0]["metric_value"] == 215639000000.0
