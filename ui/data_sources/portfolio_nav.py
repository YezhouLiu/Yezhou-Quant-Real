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
import pandas as pd
from database.readwrite.rw_exp_positions import get_exp_nav
from database.utils.db_utils import get_db_connection


class PortfolioNAVSource:
    """
    数据来源：exp_positions 聚合 NAV
    """

    def __init__(self, start_date: str = None, end_date: str = None):
        self.start_date = start_date
        self.end_date = end_date

    def load(self) -> pd.DataFrame:
        with get_db_connection() as conn:
            df = get_exp_nav(conn, self.start_date, self.end_date)

        if df.empty:
            raise RuntimeError("exp_positions NAV is empty")

        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")

        first_nav = df["nav"].iloc[0]
        df["nav"] = df["nav"] / first_nav

        return df[["nav"]]
