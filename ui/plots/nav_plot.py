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
import matplotlib.pyplot as plt
from typing import Dict
import pandas as pd


def plot_nav(series: Dict[str, pd.DataFrame], title="NAV Comparison"):
    plt.figure(figsize=(12, 6))

    for label, df in series.items():
        plt.plot(df.index, df["nav"], label=label)

    plt.title(title)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()
