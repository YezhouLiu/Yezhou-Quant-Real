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
from engine.compute_factors import (
    compute_dollar_volume,
    compute_jump_risk,
    compute_max_drawdown,
    compute_momentum,
    compute_volatility,
    compute_volatility_of_volatility,
)


def compute_all_factors():
    compute_momentum.run()
    compute_volatility.run()
    compute_dollar_volume.run()
    compute_volatility_of_volatility.run()
    compute_jump_risk.run()
    compute_max_drawdown.run()
