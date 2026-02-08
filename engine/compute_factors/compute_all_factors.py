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
