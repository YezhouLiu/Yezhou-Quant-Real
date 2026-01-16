import pandas as pd
from datetime import date
from utils.config_loader import get_config_value, get_config_value_as_date


# ----------------------------------------------------------------------------------------------------------------------------------------
# 获取配置: backtest相关默认值
# ----------------------------------------------------------------------------------------------------------------------------------------
def DEFAULT_CAPITAL():
    return get_config_value("backtest.capital")


def DEFAULT_BACKTEST_START_DATE() -> date:
    return get_config_value_as_date("backtest.default_backtest_start_date")


def DEFAULT_BACKTEST_END_DATE() -> date:
    return get_config_value_as_date("backtest.default_backtest_end_date")


# ----------------------------------------------------------------------------------------------------------------------------------------
# 获取配置: exchange相关默认值
# ----------------------------------------------------------------------------------------------------------------------------------------
def DEFAULT_SLIPPAGE():
    return get_config_value("exchange.slippage")


def DEFAULT_TRANSACTION_COST():
    return get_config_value("exchange.transaction_cost")


def DEFAULT_EXCHANGE_COST():
    return get_config_value("exchange.exchange_cost")


def DEFAULT_MIN_DIFF_BUY_SELL_RATIO():
    return get_config_value("exchange.min_diff_buy_sell_ratio", 0.02)


def DEFAULT_REBALANCE_TOTAL_VALUE_REINVEST_RATIO():
    return get_config_value("exchange.rebalance_total_value_reinvest_ratio", 0.98)


# ----------------------------------------------------------------------------------------------------------------------------------------
# 获取配置: data相关默认值
# ----------------------------------------------------------------------------------------------------------------------------------------
def DEFAULT_START_DATE() -> date:
    return get_config_value_as_date("data.default_start_date")


def DEFAULT_END_DATE() -> date:
    return get_config_value_as_date("data.default_end_date")


# ----------------------------------------------------------------------------------------------------------------------------------------
# 获取配置: price相关默认值
# ----------------------------------------------------------------------------------------------------------------------------------------
def DEFAULT_PRICE_FLOOR() -> float:
    return get_config_value("price.price_floor", 1.5)


def DEFAULT_PRICE_CEILING() -> float:
    return get_config_value("price.price_ceiling", 10000.0)


def DEFAULT_JUMP_THRESHOLD() -> float:
    return get_config_value("price.jump_threshold", 0.95)


def DEFAULT_JUMP_RATIO_LIMIT() -> float:
    return get_config_value("price.jump_ratio_limit", 10.0)
