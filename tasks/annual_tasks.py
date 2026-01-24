from datetime import date
from data_download.input.build_trading_calendar import build_trading_calendar


def annual_update():
    # 每年更新交易日历（未来三年）
    build_trading_calendar(start_date=date(2004, 12, 30), horizon_days=365 * 3,)