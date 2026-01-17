import pandas as pd
from datetime import date, datetime, timedelta
from typing import Union
from zoneinfo import ZoneInfo


DateLike = Union[str, date, datetime, pd.Timestamp]


# ----------------------------------------------------------------------------------------------------------------------------------------
# 获取当前日期
# ----------------------------------------------------------------------------------------------------------------------------------------
def DATE_TODAY() -> date:
    return pd.Timestamp.now().date()


# ----------------------------------------------------------------------------------------------------------------------------------------
# 获取当前时间戳
# ----------------------------------------------------------------------------------------------------------------------------------------
def TIMESTAMP_TODAY() -> pd.Timestamp:
    return pd.Timestamp.now()


# ----------------------------------------------------------------------------------------------------------------------------------------
# 将任意类型的时间转换为 datetime.date
# ----------------------------------------------------------------------------------------------------------------------------------------
def to_date(dt: DateLike) -> date:
    """
    将任意类型的时间（str, datetime, Timestamp）转换为 datetime.date。
    """
    if isinstance(dt, date) and not isinstance(dt, datetime):
        return dt
    elif isinstance(dt, (datetime, pd.Timestamp)):
        return dt.date()
    elif isinstance(dt, str):  # type: ignore # 底层是dateutil.parser.parse()，支持广泛的格式
        return pd.to_datetime(dt).date()
    else:
        raise TypeError(f"Unsupported type for to_date(): {type(dt)}")


# ----------------------------------------------------------------------------------------------------------------------------------------
# 将任意类型的时间转换为 pd.Timestamp
# ----------------------------------------------------------------------------------------------------------------------------------------
def to_timestamp(dt: DateLike) -> pd.Timestamp:
    """
    将任意类型的时间（str, date, datetime）转换为 pd.Timestamp。
    """
    if isinstance(dt, pd.Timestamp):
        return dt
    elif isinstance(dt, (datetime, date)):
        return pd.Timestamp(dt)
    elif isinstance(dt, str):  # type: ignore
        return pd.to_datetime(dt)
    else:
        raise TypeError(f"Unsupported type for to_timestamp(): {type(dt)}")


def latest_us_market_date() -> date:
    """
    以 America/New_York 为准，返回“最可能已经可用”的 EOD 日期。

    经验规则（务实版）：
    - 如果纽约时间还没到当日收盘后（比如 18:00 之前），就认为当天 EOD 可能还不稳，
      end_date 取前一日，避免请求到“今天但其实还没数据”的空结果。
    - 18:00 这个阈值你可按 Tiingo 实际出数时间再微调。
    """
    ny_now = datetime.now(ZoneInfo("America/New_York"))
    ny_today = ny_now.date()

    # 纽约当天还没“足够晚”，保守取昨天
    if ny_now.hour < 18:
        return ny_today - timedelta(days=1)
    return ny_today