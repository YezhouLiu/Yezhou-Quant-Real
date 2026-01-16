import pandas as pd
from datetime import date, datetime
from typing import Union

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
