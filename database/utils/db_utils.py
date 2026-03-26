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
import psycopg
from utils.config_loader import load_config


# ----------------------------------------------------------------------------------------------------------------------------------------
# 获取数据库连接
# 当使用with语法进行调用
# with get_db_connection() as conn:
#   with conn.cursor() as cursor:
#       cursor.execute("SELECT * FROM table_name")
# 连接会在with块结束时自动关闭，也会自动提交事务
# 如果之前需要手动提交或回滚事务，可以使用 conn.commit() 或 conn.rollback()
# ----------------------------------------------------------------------------------------------------------------------------------------
def get_db_connection():
    cfg = load_config()
    db = cfg["database"]
    return psycopg.connect(
        dbname=db["dbname"],
        user=db["user"],
        password=db["password"],
        host=db["host"],
        port=db["port"],
    )
