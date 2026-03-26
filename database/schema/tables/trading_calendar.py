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
from utils.logger import get_logger

log = get_logger("database")


def create_trading_calendar_table(conn, if_exists='skip'):
    """创建交易日历表"""
    
    if if_exists == 'drop':
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS trading_calendar CASCADE;")
        log.info("[✔] 已删除旧表 trading_calendar")
    
    statement = """
        CREATE TABLE IF NOT EXISTS trading_calendar (
            market TEXT NOT NULL DEFAULT 'US',
            date DATE NOT NULL,
            is_trading_day BOOLEAN NOT NULL,
            holiday_name TEXT,
            
            PRIMARY KEY (market, date)
        );
        
        COMMENT ON TABLE trading_calendar IS '交易日历（历史单分支事实）';
    """
    
    cursor = conn.cursor()
    cursor.execute(statement)
    log.info("[✔] 表 'trading_calendar' 创建成功")


def create_trading_calendar_indexes(conn):
    """创建索引"""
    
    index_statements = [
        "CREATE INDEX IF NOT EXISTS idx_trading_calendar_date ON trading_calendar(date);",
    ]
    
    cursor = conn.cursor()
    for statement in index_statements:
        cursor.execute(statement)
