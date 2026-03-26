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


def create_system_state_table(conn, if_exists='skip'):
    """创建系统状态表"""
    
    if if_exists == 'drop':
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS system_state CASCADE;")
        log.info("[✔] 已删除旧表 system_state")
    
    statement = """
        CREATE TABLE IF NOT EXISTS system_state (
            key TEXT PRIMARY KEY,
            value JSONB,
            updated_at TIMESTAMPTZ DEFAULT now()
        );
        
        COMMENT ON TABLE system_state IS '系统状态/配置（cash_instrument_id, last_price_update...）';
    """
    
    cursor = conn.cursor()
    cursor.execute(statement)
    log.info("[✔] 表 'system_state' 创建成功")


def create_system_state_indexes(conn):
    pass
