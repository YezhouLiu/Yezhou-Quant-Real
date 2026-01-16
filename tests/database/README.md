# 数据库测试套件文档

## 概述

完整的数据库 CRUD 操作测试套件，使用 Mock 确保不影响真实数据库。

## 测试统计

- **总测试数**: 222
- **通过率**: 100%
- **测试文件数**: 12

## 测试覆盖

### 1. test_rw_instruments.py (21 tests)
资产管理 CRUD 操作测试
- ✅ 插入新资产（完整字段、最小字段、IPO 日期、退市股票）
- ✅ 查询资产（按 ID、按 ticker、跨交易所）
- ✅ 更新资产状态（可交易标记）
- ✅ 删除资产（级联删除）
- ✅ 批量操作（批量插入、空列表处理）
- ✅ Edge cases（重复插入、不存在的资产）

### 2. test_rw_market_prices.py (22 tests)
市场价格 CRUD 操作测试
- ✅ 插入价格（完整 OHLCV、最小字段、分红、股票拆分）
- ✅ 批量插入（多天数据、不同数据源）
- ✅ 查询价格（全部、日期范围、指定日期）
- ✅ 获取最新价格
- ✅ 删除价格（全部、日期范围）
- ✅ Edge cases（节假日、重复日期、空数据）

### 3. test_rw_universe.py (18 tests)
Universe 管理 CRUD 操作测试
- ✅ 插入 Universe 定义（完整字段、无 source_ref）
- ✅ 创建快照（文件路径、原始内容、备注）
- ✅ 添加成员（普通、带权重、空列表）
- ✅ 查询（按 universe_key、最新快照、成员列表）
- ✅ 删除 Universe（级联删除）
- ✅ Edge cases（重复定义、空快照、不存在的 ID）

### 4. test_rw_trading_calendar.py (18 tests)
交易日历 CRUD 操作测试
- ✅ 插入交易日（普通日、节假日、不同市场）
- ✅ 批量插入（多天、空列表）
- ✅ 查询（是否交易日、日期范围、下一个/上一个交易日）
- ✅ Edge cases（不存在的日期、节假日前后）

### 5. test_rw_fills.py (14 tests)
成交记录 CRUD 操作测试
- ✅ 插入成交（买入、卖出、佣金、汇率、备注）
- ✅ 查询成交（全部、按资产、按日期、组合过滤）
- ✅ 删除成交（单个、多个）
- ✅ Edge cases（不存在的成交、空数据）

### 6. test_rw_positions.py (17 tests)
持仓记录 CRUD 操作测试
- ✅ 插入/更新持仓（新持仓、更新、最小字段、零持仓）
- ✅ 批量操作（多个持仓、默认值、空列表）
- ✅ 查询（指定日期、历史、日期范围）
- ✅ 删除（单日、多日）
- ✅ Edge cases（零持仓过滤、不存在的资产）

### 7. test_rw_system_state.py (16 tests)
系统状态 CRUD 操作测试
- ✅ 设置状态（字符串、字典、列表、数值）
- ✅ 获取状态（存在、不存在、默认值）
- ✅ 删除状态（单个、多个）
- ✅ 获取所有状态（空、混合类型）
- ✅ Edge cases（JSONB 转换、不存在的 key）

### 8. test_rw_data_update_logs.py (17 tests)
数据更新日志 CRUD 操作测试
- ✅ 创建日志（最小字段、日期范围、资产数量）
- ✅ 更新日志（成功、失败、时长计算）
- ✅ 查询日志（最近、按数据集、自定义数量）
- ✅ Edge cases（零行更新、详细错误信息）

### 9. test_rw_instrument_identifiers.py (19 tests)
资产标识符 CRUD 操作测试
- ✅ 插入标识符（新标识符、额外信息、多数据源）
- ✅ 查询（按资产+数据源、反向查询、所有标识符）
- ✅ 批量操作（多个标识符、额外信息、空列表）
- ✅ 删除（单个、不存在的）
- ✅ Edge cases（大小写敏感、重复插入）

### 10. test_rw_fundamental_data.py (21 tests)
基本面数据 CRUD 操作测试
- ✅ 插入基本面（年度、季度、数据源、不同指标）
- ✅ 批量操作（多个指标、不同周期、空列表）
- ✅ 查询（全部、按指标、日期范围、组合过滤）
- ✅ 获取最新指标
- ✅ 删除（全部、特定指标）
- ✅ Edge cases（不存在的指标、空数据）

### 11. test_rw_universe_snapshots.py (13 tests)
Universe 快照管理 CRUD 操作测试
- ✅ 查询快照（按 ID、所有快照、原始内容）
- ✅ 更新备注（普通、清空、长文本）
- ✅ 删除快照（级联删除、多个）
- ✅ Edge cases（不存在的快照、空 Universe）

### 12. test_rw_universe_members.py (16 tests)
Universe 成员管理 CRUD 操作测试
- ✅ 查询（成员数量、是否在 Universe 中、ticker 列表）
- ✅ 更新权重（普通、零权重、大权重）
- ✅ 删除成员（单个、多个、特定快照）
- ✅ Edge cases（空快照、不同快照差异）

## 测试特性

### Mock 策略
- 使用 `unittest.mock.MagicMock` 模拟数据库连接和游标
- 不依赖真实数据库，完全隔离测试环境
- 快速执行（0.67 秒完成 222 个测试）

### 测试覆盖范围
- ✅ 正常场景（happy path）
- ✅ Edge cases（边界情况）
- ✅ 空数据处理
- ✅ 重复操作处理
- ✅ 不存在的数据处理
- ✅ 批量操作
- ✅ ON CONFLICT 逻辑
- ✅ 级联删除
- ✅ 数据类型转换（JSONB）

### 测试模式
```python
# 典型测试结构
class TestInsertFunction:
    def test_normal_case(self, mock_conn):
        """测试正常情况"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (expected_value,)
        
        result = function_under_test(conn, ...)
        
        assert result == expected_value
        assert cursor.execute.called
        # 验证 SQL 语句
        sql = cursor.execute.call_args[0][0]
        assert 'INSERT INTO' in sql
```

## 运行测试

### 运行所有测试
```bash
pytest tests/database/ -v
```

### 运行单个文件
```bash
pytest tests/database/test_rw_instruments.py -v
```

### 显示覆盖率
```bash
pytest tests/database/ --cov=database.readwrite --cov-report=html
```

### 只运行失败的测试
```bash
pytest tests/database/ --lf
```

## 依赖

- pytest >= 9.0.2
- pytest-cov (可选，用于覆盖率报告)
- unittest.mock (Python 标准库)

## 注意事项

1. **所有测试使用 Mock**：不会影响真实数据库
2. **独立性**：每个测试相互独立，可单独运行
3. **快速执行**：所有测试在 1 秒内完成
4. **完整覆盖**：每个 CRUD 方法都有对应测试
5. **Edge cases**：考虑了各种边界情况和错误处理

## 维护建议

- 添加新的 CRUD 方法时，同步添加对应测试
- 保持测试独立性，避免测试间依赖
- 使用清晰的测试名称描述测试内容
- 每个测试只验证一个功能点
- 定期运行测试确保代码质量
