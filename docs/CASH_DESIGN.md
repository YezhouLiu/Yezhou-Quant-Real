# Cash 持仓设计说明

## 💡 **设计原则**

在回测系统中，**现金 (cash) 不是真实的 instrument**，因此不应该出现在 `instruments` 表中。

为了解决这个问题，我们使用了 **特殊占位符 ID** 的方案。

---

## 🎯 **实现方案**

### **1. 定义常量**

在 [`engine/constants.py`](engine/constants.py) 中：

```python
# Cash 不是真实的 instrument，用 ID=0 表示
# 不会存在于 instruments 表中
CASH_INSTRUMENT_ID = 0
```

### **2. 数据库表结构**

[`exp_positions`](database/schema/tables/exp_positions.py) 表：
- **移除外键约束** - 允许 `instrument_id = 0` 存在
- `instrument_id = 0` 专门表示现金
- 其他 ID (>0) 表示真实的股票/ETF

```sql
CREATE TABLE exp_positions (
    date DATE NOT NULL,
    instrument_id BIGINT NOT NULL,  -- 0 = cash, >0 = 股票
    quantity NUMERIC(20,8) NOT NULL,
    buy_price NUMERIC(20,6),
    current_price NUMERIC(20,6),
    market_value NUMERIC(20,6) NOT NULL,
    PRIMARY KEY (date, instrument_id)
);
```

### **3. 代码使用**

#### **Portfolio 自动使用常量**

[`Portfolio.snapshot()`](engine/portfolio.py#L106) 自动添加现金行：

```python
from engine.constants import CASH_INSTRUMENT_ID

def snapshot(self, date: str, prices: Dict[int, float]) -> pd.DataFrame:
    rows = []
    
    # 股票持仓
    for inst_id, pos in self.positions.items():
        rows.append({...})
    
    # 现金持仓（自动使用 CASH_INSTRUMENT_ID）
    rows.append({
        "date": date,
        "instrument_id": CASH_INSTRUMENT_ID,  # = 0
        "quantity": self.cash,
        "buy_price": 1.0,
        "current_price": 1.0,
        "market_value": self.cash,
    })
    
    return pd.DataFrame(rows)
```

#### **BacktestRunner 无需配置**

之前：
```python
runner = BacktestRunner(
    initial_cash=100000,
    cash_instrument_id=999,  # ❌ 需要手动配置
    ...
)
```

现在：
```python
runner = BacktestRunner(
    initial_cash=100000,
    # ✅ cash 自动使用 CASH_INSTRUMENT_ID (0)
    ...
)
```

---

## 📊 **查询持仓**

### **所有持仓（包括现金）**

```python
from database.readwrite.rw_exp_positions import get_exp_positions

df = get_exp_positions(conn, date="2023-12-29")
# instrument_id = 0 是现金
# instrument_id > 0 是股票
```

### **仅查询现金**

```python
from database.readwrite.rw_exp_positions import get_cash_only

df_cash = get_cash_only(conn, date="2023-12-29")
print(f"现金余额: ${df_cash['market_value'].iloc[0]:,.2f}")
```

### **仅查询股票（排除现金）**

```python
from database.readwrite.rw_exp_positions import get_stock_positions_only

df_stocks = get_stock_positions_only(conn, date="2023-12-29")
# 自动排除 instrument_id = 0
```

### **NAV 曲线**

```python
from database.readwrite.rw_exp_positions import get_exp_nav

df_nav = get_exp_nav(conn, start_date="2023-01-01", end_date="2023-12-31")
# 按日聚合所有持仓（包括现金）的市值
```

---

## 🔧 **完整示例**

运行 [`examples/query_cash_positions.py`](examples/query_cash_positions.py) 查看完整用法。

---

## ⚠️ **注意事项**

1. **ID=0 专用于现金**
   - 永远不要在 `instruments` 表中插入 `instrument_id = 0` 的记录
   - 代码中直接使用 `CASH_INSTRUMENT_ID` 常量

2. **外键约束已移除**
   - `exp_positions` 表不再强制要求所有 `instrument_id` 存在于 `instruments` 表
   - 这是有意的设计，允许 cash (id=0) 的存在

3. **向后兼容性**
   - 如果你之前使用了其他 cash ID (如 999)，需要：
     1. 重建 `exp_positions` 表
     2. 或者手动迁移旧数据，将 cash 的 ID 改为 0

4. **多币种扩展**
   - 如果未来需要支持多币种，可以使用：
     - `CASH_USD = 0`
     - `CASH_CNY = -1`
     - `CASH_EUR = -2`
   - 负数 ID 全部保留给 cash 类资产

---

## ✅ **优势**

| 方案 | 优点 | 缺点 |
|-----|------|------|
| ❌ 在 `instruments` 表创建假记录 | 符合外键约束 | 污染数据，语义不清 |
| ✅ **使用 ID=0 + 移除外键** | 清晰、简洁、零配置 | 失去外键保护 |

我们选择 **语义清晰** 优先，因为：
- Cash 本质上不是 instrument
- ID=0 是显然的占位符
- 代码更简洁，无需传参

---

## 📝 **升级指南**

如果你的系统已有旧数据，迁移步骤：

```sql
-- 1. 删除旧表（如果需要）
DROP TABLE exp_positions CASCADE;

-- 2. 重新创建表（无外键约束）
-- 运行 python -c "from database.schema.create_tables import create_all_tables; create_all_tables()"

-- 3. 如果有历史数据需要迁移
UPDATE exp_positions 
SET instrument_id = 0 
WHERE instrument_id = 999;  -- 假设之前用 999 表示 cash
```
