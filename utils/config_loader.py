from datetime import date
from typing import Any
import yaml
import os
from pathlib import Path
from dotenv import load_dotenv

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent

# 加载环境变量文件
load_dotenv(PROJECT_ROOT / "secrets.env")


# ----------------------------------------------------------------------------------------------------------------------------------------
# 配置加载器（支持环境变量覆盖）
# 优先级：环境变量 > config.yaml
# ----------------------------------------------------------------------------------------------------------------------------------------
def load_config(path: str = "config/config.yaml"):
    """
    加载配置文件，支持环境变量覆盖
    
    优先级：
    1. 环境变量（.env 文件或系统环境变量）
    2. config.yaml 文件
    
    这样即使 config.yaml 中有默认值，也会被环境变量覆盖
    """
    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    
    # 🔒 使用环境变量覆盖敏感配置
    # 数据库配置
    if "database" in config:
        config["database"]["host"] = os.getenv("DB_HOST", config["database"].get("host", "localhost"))
        config["database"]["port"] = int(os.getenv("DB_PORT", config["database"].get("port", 5432)))
        config["database"]["dbname"] = os.getenv("DB_NAME", config["database"].get("dbname", "quant"))
        config["database"]["user"] = os.getenv("DB_USER", config["database"].get("user", ""))
        config["database"]["password"] = os.getenv("DB_PASSWORD", config["database"].get("password", ""))
    
    # Tiingo API Key
    if "tiingo" not in config:
        config["tiingo"] = {}
    config["tiingo"]["api_key"] = os.getenv("TIINGO_API_KEY", config.get("tiingo", {}).get("api_key", ""))
    
    # FMP API Key
    if "fmp" not in config:
        config["fmp"] = {}
    config["fmp"]["api_key"] = os.getenv("FMP_API_KEY", config.get("fmp", {}).get("api_key", ""))
    
    return config


# ----------------------------------------------------------------------------------------------------------------------------------------
# 直接从环境变量获取敏感信息（推荐用于 API Key）
# ----------------------------------------------------------------------------------------------------------------------------------------
def get_secret(key: str, default: str = "") -> str:
    """
    从环境变量获取敏感信息
    
    Args:
        key: 环境变量名（如 'TIINGO_API_KEY'）
        default: 默认值
        
    Returns:
        环境变量的值，如果不存在则返回默认值
        
    Example:
        >>> api_key = get_secret('TIINGO_API_KEY')
        >>> db_password = get_secret('DB_PASSWORD')
    """
    return os.getenv(key, default)


# ----------------------------------------------------------------------------------------------------------------------------------------
# 获取配置值
# 如果需要嵌套访问，可以使用点号（.）分隔的字符串
# 例如：get_config_value(config, "database.host") 或 get_config
# ----------------------------------------------------------------------------------------------------------------------------------------
def get_config_value(
    key: str, default: Any = None, config_path: str = "config/config.yaml"
) -> Any:
    keys = key.split(".")
    value = load_config(config_path)
    try:
        for k in keys:
            value = value[k]
        return value
    except (KeyError, TypeError):
        return default


# ----------------------------------------------------------------------------------------------------------------------------------------
# 获取配置值并转换为日期类型
# ----------------------------------------------------------------------------------------------------------------------------------------
def get_config_value_as_date(
    key: str, default: str = "", config_path: str = "config/config.yaml"
) -> date:
    return date.fromisoformat(get_config_value(key, default, config_path))
