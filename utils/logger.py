import logging
import os
from logging.handlers import RotatingFileHandler
from utils.config_loader import get_config_value


# ----------------------------------------------------------------------------------------------------------------------------------------
# 获取日志记录器
# ----------------------------------------------------------------------------------------------------------------------------------------
def get_logger(name: str, level=logging.INFO) -> logging.Logger:
    log_dir = get_config_value("log.log_dir", "logs")  # 获取日志目录配置
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(name)
    if logger.handlers:  # 避免重复添加 handler
        return logger

    log_path = os.path.join(log_dir, f"{name}.log")

    handler = RotatingFileHandler(
        log_path,
        maxBytes=5 * 1024 * 1024,  # 5MB
        backupCount=5,  # 最多保留5个旧文件
        encoding="utf-8",
    )

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)

    return logger
