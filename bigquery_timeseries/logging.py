from loguru import logger
import sys

def get_logger(name):
    return logger.bind(name=name)

def set_log_level(level):
    logger.remove()
    logger.add(sys.stderr, level=level)

# 初期設定
set_log_level("INFO")