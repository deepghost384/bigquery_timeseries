# bigquery_timeseries/logger.py

import logging
import sys

def get_logger(name="bigquery_timeseries", level=logging.INFO):
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        logger.setLevel(level)
        
        # コンソールハンドラの設定
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        
        # フォーマッタの設定
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        
        # ハンドラをロガーに追加
        logger.addHandler(console_handler)
    
    return logger