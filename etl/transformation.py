# etl/transformation.py
"""
ETL 模組 - Transform (資料清洗與商業邏輯處理)
"""
import os
import pandas as pd
import logging
from typing import Optional, Dict, Any

import const

from etl.processors.merchant import MerchantPipeline
from etl.processors.card_classifier import CardClassifier
from etl.processors.transaction_classifier import TransactionClassifier


try:
    from profiles.loaders.config_loader import ConfigLoader
except ImportError:
    ConfigLoader = None

from etl.utils import save_anomaly_report

logger = logging.getLogger(__name__)

CONFIG_DIR = const.CONFIG_DIR
OUTPUT_DIR = const.OUTPUT_DIR


class DataRefiner:
    def __init__(self, config_dir: str, configs: Optional[dict] = None):
        configs = configs or {}
        self.card_classifier = CardClassifier(config_dir, rules=configs.get('cards'), gateways=configs.get('gateways'))
        self.merchant_pipeline = MerchantPipeline(config_dir=config_dir, configs=configs)
        self.classifier = TransactionClassifier(config_dir, config=configs.get('txn_types'))

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return df

        if const.COL_LOCATION not in df.columns:
            df[const.COL_LOCATION] = 'TW'

        # 1. 卡片歸戶與支付分類 (Card & VPC Classifier)
        #    標記卡別、vpc_type 並完成第三方支付交叉流轉
        df = self.card_classifier.process(df)

        # 2. 商家名稱管線清洗 (Merchant Pipeline: Gateway -> EC -> Normalizer -> Fallback -> Display)
        df = self.merchant_pipeline.process(df)

        # 3. 交易分類 (Transaction Classification)
        #    根據 merchant_display / category 標記 transaction_type
        df = self.classifier.process(df)

        return df


def transform_data(merged_df: pd.DataFrame) -> pd.DataFrame:
    """
    STEP 2: 呼叫 DataRefiner 進行商家名稱正規化、前綴處理與交易類型分類
    """
    final_df = merged_df

    if merged_df is not None and not merged_df.empty:
        try:
            logger.info("🔧 啟動 Refiner 進行商業邏輯清洗...")
            if ConfigLoader:
                configs = {
                    'merchants': ConfigLoader.load_config(CONFIG_DIR, 'dim_merchants', strategy='append'),
                    'cards': ConfigLoader.load_config(CONFIG_DIR, 'bridge_user_cards', strategy='replace'),
                    'gateways': ConfigLoader.load_config(CONFIG_DIR, 'dim_payment_process', strategy='append'),
                    'ec_platforms': ConfigLoader.load_config(CONFIG_DIR, 'dim_ec_platform', strategy='append'),
                    'txn_types': ConfigLoader.load_yaml('transaction_types.yaml', config_dir=CONFIG_DIR)
                }
                refiner = DataRefiner(config_dir=CONFIG_DIR, configs=configs)
            else:
                refiner = DataRefiner(config_dir=CONFIG_DIR)
            
            final_df = refiner.process(merged_df)

            logger.info("✨ 資料清洗完成")
        except Exception as e:
            logger.error(f"❌ Refiner 清洗過程發生嚴重錯誤: {e}")
            save_anomaly_report(merged_df, 'crash_dump_refiner.csv', "清洗過程發生崩潰，已備份原始合併資料")
            final_df = merged_df

    return final_df
