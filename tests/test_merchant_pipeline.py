# tests/test_merchant_pipeline.py
import pytest
import pandas as pd
import const
from etl.processors.merchant import (
    MerchantPipeline,
    MerchantNormalizer,
    PaymentProcessTagger,
    ECPlatformTagger,
    _sort_rules_by_priority,
    _apply_final_prefixes
)
from etl.transformation import DataRefiner
from profiles.loaders.config_loader import ConfigLoader


class TestRuleSorting:
    """驗證規則排序輔助函式 _sort_rules_by_priority"""

    def test_empty_or_none_rules(self):
        assert _sort_rules_by_priority(None).empty
        assert _sort_rules_by_priority(pd.DataFrame()).empty

    def test_priority_ascending_order(self):
        rules = pd.DataFrame([
            {'rule_id': 'R3', 'priority': 50},
            {'rule_id': 'R1', 'priority': 5},
            {'rule_id': 'R2', 'priority': 20},
        ])
        sorted_rules = _sort_rules_by_priority(rules)
        assert list(sorted_rules['rule_id']) == ['R1', 'R2', 'R3']

    def test_stable_sort_preserves_relative_order_for_same_priority(self):
        """驗證相同 priority 時維持原始順序 (kind='stable')"""
        rules = pd.DataFrame([
            {'rule_id': 'A1', 'priority': 10},
            {'rule_id': 'B1', 'priority': 10},
            {'rule_id': 'C1', 'priority': 5},
            {'rule_id': 'A2', 'priority': 10},
        ])
        sorted_rules = _sort_rules_by_priority(rules)
        assert list(sorted_rules['rule_id']) == ['C1', 'A1', 'B1', 'A2']


class TestMerchantPipelineMockRules:
    """使用精確受控的 Mock 規則驗證 MerchantPipeline 核心行為"""

    @pytest.fixture
    def mock_configs(self):
        gateways = pd.DataFrame([
            {
                'payment_process_pattern': r'LINE\s*Pay',
                'payment_process': 'LINE Pay',
                'process_prefix': 'LINE Pay',
                'priority': 10
            },
            {
                'payment_process_pattern': r'街口',
                'payment_process': '街口支付',
                'process_prefix': '街口支付',
                'priority': 10
            }
        ])
        ec_platforms = pd.DataFrame([
            {
                'ec_platform_pattern': r'APPLE\.COM/BILL',
                'ec_platform': 'APPLE.COM/BILL',
                'ec_platform_type': '訂閱',
                'ec_category': '數位娛樂',
                'ec_sub_category': '應用商店',
                'priority': 10
            },
            {
                'ec_platform_pattern': r'蝦皮',
                'ec_platform': '蝦皮購物',
                'ec_platform_type': '電商平台',
                'ec_category': '網購與電商',
                'ec_sub_category': '綜合電商',
                'priority': 10
            }
        ])
        merchants = pd.DataFrame([
            {
                'merchant_pattern': r'統一超商|7-11|7-ELEVEN',
                'normalized_merchant': '統一超商',
                'category': '日常飲食',
                'sub_category': '便利商店',
                'priority': 10
            },
            {
                'merchant_pattern': r'APPLE\.COM/BILL',
                'normalized_merchant': 'APPLE.COM/BILL',
                'category': '數位娛樂',
                'sub_category': '應用商店',
                'priority': 10
            }
        ])
        return {
            'gateways': gateways,
            'ec_platforms': ec_platforms,
            'merchants': merchants
        }

    def test_payment_and_merchant_prefix_composition(self, mock_configs):
        """驗證支付前綴與商家名稱合成：[支付前綴]－[正規化特店]"""
        pipeline = MerchantPipeline(configs=mock_configs)
        df = pd.DataFrame([
            {const.COL_MERCHANT: 'LINE Pay-統一超商台北一店'}
        ])
        res = pipeline.process(df)
        assert res[const.COL_PAYMENT_PROCESS].iloc[0] == 'LINE Pay'
        assert res[const.COL_NORMALIZED_MERCHANT].iloc[0] == '統一超商'
        assert res[const.COL_MERCHANT_DISPLAY].iloc[0] == 'LINE Pay－統一超商'
        assert res[const.COL_CATEGORY].iloc[0] == '日常飲食'
        assert res[const.COL_SUB_CATEGORY].iloc[0] == '便利商店'

    def test_ec_platform_and_merchant_deduplication(self, mock_configs):
        """驗證電商平台與商家名稱去重：APPLE.COM/BILL 不重複出現"""
        pipeline = MerchantPipeline(configs=mock_configs)
        df = pd.DataFrame([
            {const.COL_MERCHANT: 'APPLE.COM/BILL ITUNES'}
        ])
        res = pipeline.process(df)
        assert res[const.COL_EC_PLATFORM].iloc[0] == 'APPLE.COM/BILL'
        assert res[const.COL_NORMALIZED_MERCHANT].iloc[0] == 'APPLE.COM/BILL'
        # 顯示名稱應去重，不能是 APPLE.COM/BILL－APPLE.COM/BILL
        assert res[const.COL_MERCHANT_DISPLAY].iloc[0] == 'APPLE.COM/BILL'

    def test_ec_platform_fallback_when_merchant_not_in_dim_merchants(self, mock_configs):
        """驗證未在 dim_merchants 時，以電商平台名稱與分類進行階層補位"""
        pipeline = MerchantPipeline(configs=mock_configs)
        df = pd.DataFrame([
            {const.COL_MERCHANT: '街口支付-蝦皮海外賣家無名小舖'}
        ])
        res = pipeline.process(df)
        assert res[const.COL_PAYMENT_PROCESS].iloc[0] == '街口支付'
        assert res[const.COL_EC_PLATFORM].iloc[0] == '蝦皮購物'
        # normalized_merchant 應由電商平台補位
        assert res[const.COL_NORMALIZED_MERCHANT].iloc[0] == '蝦皮購物'
        assert res[const.COL_CATEGORY].iloc[0] == '網購與電商'
        assert res[const.COL_SUB_CATEGORY].iloc[0] == '綜合電商'
        # 顯示名稱合成
        assert res[const.COL_MERCHANT_DISPLAY].iloc[0] == '街口支付－蝦皮購物'

    def test_raw_merchant_fallback_when_nothing_matches(self, mock_configs):
        """驗證無任何規則命中時，直接 fallback 回原始商家名稱"""
        pipeline = MerchantPipeline(configs=mock_configs)
        df = pd.DataFrame([
            {const.COL_MERCHANT: '巷口陽春麵攤'}
        ])
        res = pipeline.process(df)
        assert res[const.COL_NORMALIZED_MERCHANT].iloc[0] == '巷口陽春麵攤'
        assert res[const.COL_MERCHANT_DISPLAY].iloc[0] == '巷口陽春麵攤'


class TestDataRefinerWithPipeline:
    """驗證 DataRefiner 與 MerchantPipeline 整合"""

    def test_refiner_end_to_end_with_common_configs(self):
        """使用專案 common configs 驗證 DataRefiner 整合流程"""
        configs = {
            'merchants': ConfigLoader.load_config(const.CONFIG_DIR, 'dim_merchants', strategy='append'),
            'cards': ConfigLoader.load_config(const.CONFIG_DIR, 'bridge_user_cards', strategy='replace'),
            'gateways': ConfigLoader.load_config(const.CONFIG_DIR, 'dim_payment_process', strategy='append'),
            'ec_platforms': ConfigLoader.load_config(const.CONFIG_DIR, 'dim_ec_platform', strategy='append'),
            'txn_types': ConfigLoader.load_yaml('transaction_types.yaml', config_dir=const.CONFIG_DIR)
        }
        refiner = DataRefiner(config_dir=const.CONFIG_DIR, configs=configs)
        
        df = pd.DataFrame([
            {
                const.COL_TXN_DATE: '2025-10-01',
                const.COL_MERCHANT: '連線商業銀行－LINE Pay*統一超商',
                const.COL_CARD_NO: '1234',
                const.COL_PAY_AMOUNT: 100.0,
                const.COL_CURRENCY: 'TWD',
                const.COL_LOCATION: 'TW'
            }
        ])

        res = refiner.process(df)
        assert not res.empty
        assert res[const.COL_PAYMENT_PROCESS].iloc[0] == 'Line Pay'
        assert res[const.COL_NORMALIZED_MERCHANT].iloc[0] == '統一超商'
        assert 'LinePay' in res[const.COL_MERCHANT_DISPLAY].iloc[0]
        assert '統一超商' in res[const.COL_MERCHANT_DISPLAY].iloc[0]
