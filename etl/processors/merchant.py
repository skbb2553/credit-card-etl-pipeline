# etl/processors/merchant.py
import pandas as pd
import logging
import re
import const
import warnings
from typing import Optional, Union, Tuple, Dict, Any

warnings.filterwarnings("ignore", message=".*has match groups.*", category=UserWarning)
logger = logging.getLogger(__name__)


def _sort_rules_by_priority(rules_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """
    統一規則優先級排序工具函式：
    - 將 priority 欄位轉為數值型態 (空值保底填入 999)
    - 以 kind='stable' (穩定排序) 確保同 Priority 內部保持 CSV 原始定義順序
    """
    if rules_df is None or rules_df.empty:
        return pd.DataFrame() if rules_df is None else rules_df
    if 'priority' in rules_df.columns:
        df = rules_df.copy()
        priority_series = pd.to_numeric(df['priority'], errors='coerce')
        if isinstance(priority_series, pd.Series):
            df['priority'] = priority_series.fillna(999)
        else:
            df['priority'] = 999 if pd.isna(priority_series) else priority_series
        return df.sort_values('priority', ascending=True, kind='stable')
    return rules_df


class MerchantNormalizer:
    def __init__(self, config_dir: Optional[str] = None, rules: Optional[pd.DataFrame] = None, **kwargs):
        """
        商戶名稱正規化處理器 (Step 3: 最內層特店識別)
        :param rules: 由外部注入的規則 DataFrame (包含 merchant_pattern, normalized_merchant, priority, category, sub_category)
        """
        if rules is None and isinstance(config_dir, pd.DataFrame):
            rules = config_dir
            config_dir = None
        self.rules = _sort_rules_by_priority(rules)

    def process(self, df: pd.DataFrame, return_mask: bool = False) -> Union[pd.DataFrame, Tuple[pd.DataFrame, pd.Series]]:
        if df.empty: 
            return (df, pd.Series(False, index=df.index)) if return_mask else df

        # 初始化必要欄位
        if const.COL_CATEGORY not in df.columns: 
            df[const.COL_CATEGORY] = None
        if const.COL_SUB_CATEGORY not in df.columns: 
            df[const.COL_SUB_CATEGORY] = None
        if const.COL_NORMALIZED_MERCHANT not in df.columns: 
            df[const.COL_NORMALIZED_MERCHANT] = None

        if self.rules.empty:
            return (df, pd.Series(False, index=df.index)) if return_mask else df

        processed_mask = pd.Series(False, index=df.index)
        merchants = df[const.COL_MERCHANT].astype(str).str.strip()

        for _, rule in self.rules.iterrows():
            pattern = rule.get(const.COL_MERCHANT_PATTERN) or rule.get('merchant_pattern') or rule.get('merchant_patterns') or rule.get('pattern')
            replacement = rule.get(const.COL_NORMALIZED_MERCHANT) or rule.get('normalized_merchant') or rule.get('merchant')
            category = rule.get(const.COL_CATEGORY) or rule.get('category')
            sub_category = rule.get(const.COL_SUB_CATEGORY) or rule.get('sub_category')

            if not isinstance(pattern, str) or pattern == '': continue

            try:
                mask = merchants.str.contains(pattern, case=False, regex=True, na=False)
            except re.error:
                logger.warning(f"⚠️ 無法解析商家正規化正則表達式: {pattern}")
                continue

            if mask.any():
                target_mask = mask & (~processed_mask)
                if target_mask.any():
                    if pd.notna(replacement) and str(replacement).strip() != '':
                        df.loc[target_mask, const.COL_NORMALIZED_MERCHANT] = str(replacement).strip()

                    if pd.notna(category) and str(category).strip() != '':
                        df.loc[target_mask, const.COL_CATEGORY] = str(category).strip()

                    if pd.notna(sub_category) and str(sub_category).strip() != '':
                        df.loc[target_mask, const.COL_SUB_CATEGORY] = str(sub_category).strip()

                    processed_mask |= target_mask

        return (df, processed_mask) if return_mask else df


class PaymentProcessTagger:
    """
    負責標記支付管道或處理方式 (Step 1: 最外層支付通路識別)
    如: LinePay, 街口, 悠遊付, 全支付
    """
    def __init__(self, config_dir: Optional[str] = None, rules: Optional[pd.DataFrame] = None, **kwargs):
        if rules is None and isinstance(config_dir, pd.DataFrame):
            rules = config_dir
            config_dir = None
        self.rules = _sort_rules_by_priority(rules)

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.rules.empty or df.empty: return df

        if const.COL_PROCESS_PREFIX not in df.columns:
            df[const.COL_PROCESS_PREFIX] = ''
        else:
            df[const.COL_PROCESS_PREFIX] = df[const.COL_PROCESS_PREFIX].fillna('')

        if const.COL_PAYMENT_PROCESS not in df.columns:
            df[const.COL_PAYMENT_PROCESS] = ''
        else:
            df[const.COL_PAYMENT_PROCESS] = df[const.COL_PAYMENT_PROCESS].fillna('')

        merchants = df[const.COL_MERCHANT].astype(str).str.strip()
        oem_pay_keywords = ['Apple Pay', 'Google Pay', 'Samsung Pay', 'Garmin Pay', 'Hami Pay', 'Google Wallet']

        for _, rule in self.rules.iterrows():
            pattern = rule.get(const.COL_PROCESS_PATTERN) or rule.get('payment_process_pattern')
            prefix = rule.get(const.COL_PROCESS_PREFIX) or rule.get('process_prefix')
            process_name = rule.get(const.COL_PAYMENT_PROCESS) or rule.get('payment_process')

            if not isinstance(pattern, str) or pattern == '': continue

            try:
                mask = merchants.str.contains(pattern, case=False, regex=True, na=False)
                if mask.any():
                    # 1. 填入前綴 (process_prefix)
                    if pd.notna(prefix):
                        prefix_str = str(prefix).strip()
                        empty_prefix = mask & (df[const.COL_PROCESS_PREFIX] == '')
                        if empty_prefix.any():
                            df.loc[empty_prefix, const.COL_PROCESS_PREFIX] = prefix_str

                    # 2. 判斷支付管道名稱 (payment_process) 或 OEM Pay (vpc_type)
                    if pd.notna(process_name):
                        val_process = str(process_name).strip()
                        is_oem = any(oem.lower() in val_process.lower() for oem in oem_pay_keywords)
                        if is_oem:
                            vpc_empty = mask & (df[const.COL_VPC_TYPE].fillna('') == '')
                            if vpc_empty.any():
                                df.loc[vpc_empty, const.COL_VPC_TYPE] = val_process
                        else:
                            empty_pay = mask & (df[const.COL_PAYMENT_PROCESS] == '')
                            if empty_pay.any():
                                df.loc[empty_pay, const.COL_PAYMENT_PROCESS] = val_process

            except re.error:
                continue

        return df


class ECPlatformTagger:
    """
    負責標記電商平台與電商分類 (Step 2: 中層電商平台識別)
    如: MOMO, 蝦皮, STEAM, PChome, APPLE.COM/BILL
    """
    def __init__(self, config_dir: Optional[str] = None, rules: Optional[pd.DataFrame] = None, **kwargs):
        if rules is None and isinstance(config_dir, pd.DataFrame):
            rules = config_dir
            config_dir = None
        self.rules = _sort_rules_by_priority(rules)

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.rules.empty or df.empty: return df

        # 初始化電商相關欄位
        for col in [const.COL_EC_PLATFORM, const.COL_EC_PLATFORM_TYPE, const.COL_EC_CATEGORY, const.COL_EC_SUB_CATEGORY]:
            if col not in df.columns:
                df[col] = ''
            else:
                df[col] = df[col].fillna('')

        merchants = df[const.COL_MERCHANT].astype(str).str.strip()

        for _, rule in self.rules.iterrows():
            pattern = rule.get(const.COL_EC_PLATFORM_PATTERN) or rule.get('ec_platform_pattern')
            platform_name = rule.get(const.COL_EC_PLATFORM) or rule.get('ec_platform')
            platform_type = rule.get(const.COL_EC_PLATFORM_TYPE) or rule.get('ec_platform_type')
            ec_category = rule.get(const.COL_EC_CATEGORY) or rule.get('ec_category')
            ec_sub_category = rule.get(const.COL_EC_SUB_CATEGORY) or rule.get('ec_sub_category')

            if not isinstance(pattern, str) or pattern == '': continue

            try:
                mask = merchants.str.contains(pattern, case=False, regex=True, na=False)
                if mask.any():
                    empty_mask = mask & (df[const.COL_EC_PLATFORM] == '')
                    if empty_mask.any():
                        if pd.notna(platform_name):
                            df.loc[empty_mask, const.COL_EC_PLATFORM] = str(platform_name).strip()
                        if pd.notna(platform_type):
                            df.loc[empty_mask, const.COL_EC_PLATFORM_TYPE] = str(platform_type).strip()
                        if pd.notna(ec_category):
                            df.loc[empty_mask, const.COL_EC_CATEGORY] = str(ec_category).strip()
                        if pd.notna(ec_sub_category):
                            df.loc[empty_mask, const.COL_EC_SUB_CATEGORY] = str(ec_sub_category).strip()

            except re.error:
                continue

        return df


def _apply_final_prefixes(df: pd.DataFrame) -> pd.DataFrame:
    """
    [標準化版本] 依照規範合併商家名稱
    公式：[支付前綴]－[電商平台]－[正規化商家名稱]
    """
    def compose_display(row):
        parts = []

        # 1. 支付前綴 (來自 process_prefix)
        prefix = str(row.get(const.COL_PROCESS_PREFIX, '')).strip()
        if prefix and prefix.lower() != 'nan':
            prefix = prefix.rstrip('－- ')
            parts.append(prefix)

        # 2. 電商平台 (來自 ec_platform)
        ec = str(row.get(const.COL_EC_PLATFORM, '')).strip()
        if ec and ec.lower() != 'nan':
            parts.append(ec)

        # 3. 正規化商家名稱 (來自 normalized_merchant)
        merchant = str(row.get(const.COL_NORMALIZED_MERCHANT, '')).strip()
        if merchant and merchant.lower() != 'nan':
            # [關鍵去重]：如果商家名稱跟電商平台完全一樣，就不重複添加
            # 例如：MOMO網購 (電商) + MOMO網購 (商家) -> 只顯示一次
            if merchant != ec:
                parts.append(merchant)

        return "－".join(parts) if parts else merchant

    df[const.COL_MERCHANT_DISPLAY] = df.apply(compose_display, axis=1)
    logger.info("✅ 已依照規範 [支付前綴]－[電商平台]－[正規化商家] 完成 Merchant_Display 合併")

    return df


class MerchantPipeline:
    """
    [統一商家名稱與分類處理管線 (Merchant Pipeline Facade)]
    封裝 SSOT 商家清洗流程：
    Step 1: 支付管道識別 (PaymentProcessTagger)
    Step 2: 電商平台識別 (ECPlatformTagger)
    Step 3: 商家正規化 (MerchantNormalizer)
    Step 4: 階層式補位 (Stack Fallback & Cascade)
      - 4.1 電商特店名稱補位 (未匹配且具電商平台)
      - 4.2 原始名稱兜底補位 (既無特店正規化亦無電商平台)
      - 4.3 電商分類層級補位 (category / sub_category)
    Step 5: 四層顯示名稱合成與去重 (_apply_final_prefixes)
    """
    def __init__(self, config_dir: Optional[str] = None, configs: Optional[Dict[str, Any]] = None, **kwargs):
        if configs is None and isinstance(config_dir, dict):
            configs = config_dir
            config_dir = None
        configs = configs or {}
        self.payment_tagger = PaymentProcessTagger(config_dir=config_dir, rules=configs.get('gateways'))
        self.ec_tagger = ECPlatformTagger(config_dir=config_dir, rules=configs.get('ec_platforms'))
        self.merchant_normalizer = MerchantNormalizer(config_dir=config_dir, rules=configs.get('merchants'))

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        # Step 1. 支付管道識別 (Payment Gateway - 最外層)
        df = self.payment_tagger.process(df)

        # Step 2. 電商平台識別 (EC Platform - 中層)
        df = self.ec_tagger.process(df)

        # Step 3. 商家正規化 (Merchant Normalization - 最內層)
        res = self.merchant_normalizer.process(df, return_mask=True)
        if isinstance(res, tuple):
            df, processed_mask = res
        else:
            df = res
            processed_mask = pd.Series(False, index=df.index)

        # Step 4. 階層式補位 (Stack Fallback & Cascade)
        if const.COL_NORMALIZED_MERCHANT not in df.columns:
            df[const.COL_NORMALIZED_MERCHANT] = None

        # 4.1 特店名稱補位：未被 dim_merchants 匹配時，若有電商平台則以電商平台名稱為準
        has_ec = (df[const.COL_EC_PLATFORM].fillna('') != '') if const.COL_EC_PLATFORM in df.columns else pd.Series(False, index=df.index)
        ec_fallback_mask = (~processed_mask) & has_ec
        if ec_fallback_mask.any():
            df.loc[ec_fallback_mask, const.COL_NORMALIZED_MERCHANT] = df.loc[ec_fallback_mask, const.COL_EC_PLATFORM]
            logger.info(f"💡 已為 {ec_fallback_mask.sum()} 筆未匹配商家套用電商平台 Fallback 清洗")

        # 4.2 若既無商家正規化也無電商平台，補為原始 merchant (銀行原始名稱)
        raw_fallback_mask = df[const.COL_NORMALIZED_MERCHANT].isna() | (df[const.COL_NORMALIZED_MERCHANT].astype(str).str.strip() == '')
        if raw_fallback_mask.any() and const.COL_MERCHANT in df.columns:
            df.loc[raw_fallback_mask, const.COL_NORMALIZED_MERCHANT] = df.loc[raw_fallback_mask, const.COL_MERCHANT]

        # 4.3 分類階層補位：若 category 為空且有 ec_category，則以 ec_category 補位
        if const.COL_EC_CATEGORY in df.columns and const.COL_CATEGORY in df.columns:
            cat_empty = df[const.COL_CATEGORY].isna() | (df[const.COL_CATEGORY].astype(str).str.strip() == '')
            cat_ec_has = df[const.COL_EC_CATEGORY].fillna('').astype(str).str.strip() != ''
            cat_fallback = cat_empty & cat_ec_has
            if cat_fallback.any():
                df.loc[cat_fallback, const.COL_CATEGORY] = df.loc[cat_fallback, const.COL_EC_CATEGORY]

        if const.COL_EC_SUB_CATEGORY in df.columns and const.COL_SUB_CATEGORY in df.columns:
            subcat_empty = df[const.COL_SUB_CATEGORY].isna() | (df[const.COL_SUB_CATEGORY].astype(str).str.strip() == '')
            subcat_ec_has = df[const.COL_EC_SUB_CATEGORY].fillna('').astype(str).str.strip() != ''
            subcat_fallback = subcat_empty & subcat_ec_has
            if subcat_fallback.any():
                df.loc[subcat_fallback, const.COL_SUB_CATEGORY] = df.loc[subcat_fallback, const.COL_EC_SUB_CATEGORY]

        # Step 5. 堆疊拼裝最終顯示名稱與去重 (Compose Merchant Display & Dedup)
        # 公式：[支付前綴]－[電商平台]－[正規化商家名稱]
        df = _apply_final_prefixes(df)

        return df


__all__ = [
    'MerchantPipeline',
    'MerchantNormalizer',
    'PaymentProcessTagger',
    'ECPlatformTagger',
    '_apply_final_prefixes',
    '_sort_rules_by_priority'
]
