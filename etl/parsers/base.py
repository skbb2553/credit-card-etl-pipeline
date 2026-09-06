# etl/parsers/base.py
import pandas as pd
import io
import os
import re
import logging
import const
from bs4 import BeautifulSoup
import lxml 
from typing import Any, List, Optional

logger = logging.getLogger(__name__)

# ==========================================
# 1. 內部工具函式
# ==========================================
def _parse_date_smart(value, base_year=None, bill_month=None):
    """
    智慧日期解析：
    1. 支援完整格式 (YYYY/MM/DD, 民國年) -> 直接解析
    2. 支援缺年格式 (MM/DD) -> 根據 base_year 與 bill_month 自動補全年份
    """
    if pd.isna(value) or str(value).strip() == '':
        return pd.NaT
    
    s = str(value).strip()
    if s.lower() == 'nan': return pd.NaT

    # --- A. 嘗試標準完整格式 (含年份) ---
    if len(re.findall(r'[-/]', s)) >= 2:
        try:
            match = re.match(r'^(\d{2,3})[-/](\d{1,2})[-/](\d{1,2})', s)
            if match:
                y_str, m, d = match.groups()
                year = int(y_str)
                if year < 1911: year += 1911
                return pd.Timestamp(year=year, month=int(m), day=int(d))
            return pd.to_datetime(s)
        except:
            pass

    # --- B. 處理缺年格式 (MM/DD) ---
    if base_year and bill_month:
        match_md = re.match(r'^(\d{1,2})[-/](\d{1,2})', s)
        if match_md:
            m_str, d_str = match_md.groups()
            m, d = int(m_str), int(d_str)
            final_year = base_year
            if bill_month < m and (m - bill_month) > 6: 
                final_year -= 1
            elif bill_month == 12 and m == 1:
                final_year += 1
            try:
                return pd.Timestamp(year=final_year, month=m, day=d)
            except ValueError:
                pass

    return pd.NaT

# ==========================================
# 2. Base Classes
# ==========================================

class BaseBillParser:
    """
    [基底類別] 負責通用邏輯：檔名解析、日期清洗
    """

    def __init__(self, bank_id_or_keyword: Optional[str] = None, **kwargs):
        self.bank = None
        self.bank_id = ""
        self.bank_no = ""
        self.bank_name = ""
        
        if bank_id_or_keyword:
            self.bank = const.get_bank_by_keyword(bank_id_or_keyword)
            if self.bank:
                self.bank_id = self.bank.get('bank_id', '')
                self.bank_no = self.bank.get('bank_no', '')
                self.bank_name = self.bank.get('bills_mapping_name', self.bank.get('bank_name', ''))
                logger.info(f"✅ 已載入銀行配置：{self.bank_name} ({self.bank_id})")
            else:
                logger.warning(f"⚠️ 無法識別的銀行標誌: {bank_id_or_keyword}")

    def get_bank_info(self, keyword_or_id: str) -> dict:
        """傳入 bank_id 或關鍵字，動態從 dim_banks.yaml 取得銀行資訊字典"""
        return const.get_bank_by_keyword(keyword_or_id) or {}

    def _get_bill_period(self, filepath: str):
        filename = os.path.basename(filepath)
        patterns = [
            (r'(\d{2,3})年(\d{1,2})月', True),
            (r'(20\d{2})(\d{2})', False),
            (r'(\d{3})(\d{2})', True)
        ]
        for pat, is_roc in patterns:
            match = re.search(pat, filename)
            if match:
                y_str, m_str = match.group(1), match.group(2)
                year = int(y_str)
                month = int(m_str)
                if is_roc: year += 1911
                return year, month
        return None, None

    def transform_common_dates(self, df: pd.DataFrame, filepath: str) -> pd.DataFrame:
        base_year, bill_month = self._get_bill_period(filepath)
        if base_year and bill_month:
            df[const.COL_STAT_MON] = pd.Timestamp(year=base_year, month=bill_month, day=1)
        else:
            df[const.COL_STAT_MON] = pd.NaT

        target_cols = [const.COL_TXN_DATE, const.COL_POST_DATE, const.COL_CONV_DATE]
        for col in target_cols:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: _parse_date_smart(x, base_year, bill_month))

        if const.COL_TXN_DATE in df.columns:
            df = df.dropna(subset=[const.COL_TXN_DATE])
        
        df = df.dropna(axis=1, how='all')
        return df
    
    def _enforce_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        for col_name, dtype in const.COLUMN_TYPES.items():
            if col_name not in df.columns:
                continue
            if dtype == 'float':
                if df[col_name].dtype == 'object':
                    df[col_name] = df[col_name].astype(str).str.replace(',', '', regex=False)
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
            elif dtype == 'str':
                s = df[col_name].astype(str)
                s = s.str.replace(r'^(\d+)\.0$', r'\1', regex=True)
                s = s.str.strip()
                df[col_name] = s
                df[col_name] = df[col_name].replace({'nan': None, 'None': None, '': None})
            elif dtype == 'date':
                if not pd.api.types.is_datetime64_any_dtype(df[col_name]):
                     df[col_name] = pd.to_datetime(df[col_name], format='mixed', errors='coerce')
        return df
    
    def _finalize_normalization(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        最終正規化邏輯：
        1. 補齊基本欄位 (確保 Standard Columns 存在)
        2. 處理 Payment_Currency：若有金額但無幣別，預設補 TWD；並進行 Enum 標準化
        3. 處理 Currency_Type：若有金額但無幣別，預設補 TWD；並進行 Enum 標準化
        4. 處理 Location：進行 Enum 標準化 (Alpha-3 轉 Alpha-2)
        """
        # 1. 處理 Payment_Currency
        if const.COL_PAY_AMOUNT in df.columns:
            if const.COL_PAY_CURR not in df.columns:
                df[const.COL_PAY_CURR] = 'TWD'
            else:
                df[const.COL_PAY_CURR] = df[const.COL_PAY_CURR].fillna('TWD')
                mask_empty = (df[const.COL_PAY_CURR].astype(str).str.strip() == '')
                df.loc[mask_empty, const.COL_PAY_CURR] = 'TWD'
            
            # Enum 標準化 (如: NTD -> TWD)
            df[const.COL_PAY_CURR] = df[const.COL_PAY_CURR].apply(lambda x: const.Currency.normalize(x))

        # 2. 處理 Currency_Type (原始幣別)
        if const.COL_CURR_AMOUNT in df.columns:
            if const.COL_CURRENCY not in df.columns:
                 df[const.COL_CURRENCY] = 'TWD'
            else:
                df[const.COL_CURRENCY] = df[const.COL_CURRENCY].fillna('TWD')
                mask_empty = (df[const.COL_CURRENCY].astype(str).str.strip() == '')
                df.loc[mask_empty, const.COL_CURRENCY] = 'TWD'
            
            # Enum 標準化
            df[const.COL_CURRENCY] = df[const.COL_CURRENCY].apply(lambda x: const.Currency.normalize(x))

        # 3. 處理 Location (國別標準化: TWN -> TW)
        if const.COL_LOCATION in df.columns:
            df[const.COL_LOCATION] = df[const.COL_LOCATION].apply(lambda x: const.Location.normalize(x))

        return df

    def extract_card_info_generic(self, df: pd.DataFrame, config: dict) -> pd.DataFrame:
        """
        [通用卡號提取邏輯]
        適用於：帳單結構為 "卡號標題列" -> "多筆交易" -> "卡號標題列" -> "多筆交易" 的形式。
        使用 ffill (Forward Fill) 將標題列資訊帶入交易列。
        
        Args:
            config (dict): 包含 'trigger', 'card_no', 'card_type' 的 Regex 設定
        """
        # 必要的欄位檢查
        if const.COL_MERCHANT not in df.columns:
            return df

        trigger = config.get('trigger')
        card_no_pattern = config.get('card_no')
        card_type_pattern = config.get('card_type')

        if not isinstance(trigger, str) or not isinstance(card_no_pattern, str):
            return df

        # 1. 標記 Master Rows (包含卡號資訊的標題列)
        # 使用 na=False 避免非字串資料報錯
        mask_master = df[const.COL_MERCHANT].astype(str).str.contains(trigger, na=False, regex=True)

        if mask_master.any():
            # 2. 擴散 (Propagate)
            # 建立暫存欄位，先把 Master 的內容填進去
            df['raw_master_info'] = df.loc[mask_master, const.COL_MERCHANT]
            # 向下填補 (Forward Fill)，讓下面的交易都知道自己屬於哪個 Master
            df['raw_master_info'] = df['raw_master_info'].ffill()

            # 3. 提取 (Extract)
            # 從暫存欄位中，用 Regex 抓出卡號與卡別
            df[const.COL_CARD_NO] = df['raw_master_info'].str.extract(card_no_pattern)
            
            if isinstance(card_type_pattern, str):
                df[const.COL_CARD_TYPE] = df['raw_master_info'].str.extract(card_type_pattern)

            # 4. [Fix Issue] 繳款/轉帳資料防呆
            # 邏輯：繳款紀錄不屬於特定卡片消費，強制清空卡號
            mask_payment = df[const.COL_MERCHANT].astype(str).str.contains('繳款|轉帳', na=False, regex=True)
            if mask_payment.any():
                df.loc[mask_payment, const.COL_CARD_NO] = None
                if const.COL_CARD_TYPE in df.columns:
                    df.loc[mask_payment, const.COL_CARD_TYPE] = None

            # 5. 清理戰場
            # 刪除原始的 Master Rows (它們不是交易)
            filtered_df = df.loc[~mask_master].copy()
            if isinstance(filtered_df, pd.DataFrame):
                df = filtered_df
            # 刪除暫存欄位
            df = df.drop(columns=['raw_master_info'])

        return df

    def _clean_amount(self, df: pd.DataFrame, col_name: str) -> pd.Series:
        """標準化金額欄位：去除逗號，轉為浮點數"""
        if col_name not in df.columns:
            return pd.Series(0, index=df.index)
            
        # 1. 先轉字串，去除前後空白
        series = df[col_name].astype(str).str.strip()
        
        # 2. [關鍵修正] 去除千分位逗號 (1,868 -> 1868)
        series = series.str.replace(',', '', regex=False)
        
        # 3. 轉為數字 (無法轉換的變成 NaN，然後填 0)
        numeric_series = pd.to_numeric(series, errors='coerce')
        if not isinstance(numeric_series, pd.Series):
            numeric_series = pd.Series(numeric_series, index=df.index)
        return numeric_series.fillna(0)

class BaseCsvParser(BaseBillParser):
    """[CSV 專用基底]"""
    def __init__(self, bank_id_or_keyword: Optional[str] = None, **kwargs):
        super().__init__(bank_id_or_keyword=bank_id_or_keyword, **kwargs)

    def read_csv_smart(self, filepath: str, encoding: str, header_keyword: str, stop_at_keyword: Optional[str] = None) -> pd.DataFrame:
        content_buffer = []
        found_header = False
        try:
            with open(filepath, 'r', encoding=encoding, errors='replace') as f:
                all_lines = f.readlines()
            for line in all_lines:
                if not found_header:
                    if header_keyword and header_keyword in line:
                        found_header = True
                        content_buffer.append(line)
                    continue
                if stop_at_keyword and stop_at_keyword in line:
                    break
                content_buffer.append(line)
            if found_header and content_buffer:
                return pd.read_csv(io.StringIO("".join(content_buffer)), on_bad_lines='skip', dtype=str)
            else:
                return pd.read_csv(filepath, encoding=encoding, header=0, on_bad_lines='skip', dtype=str)
        except Exception as e:
            logger.error(f"❌ Smart Read 失敗 ({os.path.basename(filepath)}): {e}")
            return pd.DataFrame()
        
class BaseHtmlParser(BaseBillParser):
    """[HTML/XLS 專用基底]"""
    def __init__(self, bank_id_or_keyword: Optional[str] = None, **kwargs):
        super().__init__(bank_id_or_keyword=bank_id_or_keyword, **kwargs)

    def read_html_smart(self, filepath: str, encoding: str, header_keyword: str, stop_at_keyword: Optional[str] = None) -> pd.DataFrame:
        try:
            with open(filepath, 'r', encoding=encoding, errors='replace') as f:
                soup = BeautifulSoup(f, 'lxml')
            header_node = soup.find(string=lambda t: t and header_keyword in t)
            if not header_node: return pd.DataFrame()
            target_table = header_node.find_parent('table')
            if not target_table: return pd.DataFrame()
            dfs = pd.read_html(io.StringIO(str(target_table)), header=0)
            if not dfs: return pd.DataFrame()
            df = dfs[0]
            df.columns = ["".join(f"{c}".split()) for c in df.columns]
            if stop_at_keyword:
                mask = df.astype(str).apply(lambda x: x.str.contains(stop_at_keyword, na=False)).any(axis=1)
                if mask.any():
                    sliced_df = df.iloc[:mask.idxmax()]
                    if isinstance(sliced_df, pd.DataFrame):
                        df = sliced_df
            return df
        except Exception as e:
            logger.error(f"❌ Smart HTML Read 失敗 ({os.path.basename(filepath)}): {e}")
            return pd.DataFrame()

class BasePdfParser(BaseBillParser):
    """
    [PDF 專用基底] 提供 PDF 解析通用的工具函式
    """
    def __init__(self, bank_id_or_keyword: Optional[str] = None, **kwargs):
        super().__init__(bank_id_or_keyword=bank_id_or_keyword, **kwargs)
        self.target_columns = [
            const.COL_TXN_DATE, const.COL_POST_DATE, const.COL_MERCHANT,
            const.COL_CARD_NO, const.COL_PAY_AMOUNT, const.COL_CONV_DATE, 
            const.COL_CURR_AMOUNT
        ]

    def _is_date_pattern(self, value: Any, pattern: str = r'^\d{4}/\d{2}/\d{2}$') -> bool:
        """通用日期格式偵測"""
        if not value: return False
        return bool(re.match(pattern, str(value).strip()))

    def _clean_pdf_amount(self, value: Any, strip_prefix: str = '') -> float:
        """PDF 金額清洗 (支援括號、後綴負號、前綴負號含全形)"""
        if value is None: return 0.0
        val_str = str(value).strip().replace(',', '').replace('－', '-')
        if strip_prefix and val_str.startswith(strip_prefix):
            val_str = val_str[len(strip_prefix):].strip()
        if not val_str or val_str.lower() == 'nan': return 0.0
        is_negative = (
            (val_str.startswith('(') and val_str.endswith(')')) or
            val_str.endswith('-') or
            val_str.startswith('-')
        )
        clean_num = re.sub(r'[^\d.]', '', val_str)
        try:
            f_val = float(clean_num) if clean_num else 0.0
            return -f_val if is_negative else f_val
        except ValueError:
            return 0.0
