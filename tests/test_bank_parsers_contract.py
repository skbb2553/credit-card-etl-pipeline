import os
import pytest
import pandas as pd
from typing import Dict
from unittest.mock import MagicMock, patch

import const
from etl.extraction import get_parser, get_bank_info
from etl.parsers.sinopac import SinopacBillParser
from tests.fixtures.setup_fixtures import create_mock_fixtures

# 確保測試前 Fixture 檔案已存在並明確宣告型別
FIXTURES: Dict[str, str] = create_mock_fixtures()

REQUIRED_COLUMNS = [
    const.COL_TXN_DATE,
    const.COL_MERCHANT,
    const.COL_CARD_NO,
    const.COL_PAY_AMOUNT,
    const.COL_BANK_NAME
]

@pytest.fixture(scope="module")
def mock_sinopac_pdf():
    """模擬永豐銀行 PDF 的三明治表格結構"""
    table_p1 = [
        ["消費日", "入帳日", "卡號", "交易說明", "幣別", "折算日", "新台幣金額"],
        ["2024/10/01", "2024/10/03", "1234", "LINE Pay－全家", "TWD", "2024/10/03", "$150"],
        ["", "", "", "APP特店折抵", "", "", ""],  # Bottom Bun
        ["2024/10/05", "2024/10/07", "1234", "高鐵購票", "TWD", "2024/10/07", "$1,490"],
        ["2024/10/10", "2024/10/12", "1234", "退款折抵", "TWD", "2024/10/12", "$-200"]
    ]
    mock_page = MagicMock()
    mock_page.extract_tables.return_value = [table_p1]
    mock_pdf = MagicMock()
    mock_pdf.pages = [mock_page]
    mock_pdf.__enter__.return_value = mock_pdf
    return mock_pdf


class TestCrossBankParsersContract:
    """跨銀行標準契約測試：驗證所有銀行解析器產出之 DataFrame 均符合統一架構規範"""

    @pytest.mark.parametrize("bank_key,fixture_key", [
        ("esun", "esun"),
        ("cathay", "cathay"),
        ("ctbc", "ctbc"),
        ("hncb", "hncb"),
    ])
    def test_csv_html_parsers_standard_contract(self, bank_key, fixture_key):
        file_path = FIXTURES[fixture_key]
        filename = os.path.basename(file_path)

        parser = get_parser(filename)
        assert parser is not None, f"❌ 無法根據檔名取得 Parser: {filename}"

        df = parser.parse(file_path)
        assert not df.empty, f"❌ {bank_key} Parser 解析結果不應為空"

        # 1. 驗證核心欄位存在
        for col in REQUIRED_COLUMNS:
            assert col in df.columns, f"❌ {bank_key} 缺少必要欄位: {col}"

        # 2. 驗證欄位型別契約
        assert pd.api.types.is_datetime64_any_dtype(df[const.COL_TXN_DATE]), \
            f"❌ {bank_key} {const.COL_TXN_DATE} 應為 datetime64 型態"
        assert pd.api.types.is_float_dtype(df[const.COL_PAY_AMOUNT]) or pd.api.types.is_integer_dtype(df[const.COL_PAY_AMOUNT]), \
            f"❌ {bank_key} {const.COL_PAY_AMOUNT} 應為數值型態"

        # 3. 驗證卡號不包含浮點 '.0' 殘留
        card_series = df[const.COL_CARD_NO].dropna().astype(str)
        assert not card_series.str.endswith('.0').any(), f"❌ {bank_key} 卡號不得以 '.0' 結尾"

        # 4. 驗證銀行名稱不可為空
        assert (df[const.COL_BANK_NAME].dropna() != '').all(), f"❌ {bank_key} 銀行名稱不可有空值"

    def test_sinopac_parser_standard_contract(self, mock_sinopac_pdf):
        """驗證永豐 PDF 解析器亦符合相同標準契約"""
        parser = SinopacBillParser(bank_id_or_keyword="sinopac")
        with patch("pdfplumber.open", return_value=mock_sinopac_pdf):
            df = parser.parse("202410_永豐銀行帳單.pdf")

        assert not df.empty, "❌ 永豐 PDF Parser 解析結果不應為空"
        for col in REQUIRED_COLUMNS:
            assert col in df.columns, f"❌ 永豐缺少必要欄位: {col}"

        assert pd.api.types.is_datetime64_any_dtype(df[const.COL_TXN_DATE])
        assert pd.api.types.is_float_dtype(df[const.COL_PAY_AMOUNT])
        assert (df[const.COL_BANK_NAME].dropna() != '').all()


class TestBankSpecificFeatures:
    """各銀行專屬欄位與特例邏輯邊界測試"""

    def test_esun_features(self):
        """測試玉山：e.Point 折抵、行動支付標記、Master 列卡號擴散"""
        parser = get_parser(os.path.basename(FIXTURES["esun"]))
        df = parser.parse(FIXTURES["esun"])

        # 1. 驗證多卡號正常分派 (U Bear 5413 與 Unicard 1313)
        assert "5413" in df[const.COL_CARD_NO].values
        assert "1313" in df[const.COL_CARD_NO].values

        # 2. 驗證玉山 Wallet 行動支付標註
        wallet_rows = df[df[const.COL_MERCHANT].astype(str).str.contains("全家")]
        assert not wallet_rows.empty
        if "payment_process" in wallet_rows.columns:
            assert wallet_rows["payment_process"].iloc[0] == "玉山Wallet"

    def test_cathay_features(self):
        """測試國泰：國家幣別切割、雙卡號拆分、Stop keyword 停止行"""
        parser = get_parser(os.path.basename(FIXTURES["cathay"]))
        df = parser.parse(FIXTURES["cathay"])

        # 1. 驗證 stop_at_keyword 生效，不包含 "正卡消費"
        assert not df[const.COL_MERCHANT].astype(str).str.contains("正卡消費").any()

        # 2. 驗證 APPLE.COM/BILL 國別與幣別解析
        apple_row = df[df[const.COL_MERCHANT].astype(str).str.contains("APPLE.COM/BILL")]
        assert not apple_row.empty
        assert apple_row[const.COL_LOCATION].iloc[0] == "IE"
        assert apple_row[const.COL_CURRENCY].iloc[0] == "TWD"
        assert apple_row[const.COL_CURR_AMOUNT].iloc[0] == 30.0

    def test_ctbc_features(self):
        """測試中信：cp950 編碼繁體中文無亂碼、消費地解析"""
        parser = get_parser(os.path.basename(FIXTURES["ctbc"]))
        df = parser.parse(FIXTURES["ctbc"])

        # 驗證中文摘要正常解析
        assert df[const.COL_MERCHANT].astype(str).str.contains("統一超商").any()
        assert (df[const.COL_CARD_NO] == "4321").all()
        assert (df[const.COL_LOCATION] == "TW").all()

    def test_hncb_features(self):
        """測試華南：HTML Table 解析、立即繳費停止行、星號卡號正則抽取"""
        parser = get_parser(os.path.basename(FIXTURES["hncb"]))
        df = parser.parse(FIXTURES["hncb"])

        # 1. 驗證 stop_at_keyword 生效，不包含 "立即繳費"
        assert not df[const.COL_MERCHANT].astype(str).str.contains("立即繳費").any()

        # 2. 驗證星號正則抽取卡號 5678
        assert (df[const.COL_CARD_NO] == "5678").all()
        assert df[const.COL_MERCHANT].astype(str).str.contains("全家便利商店").any()

    def test_sinopac_sandwich_and_amounts(self, mock_sinopac_pdf):
        """測試永豐：三明治結構多行合併與金額清洗"""
        parser = SinopacBillParser(bank_id_or_keyword="sinopac")
        with patch("pdfplumber.open", return_value=mock_sinopac_pdf):
            df = parser.parse("202410_永豐銀行帳單.pdf")

        # 1. 驗證三明治多行文字重組
        merged_row = df[df[const.COL_MERCHANT].str.contains("LINE Pay－全家")]
        assert not merged_row.empty
        assert "APP特店折抵" in merged_row[const.COL_MERCHANT].iloc[0]

        # 2. 驗證千分位金額清洗 ($1,490 -> 1490.0)
        hsr_row = df[df[const.COL_MERCHANT].str.contains("高鐵購票")]
        assert hsr_row[const.COL_PAY_AMOUNT].iloc[0] == 1490.0

        # 3. 驗證負數金額清洗 ($-200 -> -200.0)
        refund_row = df[df[const.COL_MERCHANT].str.contains("退款折抵")]
        assert refund_row[const.COL_PAY_AMOUNT].iloc[0] == -200.0
