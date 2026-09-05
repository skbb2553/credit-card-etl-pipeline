# generate_mock_data.py
import os
from typing import Dict
import pandas as pd
import const

def generate_mock_data(mock_dir: str = "profiles/example_public/data") -> Dict[str, str]:
    os.makedirs(mock_dir, exist_ok=True)
    print(f"Creating mock credit card bills in directory: '{mock_dir}'...")

    # ==========================================
    # 1. Cathay (Cube Card) Mock - UTF-8-sig
    # ==========================================
    cathay_filename = os.path.join(mock_dir, "202510國泰世華信用卡對帳單_mock.csv")
    # Columns: 消費日,入帳起息日,交易說明,消費國家/幣別,消費金額,新臺幣金額,卡號/行動末四碼,折算日
    cathay_data = [
        ["消費日", "入帳起息日", "交易說明", "消費國家/幣別", "消費金額", "新臺幣金額", "卡號/行動末四碼", "折算日"],
        ["2025/10/05", "2025/10/07", "麥當勞", "TW/TWD", "", "150", "0000", "2025/10/07"],
        ["2025/10/12", "2025/10/14", "新光三越", "TW/TWD", "", "3500", "0000", "2025/10/14"],
        ["2025/10/15", "2025/10/17", "點數折抵＿新光三", "TW/TWD", "", "-120", "0000", "2025/10/17"],
        ["2025/10/20", "2025/10/22", "全家便利商店", "TW/TWD", "", "200", "0000", "2025/10/22"],
        ["2025/10/25", "2025/10/27", "APPLE.COM/BILL", "IE/TWD", "30", "960", "0000", "2025/10/27"],
        ["2025/10/26", "2025/10/28", "ＯＰ錢包－統一超商", "TW/TWD", "", "100", "0000", "2025/10/28"],
        ["2025/10/28", "2025/10/30", "正卡消費", "", "", "", "", ""] # STOP KEYWORD Row
    ]
    with open(cathay_filename, "w", encoding="utf-8-sig") as f:
        for row in cathay_data:
            f.write(",".join(row) + "\n")
    print(f"Generated: {cathay_filename}")

    # ==========================================
    # 2. Esun (U Bear & Unicard) Mock - UTF-8
    # ==========================================
    esun_filename = os.path.join(mock_dir, "玉山銀行114年06月消費明細_mock.csv")
    # Columns: 消費日,入帳日,消費明細   消費地  外幣折算日,交易類別,幣別,金額,繳款幣別,金額.1,行動支付,卡號末四碼
    esun_data = [
        ["消費日", "入帳日", "消費明細   消費地  外幣折算日", "交易類別", "幣別", "金額", "繳款幣別", "金額.1", "行動支付", "卡號末四碼"],
        ["", "", "卡號：************5413（U bear卡正卡）", "", "", "", "", "", "", ""],
        ["114/06/05", "114/06/07", "APPLE.COM/BILL\tIRL\t06/07", "交易", "TWD", "200", "TWD", "200", "", "5413"],
        ["114/06/05", "114/06/07", "國外交易手續費", "交易", "TWD", "3", "TWD", "3", "", "5413"],
        ["114/06/10", "114/06/12", "全聯", "交易", "TWD", "500", "TWD", "500", "", "5413"],
        ["114/06/15", "114/06/17", "折抵現金 100 元 e point", "交易", "TWD", "-100", "TWD", "-100", "", "5413"],
        ["114/06/18", "114/06/20", "一卡通－萊爾富", "交易", "TWD", "65", "TWD", "65", "", "5413"],
        ["", "", "卡號：************1313（Unicard正卡）", "", "", "", "", "", "", ""],
        ["114/06/20", "114/06/22", "全家便利商店", "交易", "TWD", "80", "TWD", "80", "玉山Wallet", "1313"]
    ]
    with open(esun_filename, "w", encoding="utf-8") as f:
        for row in esun_data:
            f.write(",".join(row) + "\n")
    print(f"Generated: {esun_filename}")

    # ==========================================
    # 3. CTBC (LINE Pay Card) Mock - cp950
    # ==========================================
    ctbc_filename = os.path.join(mock_dir, "中國信託帳單明細查詢_20260608_mock.csv")
    # Columns: 消費日,入帳起息日,摘要,幣別,消費地金額,新臺幣金額,末四碼,外幣折算日,消費地
    ctbc_data = [
        ["消費日", "入帳起息日", "摘要", "幣別", "消費地金額", "新臺幣金額", "末四碼", "外幣折算日", "消費地"],
        ["115/06/01", "115/06/03", "連支＊統一超商", "TWD", "150", "150", "4321", "", "TW"],
        ["115/06/02", "115/06/04", "統一超商－實體門市", "TWD", "15", "15", "4321", "", "TW"],
        ["115/06/03", "115/06/05", "一卡通－統一超商", "TWD", "50", "50", "4321", "", "TW"],
        ["115/06/05", "115/06/07", "連加＊一般商品買賣", "TWD", "1200", "1200", "4321", "", "TW"]
    ]
    with open(ctbc_filename, "w", encoding="cp950") as f:
        for row in ctbc_data:
            f.write(",".join(row) + "\n")
    print(f"Generated: {ctbc_filename}")

    # ==========================================
    # 4. HNCB (Hua Nan HTML format) Mock - Big5
    # ==========================================
    hncb_filename = os.path.join(mock_dir, "華南銀行信用卡對帳單_202511_mock.xls")
    hncb_html_content = """<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=big5">
</head>
<body>
<table border="1">
  <tr>
    <td>消費日</td>
    <td>入帳日</td>
    <td>消費明細</td>
    <td>國別</td>
    <td>幣別</td>
    <td>外幣金額</td>
    <td>新臺幣金額</td>
  </tr>
  <tr>
    <td></td>
    <td></td>
    <td>華南SNY卡************5678</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
  </tr>
  <tr>
    <td>114/11/02</td>
    <td>114/11/04</td>
    <td>自動加值－ 全家便利商店</td>
    <td>TW</td>
    <td>TWD</td>
    <td></td>
    <td>500</td>
  </tr>
  <tr>
    <td>114/11/05</td>
    <td>114/11/07</td>
    <td>統一超商</td>
    <td>TW</td>
    <td>TWD</td>
    <td></td>
    <td>100</td>
  </tr>
  <tr>
    <td>114/11/08</td>
    <td>114/11/10</td>
    <td>ｉｃａｓｈ ｐａｙ－統一超商實體門市</td>
    <td>TW</td>
    <td>TWD</td>
    <td></td>
    <td>120</td>
  </tr>
  <tr>
    <td>114/11/12</td>
    <td>114/11/14</td>
    <td>立即繳費</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
  </tr>
</table>
</body>
</html>
"""
    with open(hncb_filename, "w", encoding="big5") as f:
        f.write(hncb_html_content)
    print(f"Generated: {hncb_filename}")
    
    try:
        print("\n✅ 所有模擬脫敏帳單已成功生成！")
        print(f"📁 存放路徑: {mock_dir}")
    except UnicodeEncodeError:
        print("\n[OK] 所有模擬脫敏帳單已成功生成！")
        print(f"存放路徑: {mock_dir}")

    try:
        print("🚀 立即體驗 ETL 與分析流程：")
        print("   1. 確保 .env 中設定 ACTIVE_PROFILE=example_public")
        print("   2. 執行 'python main.py' 並選擇 [1F] 進行全量解析與入庫！")
    except UnicodeEncodeError:
        pass

    return {
        "cathay": cathay_filename,
        "esun": esun_filename,
        "ctbc": ctbc_filename,
        "hncb": hncb_filename
    }

    
if __name__ == "__main__":
    generate_mock_data()
