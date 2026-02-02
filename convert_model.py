import pandas as pd
import os

# ==========================================
# 0. 設定檔案路徑與對照
# ==========================================
# 真實檔案 (您自己用的，不公開)
REAL_SOURCE_FILE = '信用卡消費資料模型.xlsx'

# 範本檔案 (公開給別人的，內容為假資料)
EXAMPLE_SOURCE_FILE = 'configs/信用卡消費資料模型_範本.xlsx' # 建議將範本放在 configs 或根目錄

OUTPUT_DIR = 'configs'

SHEET_MAPPING = {
    '卡號對照表(mapping table)': 'card_mapping.csv',
    '支付前綴關鍵字表(Regex)': 'payment_regex_rules.csv',
    '消費明細關鍵字表(Regex)': 'merchant_regex_rules.csv',
    '一般消費判斷和消費明細標籤(維度表)': 'merchant_categories.csv',
    '信用卡權益回饋紀錄(維度表)': 'reward_rates.csv',
    '信用卡權益通路認列(橋接表)': 'reward_bridges.csv',
    '外幣匯率紀錄(維度表)': 'exchange_rates.csv'
}

def get_source_file():
    """
    智慧判斷：優先讀取真實檔案，若無則讀取範本檔案。
    """
    if os.path.exists(REAL_SOURCE_FILE):
        print(f"🕵️ 偵測到真實設定檔: {REAL_SOURCE_FILE}")
        return REAL_SOURCE_FILE
    elif os.path.exists(EXAMPLE_SOURCE_FILE):
        print(f"⚠️ 找不到真實設定檔，改為讀取範本: {EXAMPLE_SOURCE_FILE}")
        return EXAMPLE_SOURCE_FILE
    else:
        return None

def main():
    # 1. 決定來源檔案
    source_excel = get_source_file()
    
    if not source_excel:
        print(f"❌ 錯誤: 找不到來源檔案！")
        print(f"   請確保 '{REAL_SOURCE_FILE}' 或 '{EXAMPLE_SOURCE_FILE}' 存在。")
        return

    # 2. 確保輸出目錄存在
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"📁 建立資料夾: {OUTPUT_DIR}/")

    print(f"📖 開始讀取 Excel: {source_excel} ...")
    
    try:
        # 讀取 Excel 所有工作表
        xls = pd.read_excel(source_excel, sheet_name=None, dtype=str)
        
        success_count = 0
        for sheet_name, output_name in SHEET_MAPPING.items():
            if sheet_name in xls:
                df = xls[sheet_name]
                
                # 清除全空的欄或列
                df = df.dropna(how='all', axis=0).dropna(how='all', axis=1)
                
                output_path = os.path.join(OUTPUT_DIR, output_name)
                df.to_csv(output_path, index=False, encoding='utf-8-sig')
                
                print(f"  ✅ [輸出] {output_name:<25} (來源: {sheet_name}, {len(df)} 筆)")
                success_count += 1
            else:
                # 這裡改用灰色或黃色提示，避免使用者以為是嚴重錯誤
                print(f"  ⚠️ [跳過] 找不到工作表: {sheet_name}")
                
        print(f"\n🎉 轉檔完成！共產生 {success_count} 個 CSV 設定檔。")
        
    except Exception as e:
        print(f"❌ 發生錯誤: {e}")

if __name__ == "__main__":
    main()