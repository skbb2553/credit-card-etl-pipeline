import pandas as pd
import yaml
import re
import io
import os
from bs4 import BeautifulSoup

# =======================================================
# 0. 全域常數定義 (Global Constants)
# =======================================================
COL_TXN_DATE = 'Transaction_Date'
COL_POST_DATE = 'Posting_Date'
COL_MERCHANT = 'Merchant'
COL_LOCATION = 'Merchant_Location'
COL_CONSUMPTION_PLACE = 'Consumption_Place'     # 玉山國外交易拆解用
COL_CONV_DATE = 'Conversion_Date'
COL_CURRENCY = 'Currency_Type'
COL_AMOUNT = 'Amount'
COL_CURR_AMOUNT = 'Currency_Amount'
COL_PAY_AMOUNT = 'Payment_Amount'               # 台幣應繳金額
COL_PAY_CURR = 'Payment_Currency'
COL_CARD_NO = 'Card_No'
COL_CARD_TYPE = 'Card_Type'
COL_TXN_TYPE = 'Transaction_Type'
COL_MOBILE_PAY = 'Mobile_Payment'
COL_BANK_NAME = 'Bank_Name'
COL_RAW_COUNTRY_CURR = 'Raw_Country_Currency'   # 國泰用

# =======================================================
# Part 1: Shared Utilities (工具函式)
# =======================================================
def load_config(config_path):
    with open(config_path, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)

def normalize_country_code(code):
    # ==========================================
    # Debug 區域：針對特定關鍵字進行詳細監控 (可視情況關閉)
    # ==========================================
    debug_mode = 'JPN' in str(code) 
    
    if debug_mode:
        print(f"   🕵️ [Trace] 進入函式 Input: '{code}' (Type: {type(code)})")

    # 1. 基礎防呆
    if pd.isna(code) or code is None:
        if debug_mode: print("   🕵️ [Trace] -> 判定為 None/NaN -> Return 'TW'")
        return 'TW'
    
    s_code = str(code)
    stripped_code = s_code.strip()
    is_empty = (stripped_code == '')
    
    if debug_mode:
        print(f"   🕵️ [Trace] Strip Check: 原字串 '{s_code}' -> 去空白後 '{stripped_code}'")
        print(f"   🕵️ [Trace] Is Empty? {is_empty}")

    if is_empty:
        if debug_mode: print("   🕵️ [Trace] -> 判定為空字串 -> Return 'TW'")
        return 'TW'

    # 2. 前置清洗 (核心邏輯：取首字)
    # "JPN CHIYODA-KU" -> "JPN"
    clean_code = stripped_code.upper().split(' ')[0]
    
    if debug_mode:
        print(f"   🕵️ [Trace] Split Logic: '{stripped_code}' -> 取首字 -> '{clean_code}'")

    # 3. 3碼轉2碼對照表
    mapping_3to2 = {
        'TWN': 'TW', 'USA': 'US', 'JPN': 'JP', 'KOR': 'KR',
        'HKG': 'HK', 'SGP': 'SG', 'GBR': 'GB', 'CHN': 'CN',
        'IRL': 'IE', 'DEU': 'DE', 'FRA': 'FR', 'AUS': 'AU',
        'VNM': 'VN', 'THA': 'TH', 'MYS': 'MY', 'IDN': 'ID'
    }
    
    # 4. 查表與回傳
    result = clean_code
    if clean_code in mapping_3to2:
        result = mapping_3to2[clean_code]
    elif len(clean_code) == 2:
        result = clean_code
    
    if debug_mode:
        print(f"   🕵️ [Trace] Final Result: '{result}'\n")

    return result

def parse_date_with_year(date_str, base_year, bill_month):
    s = str(date_str).strip()
    if pd.isna(date_str) or s in ['(null)', 'nan', '']:
        return pd.NaT
    try:
        parts = re.split(r'[/-]', s)
        # 月/日 (01/15) -> 補年份 + 跨年邏輯
        if len(parts) == 2:
            month = int(parts[0])
            day = int(parts[1])
            final_year = base_year
            # 邏輯：帳單是1月，但出現12月消費 -> 肯定是去年
            if bill_month == 1 and month == 12: final_year -= 1
            # 邏輯：帳單是12月，但出現1月消費 -> 肯定是明年 (極少見但防禦)
            if bill_month == 12 and month == 1: final_year += 1
            return pd.Timestamp(year=final_year, month=month, day=day)
        # 已有年份 (2024/01/15)
        elif len(parts) == 3:
            return pd.to_datetime(s, errors='coerce')
        else:
            return pd.NaT
    except Exception:
        return pd.NaT

# =======================================================
# Part 2: Nodes (處理節點)
# =======================================================

# [Node 1] 智慧讀取器 (Smart Ingest)
def smart_read_csv(filepath, encoding, header_keyword):
    content_buffer = ""
    found_header = False
    try:
        with open(filepath, 'r', encoding=encoding, errors='replace') as f:
            all_lines = f.readlines()
        
        # 掃描前 50 行找標題 (動態錨點)
        for i, line in enumerate(all_lines):
            if i > 50: break
            if header_keyword and header_keyword in line:
                content_buffer = "".join(all_lines[i:])
                found_header = True
                print(f"   📍 標題定位成功：第 {i+1} 行")
                break
        
        if found_header:
            return pd.read_csv(io.StringIO(content_buffer), on_bad_lines='skip')
        else:
            print("   ⚠️ 未偵測到標題關鍵字，嘗試直接讀取...")
            return pd.read_csv(filepath, encoding=encoding, header=0, on_bad_lines='skip')
    except Exception as e:
        print(f"   ❌ Smart Read 失敗: {e}")
        return None

# [Node 3] 卡號提取 (Feature Extraction - 通用版)
def extract_card_info(df, bank_id, col_merchant, col_card_no, col_card_type):
    target_banks = ['esun_bank', 'hncb_bank']
    if bank_id not in target_banks or col_merchant not in df.columns:
        return df

    # 配置與邏輯分離
    patterns = {
        'esun_bank': {
            'trigger': '卡號：',# 玉山特徵：卡號：XXXX-XXXX-XXXX-"NNNN"（"玉山卡別"－正卡）
            'card_no': r'(\d{4})（',
            'card_type': r'（(.*?)－?(?:正卡|附卡)）'
        },
        'hncb_bank': {
            'trigger': r'\*{12}', # 華南特徵："華南卡別"************"NNNN"
            'card_no': r'\*{12}(\d{4})',
            'card_type': r'^(.*?)\*{12}' # 星號前面的就是卡別
        }
    }
    config = patterns.get(bank_id)
    
    # 執行 Tag -> Fill -> Extract
    mask_master = df[col_merchant].astype(str).str.contains(config['trigger'], na=False, regex=True)
    if mask_master.any():
        print(f"   🔧 [{bank_id}] 執行卡號提取 (Group & Fill)...")
        df.loc[mask_master, 'raw_master_info'] = df.loc[mask_master, col_merchant]
        df['raw_master_info'] = df['raw_master_info'].ffill()
        
        df[col_card_no] = df['raw_master_info'].str.extract(config['card_no'])
        if config.get('card_type'):
            df[col_card_type] = df['raw_master_info'].str.extract(config['card_type'])
            
        df = df[~mask_master].copy() # 刪除 Master 行
        df = df.drop(columns=['raw_master_info'])
        
    return df

# [Node 4-1] 玉山專屬解析
def parse_esun_details(df, col_merchant, col_location, col_conv_date, base_year, bill_month):
    if col_merchant not in df.columns: return df
    print("   🔧 [玉山] 執行國外交易資料拆解 (消費地與日期)...")
    
    df[col_merchant] = df[col_merchant].astype(str).str.strip()
    
    # Regex 更新: 寬容模式，適應 2 碼或多碼地點
    # 結構: (消費明細) (分隔:2空白或Tab) (消費地:國別+地點) (分隔:至少1空白)+(日期：折算日MM/DD)?
    pat = r'^(.*?)(?:\s{2,}|\t)(.*?)(?:\s+(\d{2}/\d{2}))?$'
    
    ext = df[col_merchant].str.extract(pat)
    
    # [Debug] 手術台視角：檢查切分結果
    debug_mask = ext[1].notna()
    if debug_mask.any():
        print("\n   🔍 [Debug] 玉山國外交易拆解預覽 (前 5 筆):")
        debug_view = pd.DataFrame({
            '原始字串': df.loc[debug_mask, col_merchant],
            'G1_商店名': ext.loc[debug_mask, 0],
            'G2_消費地': ext.loc[debug_mask, 1],
            'G3_日期':   ext.loc[debug_mask, 2]
        })
    #    print(debug_view.head().to_string())
        print("-" * 60)

    # 回填資料
    # 1. 修正商店名稱
    has_name = ext[0].notna()
    df.loc[has_name, col_merchant] = ext[0].str.strip() 
    
    # 2. 填入消費地 (Consumption Place)
    df.loc[ext[1].notna(), col_location] = ext[1].str.strip()
    
    # 3. 填入換匯日期
    df.loc[ext[2].notna(), col_conv_date] = ext[2]
    
    return df

# [Node 4-2] 國泰專屬解析
def parse_cube_details(df, col_raw, col_location, col_currency):
    if col_raw in df.columns:
        print("   🔧 [國泰] 執行 消費國家(TW)/幣別(TWD) 拆解...")
        split = df[col_raw].astype(str).str.split(' / ', n=1, expand=True)
        if split.shape[1] >= 1:
            df[col_location] = split[0].str.strip().apply(normalize_country_code)
        if split.shape[1] >= 2:
            df[col_currency] = split[1].str.strip()
        df = df.drop(columns=[col_raw])
    return df

# =======================================================
# Part 3: Main Pipeline (主流程)
# =======================================================
def process_bank_file(filepath, bank_id, config):
    print(f"正在處理：{bank_id} ({os.path.basename(filepath)})...") 
    bank_config = config.get(bank_id)
    if bank_config is None:
        print(f"❌ 錯誤：找不到 {bank_id} 設定")
        return None

    # 0. 環境變數設定
    current_encoding = bank_config.get('encoding', 'utf-8')
    header_keyword = bank_config.get('header_keyword')
    file_type = bank_config.get('file_type', 'csv')

    # 1. Init: 解析檔名以獲取年份 (核心依賴)
    base_year = 2024; bill_month = 1 
    filename = os.path.basename(filepath)
    match_western = re.search(r'(20\d{2})(\d{2})', filename)
    match_roc = re.search(r'(\d{2,3})年(\d{1,2})月', filename)
    
    if match_roc:
        base_year = int(match_roc.group(1)) + 1911
        bill_month = int(match_roc.group(2))
    elif match_western:
        base_year = int(match_western.group(1))
        bill_month = int(match_western.group(2))

    df = None

    # =======================================================
    # Node 1: Ingest (讀取)
    # =======================================================
    # [Path A] 華南 HTML (特殊處理)
    if bank_id == 'hncb_bank':
        try:
            with open(filepath, 'r', encoding=current_encoding, errors='replace') as f:
                soup = BeautifulSoup(f, 'lxml')
            header_node = soup.find(string=lambda t: t and header_keyword in t)
            if header_node and header_node.find_parent('table'):
                target_table = header_node.find_parent('table')
                dfs = pd.read_html(io.StringIO(str(target_table)), header=0)
                if dfs:
                    df = dfs[0]
                    # HTML 清洗：壓扁換行符號
                    df.columns = [" ".join(str(c).replace('\n', ' ').split()) for c in df.columns]
                    print("   ✅ HTML 表格解析成功")
        except Exception as e:
            print(f"   ❌ 華南 HTML 處理失敗: {e}")
            return None

    # [Path B] 通用 CSV/Text (包含玉山、國泰、中信)
    elif file_type == 'csv' or bank_id == 'esun_bank':
        df = smart_read_csv(filepath, current_encoding, header_keyword)
    
    # [Path C] 真 Excel
    elif file_type == 'excel':
        try:
            df = pd.read_excel(filepath) 
        except Exception as e:
             print(f"❌ Excel 讀取失敗: {e}")

    if df is None or df.empty: return None
    df.columns = df.columns.astype(str).str.strip()

    # =======================================================
    # Node 2: Mapping (欄位映射)
    # =======================================================
    mapping = bank_config.get('columns_mapping', {})
    existing_cols = df.columns.tolist()
    available_cols = [c for c in mapping.keys() if c in existing_cols]
    if available_cols:
        df = df[available_cols]
        df = df.rename(columns=mapping)
    df[COL_BANK_NAME] = bank_id

    # 初始化必要欄位
    for col in [COL_LOCATION, COL_CURRENCY, COL_CONV_DATE, COL_CARD_NO, COL_CARD_TYPE, 
                COL_PAY_AMOUNT, COL_PAY_CURR, COL_CURR_AMOUNT]:
        if col not in df.columns: df[col] = None

    # =======================================================
    # Node 3: Extraction (特徵提取)
    # =======================================================
    df = extract_card_info(df, bank_id, COL_MERCHANT, COL_CARD_NO, COL_CARD_TYPE)

    # =======================================================
    # Node 4: Specific (銀行專屬清洗)
    # =======================================================
    if bank_id == 'esun_bank':
        df = parse_esun_details(df, COL_MERCHANT, COL_CONSUMPTION_PLACE, COL_CONV_DATE, base_year, bill_month)
        
        # [Node 4 搬運工] 將 Node 4-1 抓到的消費地資訊填入 location 欄位
        if COL_CONSUMPTION_PLACE in df.columns:
            raw_places = df.loc[df[COL_CONSUMPTION_PLACE].notna(), COL_CONSUMPTION_PLACE].unique()
            if len(raw_places) > 0:
                print(f"   🔍 [Debug Node 4-1 後] 抓到的消費地 (consumption_place): {raw_places}")
        
        if COL_CONSUMPTION_PLACE in df.columns and COL_LOCATION in df.columns:
            print("   🔧 [玉山] 將消費地資訊填入 location_country 欄位...")
            mask_has_place = df[COL_CONSUMPTION_PLACE].notna()
            df.loc[mask_has_place, COL_LOCATION] = df.loc[mask_has_place, COL_CONSUMPTION_PLACE]

    elif bank_id == 'cube_bank':
        df = parse_cube_details(df, COL_RAW_COUNTRY_CURR, COL_LOCATION, COL_CURRENCY)

    elif bank_id == 'ctbc_bank':
        if COL_LOCATION in df.columns:
            df[COL_LOCATION] = df[COL_LOCATION].fillna('TW')
        df[COL_CURRENCY] = df[COL_CURRENCY].fillna('TWD')
        
    elif bank_id == 'hncb_bank':
        if COL_LOCATION in df.columns:
            df[COL_LOCATION] = df[COL_LOCATION].fillna('TW')

    print(f"\n   🔍 [Debug] 檔名偵測到的 Base Year: {base_year}, Month: {bill_month}")
    
    if COL_TXN_DATE in df.columns:
        print(f"   🔍 [Debug] 原始交易日期範例 (前5筆):")
        print(df[COL_TXN_DATE].head().tolist()) 
        first_date = df[COL_TXN_DATE].iloc[0] if not df.empty else "No Data"
        parsed_result = parse_date_with_year(first_date, base_year, bill_month)
        print(f"   🔍 [Debug] 試轉第一筆: '{first_date}' -> {parsed_result}")
    else:
        print(f"   ❌ [Critical] 居然沒有 {COL_TXN_DATE} 欄位？前面 Mapping 不是說有嗎？")

    # =======================================================
    # Node 5: General (通用清洗與防呆)
    # =======================================================
    
    # 1. 卡號清理
    if COL_CARD_NO in df.columns:
        df[COL_CARD_NO] = df[COL_CARD_NO].astype(str).str.replace(r'\.0$', '', regex=True)
        df[COL_CARD_NO] = df[COL_CARD_NO].replace({'nan': None, 'NaN': None, '': None})

    # 2. 日期解析
    for col in [COL_TXN_DATE, COL_POST_DATE, COL_CONV_DATE]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: parse_date_with_year(x, base_year, bill_month))
            if col == COL_TXN_DATE: df = df.dropna(subset=[col])

    # 3. 金額清洗
    for col in [COL_AMOUNT, COL_PAY_AMOUNT, COL_CURR_AMOUNT]:
        if col in df.columns:
            s = df[col].astype(str).str.strip().str.replace(',', '')
            df[col] = pd.to_numeric(s, errors='coerce')

    # 4. 繳款金額/幣別補完
    if COL_PAY_AMOUNT in df.columns and COL_AMOUNT in df.columns:
        df[COL_PAY_AMOUNT] = df[COL_PAY_AMOUNT].fillna(df[COL_AMOUNT])
    if COL_PAY_CURR in df.columns:
        df[COL_PAY_CURR] = df[COL_PAY_CURR].fillna('TWD')

    # 5. 地點與幣別標準化
    if COL_LOCATION in df.columns:
        # [Debug] Node 5 監控
        raw_locs = df.loc[df[COL_LOCATION].notna(), COL_LOCATION].unique()
        if len(raw_locs) > 0:
            print(f"   🔍 [Debug Node 5] 正規化前 Location (Unique): {raw_locs}")
            
        df[COL_LOCATION] = df[COL_LOCATION].apply(normalize_country_code)

        # [Debug] Node 5 監控
        norm_locs = df.loc[df[COL_LOCATION].notna(), COL_LOCATION].unique()
        if len(norm_locs) > 0:
            print(f"   🔍 [Debug Node 5] 正規化後 Location (Unique): {norm_locs}")

    # 6. 國內交易清理
    mask_domestic = df[COL_LOCATION] == 'TW'
    df.loc[mask_domestic, COL_CURRENCY] = None    # 國內不需標示幣別
    df.loc[mask_domestic, COL_CURR_AMOUNT] = None # 國內不需標示外幣金額

    # 7. 國外交易預設 TWD
    mask_foreign_empty = (df[COL_LOCATION] != 'TW') & df[COL_CURRENCY].isna()
    if mask_foreign_empty.any():
        df.loc[mask_foreign_empty, COL_CURRENCY] = 'TWD'

    return df

# =======================================================
# Part 4: Main Execution (主執行區)
# =======================================================
if __name__ == "__main__":
    config_path = 'configs/banks_config.yaml'
    if not os.path.exists(config_path):
        print(f"❌ 錯誤：找不到設定檔 {config_path}")
        exit()
        
    config = load_config(config_path)
    
    bank_keyword_map = {
        '玉山': 'esun_bank',
        '國泰': 'cube_bank', '國泰世華': 'cube_bank',
        '中信': 'ctbc_bank', '中國信託': 'ctbc_bank',
        '華南': 'hncb_bank',
        '永豐': 'sinopac_bank', 'DAWAY': 'sinopac_bank'
    }
    data_folder = 'data'
    all_data = []

    print(f"📂 掃描目錄: {data_folder}")
    if os.path.exists(data_folder):
        file_list = os.listdir(data_folder)
        for filename in file_list:
            if filename.startswith('.') or not re.search(r'\.(csv|xlsx|xls|html)$', filename, re.I):
                continue
            
            detected_bank_id = None
            for keyword, bank_id in bank_keyword_map.items():
                if keyword in filename:
                    detected_bank_id = bank_id
                    break
            
            if detected_bank_id:
                full_path = os.path.join(data_folder, filename)
                cleaned_df = process_bank_file(full_path, detected_bank_id, config)
                if cleaned_df is not None:
                    all_data.append(cleaned_df)
            else:
                 print(f"⚠️  略過 (未匹配銀行): {filename}")

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        
        desired_cols = [
            COL_TXN_DATE, COL_POST_DATE, COL_MERCHANT, COL_LOCATION, COL_CONSUMPTION_PLACE,
            COL_CURRENCY, COL_CONV_DATE, COL_AMOUNT, COL_CURR_AMOUNT, 
            COL_PAY_AMOUNT, COL_PAY_CURR, 
            COL_TXN_TYPE, COL_MOBILE_PAY, COL_CARD_TYPE, COL_CARD_NO, COL_BANK_NAME
        ]

        final_cols = [c for c in desired_cols if c in final_df.columns]

        print("\n=== 結果預覽 ===")
        print(final_df[final_cols].head())
        
        output_path = "data/result_all_banks.csv"
        final_df[final_cols].to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"\n✅ 處理完成，結果已輸出至: {output_path}")
    else:
        print("無資料產出")