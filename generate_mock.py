import numpy as np
import pandas as pd
import os
import shutil
import random
import refine  


# ... 其他 import


try:
    from Himitsu import CUSTOM_CARD_MAP
    print("🔐 已載入 Himitsu.py 指定的卡號映射。")
except ImportError:
    print("⚠️ 找不到 Himitsu.py，將使用全自動隨機卡號。")
    CUSTOM_CARD_MAP = {} # 若沒有檔案，就保持空字典，程式會走自動遞增邏輯


# 尚未定義的卡號，會從這個數字開始自動遞增
AUTO_INCREMENT_START = 1000


# ==========================================
# 配置區
# ==========================================
SOURCE_FILE = 'data/result_all_banks.csv'
OUTPUT_DIR = 'examples'  # 輸出到 examples 資料夾供 GitHub 使用
SAMPLE_SIZE = 30         # 範本要幾筆資料

# 為了展示 Regex 能力，我們希望保留特定的髒資料
# 這裡可以用關鍵字強制保留某些有趣的案例
INTERESTING_KEYWORDS = [
    '連加', 'Line', '統一超商', '全家', 'UBER', 'NETFLIX', 
    'Steam', '蝦皮', 'foodpanda', '繳款'
]


def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

# ==========================================
# 1. 智慧採樣 (Smart Sampling)
# ==========================================
def smart_sample(df, n=20):
    """
    不只是隨機，而是優先挑選「看起來很髒」或「有代表性」的資料
    """
    pool = []
    
    # 1. 關鍵字命中採樣 (確保展示案例包含各種支付場景)
    for kw in INTERESTING_KEYWORDS:
        mask = df['Merchant'].astype(str).str.contains(kw, case=False, na=False)
        if mask.any():
            # 每個關鍵字抽 1-2 筆
            sample = df[mask].sample(min(len(df[mask]), 2))
            pool.append(sample)
    
    # 2. 隨機補足剩餘數量
    current_count = sum([len(x) for x in pool])
    if current_count < n:
        remaining = n - current_count
        pool.append(df.sample(remaining))
        
    sampled_df = pd.concat(pool).drop_duplicates().reset_index(drop=True)
    return sampled_df

# ==========================================
# 2. 去敏引擎 (Masking Engine)
# ==========================================
def anonymize_data(df):
    """
    執行非對稱去敏：保留格式真實性，但數值與個資造假
    """
    print(">>> 執行去敏化處理...")
    
    # A. 日期平移 (全部移到 2023 年，保持相對間隔)
    # 找出資料中的最大日期，算出與 2023-12-31 的差值，進行全體平移
    if 'Transaction_Date' in df.columns:
        dates = pd.to_datetime(df['Transaction_Date'], errors='coerce')
        valid_dates = dates.dropna()
        if not valid_dates.empty:
            max_date = valid_dates.max()
            # 讓最新一筆資料變成 '2023-12-25' (虛構過去時間)
            target_date = pd.to_datetime('2023-12-25')
            delta = target_date - max_date
            
            # 套用平移
            for col in ['Transaction_Date', 'Posting_Date', 'Conversion_Date']:
                if col in df.columns:
                    # 轉 datetime -> 平移 -> 轉回字串 (YYYY-MM-DD)
                    dt_series = pd.to_datetime(df[col], errors='coerce')
                    df[col] = (dt_series + delta).dt.strftime('%Y-%m-%d')
    
    # B. 金額擾動 (Noise Injection)
    # 邏輯：金額 * (0.9 ~ 1.1 的隨機數)，並取整數或小數點後兩位
    numeric_cols = ['Amount', 'Currency_Amount', 'Payment_Amount']
    for col in numeric_cols:
        if col in df.columns:
            # 產生隨機雜訊 mask (e.g., 0.95 ~ 1.05)
            noise = np.random.uniform(0.95, 1.05, size=len(df))
            
            # 處理原始數據 (防呆：轉數值)
            val = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            # 擾動後，若是台幣通常為整數，外幣保留小數
            if 'Currency' in col or 'Amount' in col: 
                # 簡單判斷：若原欄位看起來像整數，就轉整數
                is_integer_col = (val % 1 == 0).all()
                new_val = val * noise
                if is_integer_col:
                    df[col] = new_val.round(0).astype(int)
                else:
                    df[col] = new_val.round(2)

# C. 敏感個資覆蓋 (Masking) - 支援指定卡號
    if 'Card_No' in df.columns:
        # 1. 取得資料中所有的真實卡號 (Unique)
        # 注意：有些資料可能是 None/NaN，要濾掉
        real_cards = df['Card_No'].dropna().unique()
        
        fake_map = {}
        auto_counter = 0
        
        for real_card in real_cards:
            real_card_str = str(real_card).strip()
            # 嘗試從真實卡號中提取末四碼 (假設格式可能包含 - 或 *)
            # 這裡假設 etl.py 產出的 Card_No 已經是乾淨的 '1234' 或 '****1234'
            # 我們直接取最後 4 碼作為 Key
            real_suffix = real_card_str[-4:]
            
            # [核心邏輯] 判斷是否在使用者定義的清單中
            if real_suffix in CUSTOM_CARD_MAP:
                target_suffix = CUSTOM_CARD_MAP[real_suffix]
            else:
                # 若沒定義，就自動產生 (例如 1000, 2000...)
                auto_counter += 1
                target_suffix = str(AUTO_INCREMENT_START * auto_counter)
            
            # 建立完整的假卡號字串
            fake_map[real_card] = f"****-****-****-{target_suffix}"
            
        # 套用 Mapping
        df['Card_No'] = df['Card_No'].map(fake_map).fillna(df['Card_No'])
        
        # [重要] 這裡要回傳 fake_map，因為 generate_dummy_configs 需要用它來寫入 CSV
        return df, fake_map

    return df, {}

# ==========================================
# 3. 設定檔淨化 (Config Sanitizer)
# ==========================================
def generate_dummy_configs(real_config_dir, output_config_dir, card_map):
    print(f">>> 生成範例設定檔至 {output_config_dir}...")
    ensure_dir(output_config_dir)
    
    # --- [新增] 台灣熱門神卡池 (Flavor Text) ---
    # 讓 Mock Data 看起來像真的一樣，包含常見的行動支付設定
    POPULAR_CARDS_POOL = [
        {'name': '國泰CUBE卡', 'mobile': 'ApplePay', 'prefix': 'ApplePay－', 'note': '權益切換卡'},
        {'name': '玉山Unicard', 'mobile': 'LinePay', 'prefix': 'LinePay－', 'note': '自選權益卡'},
        {'name': '玉山Ubear卡', 'mobile': '', 'prefix': '', 'note': '無腦網購卡'},
        {'name': '台新Richart卡', 'mobile': 'SamsungPay', 'prefix': 'SamsungPay－', 'note': '七選一無上限回饋'},
        {'name': '富邦J卡', 'mobile': '', 'prefix': '', 'note': '日韓旅遊卡'},
        {'name': '聯邦吉鶴卡', 'mobile': 'ApplePay', 'prefix': 'ApplePay－', 'note': '日韓旅遊卡'},
        {'name': '永豐DAWHO現金回饋信用卡', 'mobile': 'GooglePay', 'prefix': 'GooglePay－', 'note': '現金無腦回饋卡'},
        {'name': '中信Uniopen聯名卡', 'mobile': '', 'prefix': '', 'note': '統一集團生態圈聯名卡'},
        {'name': '中信LINE Pay信用卡', 'mobile': 'LinePay', 'prefix': 'LinePay－', 'note': 'LinePay聯名卡'},
    ]
    
    # 1. 處理 Card Mapping
    real_card_map_path = os.path.join(real_config_dir, refine.FILE_CARDS)
    if os.path.exists(real_card_map_path):
        df_real_map = pd.read_csv(real_card_map_path, dtype=str)
        dummy_rows = []
        processed_suffixes = set()
        
        # 為了避免同一張神卡被重複使用，我們打亂池子
        random.shuffle(POPULAR_CARDS_POOL)
        pool_index = 0

        for idx, row in df_real_map.iterrows():
            real_raw = str(row['卡號']).strip()
            if not real_raw: continue
            
            real_suffix = real_raw[-4:]
            
            # 只處理有被 Himitsu 定義，或是本次有抽樣到的卡
            fake_suffix = "0000"
            if real_suffix in CUSTOM_CARD_MAP:
                fake_suffix = CUSTOM_CARD_MAP[real_suffix]
            else:
                # 這裡看你要不要把所有真實卡表都列出來，
                # 為了展示豐富度，建議可以隨機把沒定義的也放進來 (只要不洩漏真卡號)
                continue 

            if fake_suffix in processed_suffixes: continue
            
            # --- [核心修改] 從神卡池抽一個身份 ---
            if pool_index < len(POPULAR_CARDS_POOL):
                flavor = POPULAR_CARDS_POOL[pool_index]
                pool_index += 1
            else:
                # 萬一卡片太多，池子用完了，就用通用名稱
                flavor = {'name': f'通用回饋卡_{pool_index}', 'mobile': '', 'prefix': '', 'note': 'Auto Gen'}
                pool_index += 1

            # 建立 Mock Row
            new_row = row.copy()
            new_row['對應卡片'] = flavor['name']          # 替換成神卡名稱
            new_row['卡號'] = f"**** {fake_suffix}"       # 假號碼
            new_row['行動支付標籤'] = flavor['mobile']     # 套用該神卡的常見設定
            new_row['加在消費明細摘要前方'] = flavor['prefix']
            new_row['卡號代換'] = fake_suffix
            new_row['備註'] = f"[Mock] {flavor['note']}"  # 標註這是模擬資料
            
            dummy_rows.append(new_row)
            processed_suffixes.add(fake_suffix)

        # 防呆：如果都沒資料 (例如沒設定 Himitsu)，至少生兩筆給人家看
        while not dummy_rows and pool_index < 2:
             flavor = POPULAR_CARDS_POOL[pool_index]
             new_row = {
                 '對應卡片': flavor['name'],
                 '卡號': f"**** {1000 + pool_index}",
                 '行動支付標籤': flavor['mobile'],
                 '加在消費明細摘要前方': flavor['prefix'],
                 '卡號代換': str(1000 + pool_index),
                 '備註': '[Mock] Auto Generated Demo'
             }
             dummy_rows.append(new_row)
             pool_index += 1

        # 寫入檔案
        pd.DataFrame(dummy_rows).to_csv(
            os.path.join(output_config_dir, refine.FILE_CARDS), 
            index=False, encoding='utf-8-sig'
        )

    # 2. 複製其他設定檔
    # 這些通常不含個資，可以直接複製，或過濾掉 Priority 低的私有規則
    files_to_copy = [
        refine.FILE_CHANNELS,          # payment_gateway.csv
        refine.FILE_EXCLUDED_TYPES,        # transaction_types.yaml
        refine.FILE_EXAMPLE_MERCHANTS, # example_merchants.csv

    ]
    
    # 如果使用者本地只有真實檔，沒有範本檔 (雖然照你的計畫是會有)，我們可以做個 fallback
    if not os.path.exists(os.path.join(real_config_dir, 'example_merchant_regex_rules.csv')):
         # 如果沒有範本，只好暫時拿真實檔 (這行視你的資安潔癖程度決定要不要加)
         # files_to_copy.append('merchant_regex_rules.csv')
         pass
    
    for f in files_to_copy:
        src = os.path.join(real_config_dir, f)
        if os.path.exists(src):
            # 如果是範本檔 (example_merchants.csv)，複製過去時要改回正式名稱 (merchants.csv)
            # 這樣 Mock 環境的程式才能讀到
            dst_filename = f
            if f == refine.FILE_EXAMPLE_MERCHANTS:
                dst_filename = refine.FILE_MERCHANTS
                
            shutil.copy(src, os.path.join(output_config_dir, dst_filename))
            print(f"  - Copied: {f} -> {dst_filename}")

# ==========================================
# 主程式
# ==========================================
def main():
    ensure_dir(OUTPUT_DIR)
    
    # 1. 讀取真實資料
    if not os.path.exists(SOURCE_FILE):
        print("❌ 找不到來源資料，請先執行 etl.py")
        return

    print(f"1. 讀取資料: {SOURCE_FILE}")
    df_raw = pd.read_csv(SOURCE_FILE, dtype=str) # 全部讀為字串以免型態跑掉
    
    # 2. 採樣
    print("2. 智慧採樣...")
    df_sample = smart_sample(df_raw, n=SAMPLE_SIZE)
    
    # 3. 去敏
    print("3. 去敏化...")
    df_masked, card_map = anonymize_data(df_sample.copy())
    
    # 輸出 Raw Example
    raw_out_path = os.path.join(OUTPUT_DIR, 'example_raw_data.csv')
    df_masked.to_csv(raw_out_path, index=False, encoding='utf-8-sig')
    print(f"✅ 已輸出 Raw Example: {raw_out_path}")
    
    # 4. 準備 Mock Configs (為了讓 refine.py 可以跑)
    # 我們在 examples/configs 建立一套假的設定檔
    mock_config_dir = os.path.join(OUTPUT_DIR, 'configs')
    generate_dummy_configs('configs', mock_config_dir, card_map)
    
    # 5. 呼叫 Refine 邏輯生成對照組
    print("4. 執行 Refine (使用 Mock Configs)...")
    
    # 這裡有個技巧：我們暫時欺騙 refine.py 關於 Config 的路徑
    # 或者我們直接傳入 dataframe 給 refine 的函式處理 (因為 refine.py 寫得很模組化!)
    
    # 載入剛剛生成的 Mock Configs
    mock_payment_rules = refine.load_payment_rules(mock_config_dir)
    mock_merchant_rules = refine.load_merchant_regex_rules(mock_config_dir)
    mock_mapping_config = refine.load_yaml_config(os.path.join(mock_config_dir, 'mapping_rules.yaml'))
    
    # 開始串接 Refine 流程 (複製 refine.main 的邏輯，但改用變數傳遞)
    df_refined = df_masked.copy()
    
    # 型態轉換 (參考 refine.py)
    for col in ['Currency_Amount', 'Payment_Amount']:
        if col in df_refined.columns:
            df_refined[col] = pd.to_numeric(df_refined[col], errors='coerce')
            
    # 執行各階段清洗
    # 注意：因為我們生成的 dummy card mapping 可能對不上這裡的假卡號，
    # 所以 apply_card_mapping 效果可能有限，這反而是好事 (展示未歸戶狀態 vs 已歸戶)
    df_refined = refine.apply_card_mapping(df_refined, config_dir=mock_config_dir)
    df_refined = refine.cleanup_cathay_remaining(df_refined)
    df_refined = refine.identify_third_party_payment(df_refined, payment_rules=mock_payment_rules)
    df_refined = refine.process_esun_epoint(df_refined)
    df_refined = refine.clean_merchant_by_regex(df_refined, regex_rules=mock_merchant_rules)
    df_refined = refine.classify_transaction_type(df_refined, mock_mapping_config)
    df_refined = refine.apply_final_prefixes(df_refined)
    
    # 輸出 Refined Example
    refined_out_path = os.path.join(OUTPUT_DIR, 'example_refined_data.csv')
    df_refined.to_csv(refined_out_path, index=False, encoding='utf-8-sig')
    print(f"✅ 已輸出 Refined Example: {refined_out_path}")

    # 6. 生成 README 的 Markdown 表格 (Optional)
    # 這功能超實用，直接 print 出來讓你貼到 GitHub README
    print("\n=== GitHub README 表格預覽 ===")
    cols_to_show = ['Merchant', 'Merchant_Location', 'Transaction_Type', 'Payment_Amount']
    print("| 原始商家 (Raw) | 清洗後 (Refined) | 交易類型 | 金額 |")
    print("| :--- | :--- | :--- | ---: |")
    
    # 挑 5 筆展示
    comparison = pd.DataFrame({
        'Raw': df_masked['Merchant'],
        'Refined': df_refined['Merchant'],
        'Type': df_refined['Transaction_Type'],
        'Amt': df_refined['Payment_Amount']
    }).head(5)
    
    for _, row in comparison.iterrows():
        print(f"| `{str(row['Raw'])[:15]}...` | `{row['Refined']}` | {row['Type']} | {row['Amt']} |")

if __name__ == "__main__":
    main()