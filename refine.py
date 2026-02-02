import pandas as pd
import yaml
import os
import numpy as np
import warnings

# 忽略 Pandas 的 Regex Group 警告
warnings.filterwarnings("ignore", "This pattern is interpreted as a regular expression")

# ==========================================
# 0. 路徑與 Schema 配置
# ==========================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(BASE_DIR, 'configs')
DATA_DIR = os.path.join(BASE_DIR, 'data')

INPUT_FILE = os.path.join(DATA_DIR, 'result_all_banks.csv')
OUTPUT_FILE = os.path.join(DATA_DIR, 'refined_all_banks.csv')

# 載入 YAML (只剩關鍵字設定)
YAML_CONFIG_FILE = os.path.join(CONFIG_DIR, 'mapping_rules.yaml')

COLUMN_TYPES = {
    'Currency_Amount': float,  
    'Payment_Amount': float,   
    'Transaction_Date': str,
    'Posting_Date': str,
    'Conversion_Date': str, 
    'Card_No': str,
    'Bank_Name': str,
    'Card_Type': str,
    'Merchant': str,
    'Merchant_Location': str,
    'Consumption_Place': str,
    'Currency_Type': str,
    'Payment_Currency': str,
    'Transaction_Type': str,
    'Mobile_Payment': str,
}

# ==========================================
# 0. 規則引入
# ==========================================

def load_yaml_config(config_path):
    if not os.path.exists(config_path):
        return {}
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def load_payment_rules(config_dir):
    rule_path = os.path.join(config_dir, 'payment_regex_rules.csv') 
    if not os.path.exists(rule_path):
        print(f"⚠️ 警告: 找不到支付規則檔 {rule_path}")
        return []
    try:
        df_rules = pd.read_csv(rule_path, dtype=str)
        if 'Priority' in df_rules.columns:
            df_rules['Priority'] = pd.to_numeric(df_rules['Priority'], errors='coerce').fillna(999)
            df_rules = df_rules.sort_values(by='Priority', ascending=False)
        return df_rules.to_dict('records')
    except Exception as e:
        print(f"❌ 讀取支付規則失敗: {e}")
        return []

def load_merchant_regex_rules(config_dir):
    rule_path = os.path.join(config_dir, 'merchant_regex_rules.csv')
    if not os.path.exists(rule_path):
        print(f"⚠️ 警告: 找不到商家規則檔 {rule_path}")
        return []
    try:
        df_rules = pd.read_csv(rule_path, dtype=str)
        if 'Priority' in df_rules.columns:
            df_rules['Priority'] = pd.to_numeric(df_rules['Priority'], errors='coerce').fillna(999)
            df_rules = df_rules.sort_values(by='Priority', ascending=False)
        
        if 'Replacement' not in df_rules.columns:
            df_rules['Replacement'] = ''
        else:
            df_rules['Replacement'] = df_rules['Replacement'].fillna('')

        df_rules = df_rules[df_rules['Pattern'].notna() & (df_rules['Pattern'].str.strip() != '')]
        return df_rules.to_dict('records')
    except Exception as e:
        print(f"❌ 讀取商家規則失敗: {e}")
        return []

# ==========================================
# 1. 核心邏輯：卡號歸戶與清洗
# ==========================================

def apply_card_mapping(df, config_dir):
    print(">>> 執行邏輯: 讀取對照表進行卡號歸戶與標記...")
    mapping_file = os.path.join(config_dir, 'card_mapping.csv')
    if not os.path.exists(mapping_file):
        print(f"❌ 錯誤: 找不到 {mapping_file}")
        return df

    MAPPING_SCHEMA = {
        '對應卡片': str, '卡號': str, '行動支付標籤': str,
        '加在消費明細摘要前方': str, '卡號代換': str
    }

    try:
        rules = pd.read_csv(mapping_file, dtype=MAPPING_SCHEMA, keep_default_na=False)
    except Exception as e:
        print(f"❌ 讀取 CSV 失敗: {e}")
        return df

    df['payment_prefix'] = ''
    for col in MAPPING_SCHEMA.keys(): rules[col] = rules[col].str.strip()

    df['_Debug_Rule'] = np.nan
    df['_Debug_Rule'] = df['_Debug_Rule'].astype(object)
    match_count = 0
    
    df_card_clean = df['Card_No'].astype(str).str.replace(' ', '').str.strip()
    df_mobile_clean = df['Mobile_Payment'].astype(str).str.strip().replace('nan', '')

    for idx, row in rules.iterrows():
        target_card = row['卡號'].replace(' ', '')
        if not target_card: continue
        
        target_card_type = row['對應卡片']
        target_mobile = row['行動支付標籤']
        target_prefix = row['加在消費明細摘要前方']
        replace_card = row['卡號代換']

        mask = None
        rule_desc = ""

        if '/' in target_card:
            mask = (df_card_clean == target_card)
            rule_desc = f"國泰雙號: {target_card}"
        elif target_mobile:
            mask = (df_card_clean == target_card) & (df_mobile_clean == target_mobile)
            rule_desc = f"行動支付(玉山): {target_card} + {target_mobile}"
        else:
            mask = (df_card_clean == target_card)
            rule_desc = f"一般歸戶: {target_card}"

        if mask.any():
            match_count += mask.sum()
            df.loc[mask, '_Debug_Rule'] = rule_desc
            if target_card_type: df.loc[mask, 'Card_Type'] = target_card_type
            if target_mobile: df.loc[mask, 'Mobile_Payment'] = target_mobile
            if target_prefix: df.loc[mask, 'payment_prefix'] = target_prefix
            if replace_card: df.loc[mask, 'Card_No'] = replace_card

    print(f"   - 共處理 {match_count} 筆符合對照表的交易。")
    df = df.drop(columns=['_Debug_Rule'])
    return df

def cleanup_cathay_remaining(df):
    print(">>> 執行邏輯: 掃描剩餘的國泰雙卡號格式...")
    mask = (df['Bank_Name'] == 'cube_bank') & (df['Card_No'].astype(str).str.contains('/', regex=False))
    if mask.any():
        print(f"   - 發現 {mask.sum()} 筆未定義的雙卡號資料，進行修復(取前半段)...")
        df.loc[mask, 'Card_No'] = df.loc[mask, 'Card_No'].str.split('/', n=1).str[0].str.strip()
    return df

def identify_third_party_payment(df, payment_rules):
    print(">>> 執行邏輯: 識別第三方支付關鍵字 (Config Driven)...")
    if not payment_rules: return df
    match_count = 0
    for rule in payment_rules:
        pattern = rule.get('Pattern')
        category = rule.get('Category')
        prefix_label = rule.get('Prefix_Label')
        if not pattern: continue
        
        mask = df['Merchant'].astype(str).str.contains(pattern, regex=True, na=False)
        target_mask = mask & (df['Mobile_Payment'] == '')
        
        if target_mask.any():
            match_count += target_mask.sum()
            df.loc[target_mask, 'payment_prefix'] = prefix_label
            df.loc[target_mask, 'Mobile_Payment'] = category
    print(f"   - 已識別 {match_count} 筆第三方支付交易。")
    return df

# ==========================================
# 2. 其他商業邏輯
# ==========================================

def process_esun_epoint(df):
    print(">>> 執行邏輯: 玉山銀行 e.Point 折抵金額補全...")
    bank_mask = (df['Bank_Name'] == 'esun_bank') 
    if not bank_mask.any(): return df

    pattern = r'使用e point\s*(?P<points>[\d,]+)\s*點折現金\s*(?P<amount>[\d,]+)\s*元'
    target_mask = bank_mask & df['Merchant'].astype(str).str.contains('使用e point', case=False, na=False)
    
    extracted = df.loc[target_mask, 'Merchant'].astype(str).str.extract(pattern)
    valid_extract_mask = extracted['amount'].notna()
    valid_indices = df.loc[target_mask][valid_extract_mask].index
    
    if len(valid_indices) > 0:
        amounts = extracted.loc[valid_extract_mask, 'amount'].str.replace(',', '').astype(float)
        negative_amounts = amounts * -1
        df.loc[valid_indices, 'Payment_Amount'] = negative_amounts
        df.loc[valid_indices, 'Payment_Currency'] = 'TWD'
        print(f"   - 已補全 {len(valid_indices)} 筆 e.Point 折抵金額。")
    return df

def clean_merchant_by_regex(df, regex_rules):
    print(">>> 執行邏輯: 商家名稱正則清洗 (正規化模式)...")
    if not regex_rules: return df
    count = 0
    df['Merchant'] = df['Merchant'].astype(str)
    for rule in regex_rules:
        pat = rule.get('Pattern')
        repl = rule.get('Replacement')
        if not repl: continue
        try:
            mask = df['Merchant'].str.contains(pat, regex=True, na=False)
            if mask.any():
                df.loc[mask, 'Merchant'] = repl 
                count += 1
        except Exception as e:
            print(f"❌ Regex 規則錯誤 '{pat}': {e}")
    print(f"   - 已執行 {count} 條清洗規則循環。")
    return df

def apply_final_prefixes(df):
    print(">>> 執行邏輯: 合併支付前綴至商家名稱...")
    if 'payment_prefix' not in df.columns: return df
    has_prefix = df['payment_prefix'] != ''
    count = has_prefix.sum()
    if count > 0:
        df.loc[has_prefix, 'Merchant'] = df.loc[has_prefix, 'payment_prefix'] + df.loc[has_prefix, 'Merchant']
        print(f"   - 已為 {count} 筆交易加上支付前綴。")
    df = df.drop(columns=['payment_prefix'])
    return df

def classify_transaction_type(df, config):
    """
    [任務 F] 交易類型分類 (包含國外交易細分邏輯)
    """
    print(">>> 執行邏輯: 交易類型分類 (繳款/折抵/費用/交易/國外)...")
    
    payment_kws = config.get('payment_keywords', [])
    credit_kws = config.get('credit_keywords', [])
    fee_kws = config.get('fee_keywords', [])
    
    payment_pat = '|'.join(payment_kws) if payment_kws else '(?!)'
    credit_pat = '|'.join(credit_kws) if credit_kws else '(?!)'
    fee_pat = '|'.join(fee_kws) if fee_kws else '(?!)'
    merchant_str = df['Merchant'].astype(str)

    # 1. 繳款
    mask_payment = (
        merchant_str.str.contains(payment_pat, case=False, regex=True) & 
        (df['Transaction_Type'] == '') &
        ~merchant_str.str.contains('代收|手續費|運費', case=False, regex=True)
    )
    if mask_payment.any():
        df.loc[mask_payment, 'Transaction_Type'] = '繳款'
        for col in ['Card_Type', 'Mobile_Payment', 'Consumption_Place', 'payment_prefix']:
            if col in df.columns: df.loc[mask_payment, col] = ''

    # 2. 折抵
    mask_credit = (
        merchant_str.str.contains(credit_pat, case=False, regex=True) & 
        (df['Transaction_Type'] == '')
    )
    if mask_credit.any():
        df.loc[mask_credit, 'Transaction_Type'] = '折抵'
        for col in ['Mobile_Payment', 'payment_prefix']:
            if col in df.columns: df.loc[mask_credit, col] = ''

    # 3. 退刷
    mask_refund = (df['Payment_Amount'] < 0) & (df['Transaction_Type'] == '')
    if mask_refund.any():
        df.loc[mask_refund, 'Transaction_Type'] = '退刷'

    # 4. 費用
    mask_fee = (
        merchant_str.str.contains(fee_pat, case=False, regex=True) & 
        (df['Transaction_Type'] == '')
    )
    if mask_fee.any():
        df.loc[mask_fee, 'Transaction_Type'] = '各項費用'
        for col in ['Mobile_Payment', 'payment_prefix']:
            if col in df.columns: df.loc[mask_fee, col] = ''

    # 5. 驗證/零元
    mask_zero = (df['Payment_Amount'] == 0) & (df['Transaction_Type'] == '')
    if mask_zero.any():
        df.loc[mask_zero, 'Transaction_Type'] = '驗證/零元'

    # ==========================================================
    # 6. 交易 (Transaction) - 含國外交易細分邏輯 (New)
    # ==========================================================
    # 先找出所有尚未被分類的交易 (Transaction_Type 為空) 且金額 > 0
    mask_general = (df['Payment_Amount'] > 0) & (df['Transaction_Type'] == '')
    
    if mask_general.any():
        # Step A: 先全部標記為 '交易' (當作 Default)
        df.loc[mask_general, 'Transaction_Type'] = '交易'
        
        # Step B: 篩選出「國外交易候選人」
        target_indices = df[mask_general].index
        
        # 條件: 地點不是 TW
        c_loc = df.loc[target_indices, 'Merchant_Location'] != 'TW'
        mask_foreign = c_loc
        foreign_indices = target_indices[mask_foreign]
        
        # [Debug / Strict Checks] 嚴格檢查保留區 (已註解)
        # 如果發現國外交易判斷太寬鬆，可解開這邊來檢查
        # c1 = df.loc[target_indices, 'Currency_Type'] != ''        #交易幣別不為空
        # c2 = df.loc[target_indices, 'Payment_Currency'] != ''     #結算幣別不為空
        # c3 = (df.loc[target_indices, 'Currency_Amount'].notna()) & (df.loc[target_indices, 'Currency_Amount'] != 0)
        #                                                           # 強化條件: 交易幣別、結算幣別不為空，且交易金額不為零
        # mask_foreign = mask_foreign & c1 & c2 & c3  # 若要啟用嚴格模式，請取消這行註解

        if len(foreign_indices) > 0:
            print(f"   - 偵測到 {len(foreign_indices)} 筆潛在國外交易，進行細分類...")
            
            # 分類邏輯 1: 幣別不一致 -> 一般國外交易
            mask_diff = df.loc[foreign_indices, 'Currency_Type'] != df.loc[foreign_indices, 'Payment_Currency']
            df.loc[foreign_indices[mask_diff], 'Transaction_Type'] = '一般國外交易'
            
            # 分類邏輯 2: 幣別一致 (同幣別)
            mask_same = ~mask_diff
            same_indices = foreign_indices[mask_same]
            
            if len(same_indices) > 0:
                # 2-1: 都是 TWD -> 台幣跨境交易
                mask_twd = df.loc[same_indices, 'Currency_Type'] == 'TWD'
                target_twd = same_indices[mask_twd]
                df.loc[same_indices[mask_twd], 'Transaction_Type'] = '台幣跨境交易'
                # 強制同步金額：因為是台幣跨境，消費額 = 結算額
                df.loc[target_twd, 'Currency_Amount'] = df.loc[target_twd, 'Payment_Amount']


                # 2-2: 都不是 TWD (且幣別相同) -> 一般雙幣交易
                mask_not_twd = ~mask_twd
                df.loc[same_indices[mask_not_twd], 'Transaction_Type'] = '一般雙幣交易'

    return df

# ==========================================
# 3. 主程序
# ==========================================

def main():
    print("--- 開始執行 Refine 程序 (整合版) ---")
    
    payment_rules_list = load_payment_rules(CONFIG_DIR)
    merchant_rules_list = load_merchant_regex_rules(CONFIG_DIR)  
    config = load_yaml_config(YAML_CONFIG_FILE)
    
    if not os.path.exists(INPUT_FILE):
        print(f"❌ 找不到輸入檔: {INPUT_FILE}")
        return
        
    df = pd.read_csv(INPUT_FILE, dtype=COLUMN_TYPES)
    print(f"成功讀取 {len(df)} 筆資料")

    # 型態強制與初始化
    for col, dtype in COLUMN_TYPES.items():
        if col not in df.columns:
            if dtype == str: df[col] = ''
            elif dtype == float: df[col] = np.nan
        else:
            if dtype == str:
                df[col] = df[col].fillna('').astype(str).str.strip()
                df.loc[df[col].str.lower() == 'nan', col] = ''
            elif dtype == float:
                df[col] = pd.to_numeric(df[col], errors='coerce')

    if 'payment_prefix' not in df.columns: df['payment_prefix'] = ''
    df['payment_prefix'] = df['payment_prefix'].fillna('').astype(str).str.strip()

    print("✅ 資料型態清洗完成，開始執行 ETL 邏輯...")
    
    # Step 1: 卡號處理
    df = apply_card_mapping(df, config_dir=CONFIG_DIR)
    df = cleanup_cathay_remaining(df)

    # Step 2: 第三方支付識別
    df = identify_third_party_payment(df, payment_rules=payment_rules_list)

    # Step 3: 商家名稱清洗
    df = process_esun_epoint(df)
    df = clean_merchant_by_regex(df, regex_rules=merchant_rules_list)

    # Step 4: 商業邏輯分類 (含國外交易)
    df = classify_transaction_type(df, config)

    # Step 5: 最終前綴組裝
    df = apply_final_prefixes(df)

    # Step 6: 輸出
    cols_order = [
        'Transaction_Date', 'Posting_Date',
        'Bank_Name', 'Card_Type', 'Card_No',
        'Merchant', 'Merchant_Location', 'Consumption_Place','Conversion_Date',
        'Transaction_Type', 'Mobile_Payment',
        'Currency_Type', 'Currency_Amount', 
        'Payment_Currency', 'Payment_Amount'
    ]
    
    df_final = df[cols_order]
    df_final.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
    print(f"✅ 處理完成！結果已輸出至 {OUTPUT_FILE}")

if __name__ == "__main__":
    main()