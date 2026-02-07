import pandas as pd
import sqlite3
import re
import os
from datetime import datetime, timedelta

# [新增] 引入 refine 以取得全域路徑變數
import refine 

# ==========================================
# 1. 設定區 (Configuration)
# ==========================================
# 使用 refine 模組的路徑，確保 Single Source of Truth
DB_PATH = os.path.join(refine.DATA_DIR, 'Bills.db') 
MERCHANT_CONFIG_PATH = os.path.join(refine.CONFIG_DIR, refine.FILE_MERCHANTS)
PAYMENT_CONFIG_PATH = os.path.join(refine.CONFIG_DIR, refine.FILE_CHANNELS)

SHORT_TERM_DAYS = 365
OUTPUT_CSV = 'rfm_analysis_result.csv'
OUTPUT_TABLE_NAME = 'Analysis_RFM_Merchant'

# 銀行非消費項目排除關鍵字 (建議也可以移到 excluded_types.yaml 管理，目前先維持寫死)
EXCLUDE_TYPE_KEYWORDS = r"繳款|折抵|各項費用|手續費|年費|利息"

# ==========================================
# 2. 資料讀取與載入函式
# ==========================================

def load_payment_prefixes(csv_path):
    """
    讀取支付設定檔，只提取 'Prefix_Label' 欄位
    """
    if not os.path.exists(csv_path):
        print(f"警告：找不到支付設定檔 {csv_path}")
        return []
    
    df = pd.read_csv(csv_path)
    if 'Prefix_Label' not in df.columns:
        print("警告：支付設定檔缺少 'Prefix_Label' 欄位。")
        return []
    
    # 取出非空值，轉為字串並去除空白
    prefixes = df['Prefix_Label'].dropna().astype(str).str.strip().tolist()
    # 依長度降冪排序
    prefixes.sort(key=len, reverse=True)
    return prefixes

def load_merchant_config_hybrid(csv_path):
    """
    讀取商家設定檔，同時建立「Regex 列表」與「查表字典」
    """
    if not os.path.exists(csv_path):
        print(f"錯誤：找不到商家設定檔 {csv_path}")
        return [], {}
    
    # 讀取時指定 dtype=str 以防純數字商家名被轉成 int
    df = pd.read_csv(csv_path, dtype=str)
    
    required = ['Pattern', 'Replacement', 'Priority', 'Category', 'RFM_Exclusion']
    if not all(col in df.columns for col in required):
        print(f"錯誤：CSV 缺少必要欄位: {required}")
        return [], {}
    
    # 轉換 Priority 為數字並排序
    if 'Priority' in df.columns:
        df['Priority'] = pd.to_numeric(df['Priority'], errors='coerce').fillna(999)
    
    df_sorted = df.sort_values(by='Priority', ascending=False)
    
    rules_list = []
    lookup_dict = {}
    
    for _, row in df_sorted.iterrows():
        try:
            # 1. 建立 Regex 規則
            pattern = re.compile(str(row['Pattern']), re.IGNORECASE)
            info = {
                'name': str(row['Replacement']),
                'category': str(row['Category']),
                'sub_category': str(row.get('Sub_Category', '')),
                # 處理布林值轉換 (CSV 讀進來可能是 'True'/'False' 字串)
                'RFM_Exclusion': str(row['RFM_Exclusion']).lower() == 'true'
            }
            
            rules_list.append({
                'pattern': pattern,
                **info
            })
            
            # 2. 建立直連查表 (Key = Replacement)
            lookup_key = str(row['Replacement']).strip()
            if lookup_key not in lookup_dict:
                lookup_dict[lookup_key] = info
                
        except re.error as e:
            print(f"警告: 無效的 Regex '{row['Pattern']}' - {e}")
            
    print(f"已載入 {len(rules_list)} 條 Regex 規則，並建立 {len(lookup_dict)} 筆直連索引。")
    return rules_list, lookup_dict

# ==========================================
# 3. 核心處理邏輯 (Hybrid Strategy)
# ==========================================

def process_merchant_hybrid(raw_name, rules_list, lookup_dict, payment_prefixes):
    """
    整合流程：切除前綴 -> 精確查表 -> Regex 補漏
    """
    if not isinstance(raw_name, str):
        return "Unknown", "Unknown", "", False
    
    current_name = raw_name.strip()
    
    # --- Step 1: 精準切除支付前綴 ---
    for prefix in payment_prefixes:
        if current_name.startswith(prefix):
            current_name = current_name[len(prefix):]
            break 
            
    current_name = current_name.strip()
    
    # --- Step 2: 精確查表 (Lookup First) ---
    if current_name in lookup_dict:
        info = lookup_dict[current_name]
        return current_name, info['category'], info['sub_category'], info['RFM_Exclusion']
    
    # --- Step 3: Regex 掃描 (Fallback) ---
    for rule in rules_list:
        if rule['pattern'].search(current_name):
            return rule['name'], rule['category'], rule['sub_category'], rule['RFM_Exclusion']
            
    # --- Step 4: 宣告放棄 ---
    final_name = current_name if current_name else raw_name
    return final_name, "Unknown", "", False

def calculate_rfm(df_subset, analysis_date, prefix=''):
    if df_subset.empty: return pd.DataFrame()

    # Group by 'clean_merchant_name'
    rfm = df_subset.groupby('clean_merchant_name').agg({
        'transaction_date': lambda x: (analysis_date - x.max()).days,
        'transaction_id': 'nunique',
        'payment_amount': 'sum',
        'Category': 'first',
        'Sub_Category': 'first'
    }).rename(columns={
        'transaction_date': f'{prefix}recency_days',
        'transaction_id': f'{prefix}frequency',
        'payment_amount': f'{prefix}monetary'
    })

    # Ranking
    rfm[f'{prefix}r_rank'] = rfm[f'{prefix}recency_days'].rank(pct=True, ascending=False)
    rfm[f'{prefix}f_rank'] = rfm[f'{prefix}frequency'].rank(pct=True, ascending=True)
    rfm[f'{prefix}m_rank'] = rfm[f'{prefix}monetary'].rank(pct=True, ascending=True)
    return rfm

# ==========================================
# 4. 主程式
# ==========================================

def main():
    print("=== 開始 RFM 分析 (Hybrid Strategy) ===")
    
    # 1. 載入設定 (使用新變數)
    payment_prefixes = load_payment_prefixes(PAYMENT_CONFIG_PATH)
    merchant_rules, lookup_dict = load_merchant_config_hybrid(MERCHANT_CONFIG_PATH)
    
    if not merchant_rules: 
        print("❌ 無法載入商家規則，程式終止。")
        return

    # 2. 讀取資料庫
    if not os.path.exists(DB_PATH):
        print(f"❌ 找不到資料庫: {DB_PATH}")
        return

    with sqlite3.connect(DB_PATH) as conn:
        print("讀取資料庫 (all_transactions)...")
        # 讀取必要欄位
        df = pd.read_sql("SELECT transaction_id, transaction_date, merchant_name, payment_amount, transaction_type FROM all_transactions", conn)
    
    if df.empty:
        print("❌ 資料庫無資料。")
        return

    df['transaction_date'] = pd.to_datetime(df['transaction_date'])
    print(f"原始資料筆數: {len(df)}")
    
    # 3. 執行清洗
    print("正在執行混合式清洗邏輯...")
    
    # 套用 Hybrid 清洗
    processed_data = df['merchant_name'].apply(
        lambda x: process_merchant_hybrid(x, merchant_rules, lookup_dict, payment_prefixes)
    )
    
    # 將結果拆分回 DataFrame
    # 這裡使用 list(processed_data) 確保格式正確
    temp_df = pd.DataFrame(processed_data.tolist(), index=df.index, columns=['clean_merchant_name', 'Category', 'Sub_Category', 'RFM_Exclusion'])
    df = pd.concat([df, temp_df], axis=1)
    
    # 4. 排除邏輯
    mask_not_excluded = ~df['RFM_Exclusion']
    mask_not_bank_fee = ~df['transaction_type'].astype(str).str.contains(EXCLUDE_TYPE_KEYWORDS, na=False, regex=True)
    
    df_filtered = df[mask_not_excluded & mask_not_bank_fee].copy()
    print(f"\n排除非消費項目後有效筆數: {len(df_filtered)}")

    # --- 診斷報告 ---
    unknown_df = df_filtered[df_filtered['Category'] == 'Unknown']
    unknown_count = len(unknown_df)
    total_valid_txns = len(df_filtered)
    
    if total_valid_txns > 0:
        unknown_rate = (unknown_count / total_valid_txns) * 100
    else:
        unknown_rate = 0.0

    print(f"\n[診斷] Unknown 比例: {unknown_rate:.2f}% ({unknown_count}/{total_valid_txns})")
    
    if unknown_count > 0:
        print("[診斷] 前 10 大 Unknown 有效商家:")
        top_unknown = unknown_df.groupby('clean_merchant_name').agg({
            'transaction_id': 'count',
            'payment_amount': 'sum'
        }).rename(columns={'transaction_id': '筆數', 'payment_amount': '總金額'})
        print(top_unknown.sort_values(by='筆數', ascending=False).head(10))
    # ----------------

    # 5. RFM 計算
    if df_filtered.empty:
        print("警告：無有效資料。")
        return

    analysis_date = df_filtered['transaction_date'].max() + timedelta(days=1)
    
    print("計算 RFM 指標...")
    rfm_life = calculate_rfm(df_filtered, analysis_date, prefix='life_')
    
    cutoff = analysis_date - timedelta(days=SHORT_TERM_DAYS)
    rfm_short = calculate_rfm(df_filtered[df_filtered['transaction_date'] >= cutoff], analysis_date, prefix='short_')
    
    # 6. 合併與分群
    final_df = rfm_life.join(rfm_short, how='left', lsuffix='_l', rsuffix='_s')
    
    # 清理欄位
    final_df.drop(columns=['Category_s', 'Sub_Category_s'], errors='ignore', inplace=True)
    final_df.rename(columns={'Category_l': 'Category', 'Sub_Category_l': 'Sub_Category'}, inplace=True)
    
    # 填補空值
    fill_zero_cols = ['short_frequency', 'short_monetary', 'short_r_rank', 'short_f_rank', 'short_m_rank']
    for col in fill_zero_cols:
        if col in final_df.columns: final_df[col] = final_df[col].fillna(0)
    final_df['short_recency_days'] = final_df['short_recency_days'].fillna(9999)

    # 定義分群
    def label_segment(row):
        is_high_value = row['life_m_rank'] >= 0.8
        is_active = row['short_frequency'] > 0
        
        if is_high_value and is_active: return "核心商家 (Core)"
        elif is_high_value and not is_active: return "流失高價值 (Churned VIP)"
        elif not is_high_value and is_active and row['short_m_rank'] >= 0.8: return "潛力新星 (Rising Star)"
        elif is_active: return "一般活躍 (Active)"
        else: return "沉睡商家 (Dormant)"

    final_df['segment'] = final_df.apply(label_segment, axis=1)

    # 7. 輸出
    final_df.sort_values(by='life_monetary', ascending=False, inplace=True)
    
    # 輸出 CSV
    csv_path = os.path.join(refine.DATA_DIR, OUTPUT_CSV)
    final_df.to_csv(csv_path)
    
    # 寫入 DB
    with sqlite3.connect(DB_PATH) as conn:
        final_df.to_sql(OUTPUT_TABLE_NAME, conn, if_exists='replace', index=True)
        
    print(f"\n✅ 完成！RFM 分析結果已輸出至:\n   - CSV: {csv_path}\n   - DB Table: {OUTPUT_TABLE_NAME}")

if __name__ == "__main__":
    main()