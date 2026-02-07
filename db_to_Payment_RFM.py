import pandas as pd
import sqlite3
import re
import os
from datetime import datetime, timedelta

# ==========================================
# 設定區
# ==========================================
DB_FILE = 'data/Bills.db'
PAYMENT_CONFIG_FILE = 'configs/payment_regex_rules.csv'

# 設定要分析的 Priority 門檻 (包含此數值)
# 根據您的描述，我們只分析特定 Priority 以上的支付方式 (如第三方支付)
TARGET_PRIORITY_THRESHOLD = 20

SHORT_TERM_DAYS = 365
OUTPUT_CSV = 'payment_rfm_result.csv'
OUTPUT_TABLE_NAME = 'Analysis_RFM_Payment'

# 銀行非消費項目排除關鍵字 (與商店 RFM 保持一致)
EXCLUDE_TYPE_KEYWORDS = r"繳款|折抵|各項費用|手續費|年費|利息"

# ==========================================
# 核心邏輯
# ==========================================

def load_payment_config(csv_path):
    """
    讀取支付設定檔，建立前綴映射表
    回傳: 
    1. prefix_map: { 'LinePay－': 'Line Pay', ... }
    2. valid_prefixes: 排序後的前綴列表 (長字串優先)
    """
    if not os.path.exists(csv_path):
        print(f"錯誤：找不到支付設定檔 {csv_path}")
        return {}, []
    
    df = pd.read_csv(csv_path)
    required = ['Category', 'Prefix_Label', 'Priority']
    if not all(col in df.columns for col in required):
        print(f"錯誤：CSV 缺少必要欄位: {required}")
        return {}, []
    
    # 過濾 Priority (只保留第三方支付/電子錢包)
    df_filtered = df[df['Priority'] >= TARGET_PRIORITY_THRESHOLD].copy()
    
    prefix_map = {}
    valid_prefixes = []
    
    for _, row in df_filtered.iterrows():
        prefix = str(row['Prefix_Label']).strip()
        category = str(row['Category']).strip()
        
        if prefix and prefix.lower() != 'nan':
            prefix_map[prefix] = category
            valid_prefixes.append(prefix)
            
    # 依長度降冪排序 (避免 LinePay 被 Line 誤判)
    valid_prefixes.sort(key=len, reverse=True)
    
    print(f"已載入 {len(valid_prefixes)} 種支付前綴 (Priority >= {TARGET_PRIORITY_THRESHOLD})")
    return prefix_map, valid_prefixes

def identify_payment_method(merchant_name, prefix_map, valid_prefixes):
    """
    從商家名稱識別支付方式
    """
    if not isinstance(merchant_name, str):
        return "實體卡/其他"
    
    name = merchant_name.strip()
    
    for prefix in valid_prefixes:
        if name.startswith(prefix):
            return prefix_map[prefix]
            
    return "實體卡/其他"

def calculate_rfm(df_subset, analysis_date, prefix=''):
    if df_subset.empty: return pd.DataFrame()

    # Group By 的對象變成 'Payment_Method'
    rfm = df_subset.groupby('Payment_Method').agg({
        'transaction_date': lambda x: (analysis_date - x.max()).days,
        'transaction_id': 'nunique', # 計算交易筆數
        'payment_amount': 'sum'      # 計算總金額
    }).rename(columns={
        'transaction_date': f'{prefix}recency_days',
        'transaction_id': f'{prefix}frequency',
        'payment_amount': f'{prefix}monetary'
    })

    # 計算排名 (PR值)
    rfm[f'{prefix}r_rank'] = rfm[f'{prefix}recency_days'].rank(pct=True, ascending=False)
    rfm[f'{prefix}f_rank'] = rfm[f'{prefix}frequency'].rank(pct=True, ascending=True)
    rfm[f'{prefix}m_rank'] = rfm[f'{prefix}monetary'].rank(pct=True, ascending=True)
    
    return rfm

def main():
    print("=== 開始支付方式 (Payment) RFM 分析 ===")
    
    # 1. 載入支付設定
    prefix_map, valid_prefixes = load_payment_config(PAYMENT_CONFIG_FILE)
    if not prefix_map: return

    # 2. 讀取資料庫
    with sqlite3.connect(DB_FILE) as conn:
        print("讀取資料庫...")
        df = pd.read_sql("SELECT transaction_id, transaction_date, merchant_name, payment_amount, transaction_type FROM all_transactions", conn)
    
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])
    
    # 3. 識別支付方式
    print("識別支付方式...")
    df['Payment_Method'] = df['merchant_name'].apply(lambda x: identify_payment_method(x, prefix_map, valid_prefixes))
    
    # 診斷：看看抓到了什麼
    print("\n[支付方式分佈預覽]")
    print(df['Payment_Method'].value_counts().head(10))

    # 4. 排除邏輯 (排除繳費/手續費等非消費行為)
    # 注意：這裡不排除 "RFM_Exclusion" (商家黑名單)，因為我們想知道連加值都算在內的支付習慣
    # 但要排除 "銀行費用"
    mask_not_bank_fee = ~df['transaction_type'].astype(str).str.contains(EXCLUDE_TYPE_KEYWORDS, na=False, regex=True)
    
    df_filtered = df[mask_not_bank_fee].copy()
    print(f"\n排除銀行費用後有效筆數: {len(df_filtered)}")

    # 5. 雙軌 RFM 計算
    if df_filtered.empty:
        print("警告：無有效資料。")
        return

    analysis_date = df_filtered['transaction_date'].max() + timedelta(days=1)
    
    print("計算 RFM 指標...")
    rfm_life = calculate_rfm(df_filtered, analysis_date, prefix='life_')
    
    cutoff = analysis_date - timedelta(days=SHORT_TERM_DAYS)
    rfm_short = calculate_rfm(df_filtered[df_filtered['transaction_date'] >= cutoff], analysis_date, prefix='short_')
    
    # 6. 合併
    final_df = rfm_life.join(rfm_short, how='left', lsuffix='_l', rsuffix='_s')
    
    # 填補空值
    fill_zero_cols = ['short_frequency', 'short_monetary', 'short_r_rank', 'short_f_rank', 'short_m_rank']
    for col in fill_zero_cols:
        if col in final_df.columns: final_df[col] = final_df[col].fillna(0)
    final_df['short_recency_days'] = final_df['short_recency_days'].fillna(9999)

    # 7. 分群標籤 (針對支付方式的特殊分群)
    def label_payment_segment(row):
        # 邏輯稍作調整：支付方式通常較少，Top 20% 可能只有 1-2 種
        is_high_freq = row['life_f_rank'] >= 0.7  # 常用支付 (Frequency 比 Monetary 重要)
        is_active = row['short_frequency'] > 0
        
        if is_high_freq and is_active: return "主力支付 (Main Wallet)"
        elif is_high_freq and not is_active: return "已棄用支付 (Abandoned)"
        elif not is_high_freq and is_active: return "輔助支付 (Backup)"
        else: return "冷門支付 (Rare)"

    final_df['segment'] = final_df.apply(label_payment_segment, axis=1)

    # 8. 輸出
    final_df.sort_values(by='life_frequency', ascending=False, inplace=True) # 支付方式依頻次排序較合理
    final_df.to_csv(OUTPUT_CSV)
    
    with sqlite3.connect(DB_FILE) as conn:
        final_df.to_sql(OUTPUT_TABLE_NAME, conn, if_exists='replace', index=True)
        
    print(f"\n完成！已輸出至 {OUTPUT_CSV} 與資料庫。")

if __name__ == "__main__":
    main()