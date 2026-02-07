import pandas as pd
import sqlite3
import os
from datetime import datetime, timedelta

# [新增] 引入 refine 以取得全域路徑變數
import refine 

# ==========================================
# 設定區
# ==========================================
# 使用 refine 模組的路徑
DB_PATH = os.path.join(refine.DATA_DIR, 'Bills.db')
OUTPUT_CSV = 'card_rfm_result.csv'

# 分析期間 (預設看近一年，才能包含年繳保費等週期性消費)
ANALYSIS_DAYS = 365 

# 銀行雜訊排除 (只看消費，不看繳費)
EXCLUDE_TYPE_KEYWORDS = r"繳款|折抵|各項費用|手續費|年費|利息"

# ==========================================
# 核心邏輯
# ==========================================

def calculate_card_rfm(df, analysis_date):
    """
    計算每張卡的 RFM 指標
    """
    if df.empty: return pd.DataFrame()

    # 1. 聚合運算 (GroupBy 銀行+卡別)
    # 這裡假設 load_to_db.py 已經把 Card_Type 存為 card_name
    rfm = df.groupby(['bank_name', 'card_name']).agg({
        'transaction_date': lambda x: (analysis_date - x.max()).days, # Recency
        'transaction_id': 'nunique', # Frequency
        'payment_amount': 'sum'      # Monetary
    }).rename(columns={
        'transaction_date': 'recency_days',
        'transaction_id': 'frequency',
        'payment_amount': 'monetary'
    })

    # 2. 計算 PR 值 (排名) - 用於判斷相對強弱
    # 注意：這裡的分數是跟「您自己的其他卡片」比
    if len(rfm) > 0:
        rfm['f_rank'] = rfm['frequency'].rank(pct=True, ascending=True)
        rfm['m_rank'] = rfm['monetary'].rank(pct=True, ascending=True)
    else:
        rfm['f_rank'] = 0
        rfm['m_rank'] = 0
    
    return rfm

def label_card_segment(row):
    """
    定義卡片角色矩陣
    """
    # R: 超過半年沒刷就是危險訊號
    if row['recency_days'] > 180:
        return "❄️ 冷凍/沉睡卡 (Dormant)"
    
    # 門檻設定：PR 50% 以上算「高」
    is_high_freq = row['f_rank'] >= 0.5
    is_high_money = row['m_rank'] >= 0.5
    
    if is_high_freq and is_high_money:
        return "👑 主力攻擊手 (Main Driver)"
    elif not is_high_freq and is_high_money:
        return "🎯 狙擊手 (Sniper)"
    elif is_high_freq and not is_high_money:
        return "🔄 後勤補給 (Utility)"
    else:
        return "📉 低效冗餘 (Inefficient)"

def main():
    print("=== 信用卡 RFM 分析 (Portfolio Check) ===")
    
    # 1. 讀取資料
    if not os.path.exists(DB_PATH):
        print(f"❌ 找不到資料庫: {DB_PATH}")
        return

    with sqlite3.connect(DB_PATH) as conn:
        print("讀取資料庫...")
        try:
            # 確保欄位名稱與 load_to_db.py 的 rename_map 一致
            # (transaction_date, payment_amount, transaction_type, bank_name, card_name)
            sql = """
            SELECT transaction_id, transaction_date, payment_amount, transaction_type, 
                   bank_name, card_name 
            FROM all_transactions
            """
            df = pd.read_sql(sql, conn)
        except Exception as e:
            print(f"錯誤：讀取資料庫失敗，請檢查欄位名稱。\n{e}")
            return

    if df.empty:
        print("❌ 資料庫無資料。")
        return

    df['transaction_date'] = pd.to_datetime(df['transaction_date'])
    
    # 2. 資料清洗
    # A. 排除銀行雜訊
    mask_not_bank_fee = ~df['transaction_type'].astype(str).str.contains(EXCLUDE_TYPE_KEYWORDS, na=False, regex=True)
    
    # B. 排除卡片資訊不明的資料 (避免 Unknown 干擾)
    # 有些舊資料可能沒有 card_name，濾掉比較準
    mask_has_card_info = df['card_name'].notna() & (df['card_name'] != '')
    
    df_clean = df[mask_not_bank_fee & mask_has_card_info].copy()
    
    if df_clean.empty:
        print("警告：排除雜訊後無有效資料。")
        return

    # C. 時間篩選 (近一年)
    latest_date = df_clean['transaction_date'].max()
    cutoff_date = latest_date - timedelta(days=ANALYSIS_DAYS)
    df_final = df_clean[df_clean['transaction_date'] >= cutoff_date].copy()
    
    print(f"分析區間: {cutoff_date.date()} ~ {latest_date.date()} (近 {ANALYSIS_DAYS} 天)")
    print(f"有效消費筆數: {len(df_final)}")

    # 3. 執行 RFM 計算
    analysis_date = latest_date + timedelta(days=1)
    
    # 這裡的 df_final 已經只剩下 bank_name, card_name, transaction_date, payment_amount 等欄位
    rfm_df = calculate_card_rfm(df_final, analysis_date)
    
    if rfm_df.empty:
        print("警告：無法計算 RFM 指標。")
        return

    # 4. 貼標籤 (Segmentation)
    rfm_df['segment'] = rfm_df.apply(label_card_segment, axis=1)
    
    # 5. 整理輸出
    # 依金額排序 (Monetary)
    rfm_df = rfm_df.sort_values(by='monetary', ascending=False)
    
    # 計算一些輔助指標
    # Avg_Ticket: 平均客單價 (看這張卡是刷大額還是小額)
    rfm_df['avg_ticket'] = (rfm_df['monetary'] / rfm_df['frequency']).astype(int)
    
    # 格式化輸出
    output_cols = ['segment', 'recency_days', 'frequency', 'monetary', 'avg_ticket']
    
    # [修改] 輸出到 data 資料夾，而非根目錄
    csv_path = os.path.join(refine.DATA_DIR, OUTPUT_CSV)
    rfm_df[output_cols].to_csv(csv_path, encoding='utf-8-sig')
    
    print(f"\n已輸出報表: {csv_path}")
    print("-" * 30)
    print("[卡片戰力預覽]")
    print(rfm_df[['segment', 'monetary', 'recency_days']].head(10))

if __name__ == "__main__":
    main()