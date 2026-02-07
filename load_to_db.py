import pandas as pd
import sqlite3
import hashlib
import os
import numpy as np

# ==========================================
# 0. 配置與路徑
# ==========================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')

INPUT_CSV = os.path.join(DATA_DIR, 'refined_all_banks.csv')
DB_NAME = 'Bills.db' 
DB_PATH = os.path.join(DATA_DIR, DB_NAME)
TABLE_NAME = 'all_transactions'

# ==========================================
# 1. 輔助函式：生成唯一 ID
# ==========================================
def generate_transaction_id(row):
    """
    建立唯一的交易 ID (Hash)
    組合：日期 + 商家 + 金額 + 卡號 + 交易類型
    """
    # 轉字串並處理 None，確保 Hash 穩定
    def safe_str(val):
        return str(val).strip() if pd.notna(val) else ""

    unique_str = (
        safe_str(row.get('Transaction_Date')) +
        safe_str(row.get('Merchant')) +
        safe_str(row.get('Card_No')) +
        safe_str(row.get('Payment_Amount')) + 
        safe_str(row.get('Transaction_Type'))
    )
    # 回傳 MD5 Hash
    return hashlib.md5(unique_str.encode('utf-8')).hexdigest()

# ==========================================
# 2. 核心邏輯
# ==========================================
def load_csv_and_save_to_db():
    if not os.path.exists(INPUT_CSV):
        print(f"❌ 錯誤: 找不到 CSV 檔案: {INPUT_CSV}")
        return

    print(f"📂 讀取 CSV: {INPUT_CSV}")
    
    # 定義讀取型態
    dtype_mapping = {
        'Currency_Amount': float,
        'Payment_Amount': float,
        'Card_No': str,
        'Mobile_Payment': str,
        'Merchant': str
    }
    
    try:
        df = pd.read_csv(INPUT_CSV, dtype=dtype_mapping)
        
        # 1. 生成 Primary Key (Transaction_ID)
        print("🔨 正在生成交易雜湊 ID (Transaction Hash)...")
        df['transaction_id'] = df.apply(generate_transaction_id, axis=1)

        # 2. 欄位更名 (Mapping to Snake Case)
        # 讓資料庫欄位變成小寫底線風格，比較好寫 SQL
	# 欄位順序：
        # 1.日期組： Transaction_Date, Posting_Date, Conversion_Date
        # 2.卡片組： Bank_Name, Card_Type, Card_No
        # 3.商家組： Merchant, Merchant_Location, Consumption_Place
        # 4.交易組： Transaction_Type, Mobile_Payment
        # 5.金額組： Currency_Amount, Payment_Amount, Currency_Type, Payment_Currency
    
        rename_map = {
            'Transaction_Date': 'transaction_date',
            'Posting_Date': 'posting_date',
            'Conversion_Date': 'conversion_date',
            'Bank_Name': 'bank_name',
            'Card_Type': 'card_name',     # 對應您的 card_name
            'Card_No': 'card_no',
            'Merchant': 'merchant_name',  # 對應您的 merchant_name
            'Merchant_Location': 'merchant_location',
            'Consumption_Place': 'consumption_place',
            'Transaction_Type': 'transaction_type',
            'Mobile_Payment': 'mobile_payment',
            'Currency_Amount': 'currency_amount',
            'Payment_Amount': 'payment_amount',
            'Currency_Type': 'currency_type',
            'Payment_Currency': 'payment_currency'
        }
        
        # 只選取有定義的欄位，避免寫入不必要的雜訊
        available_cols = [c for c in rename_map.keys() if c in df.columns]
        df_db = df[available_cols].rename(columns=rename_map)
        
        # 把 transaction_id 加回去 (因為它是新生成的，不在 rename_map 裡)
        df_db['transaction_id'] = df['transaction_id']

        # 3. 處理日期與空值
        date_cols = ['transaction_date', 'posting_date', 'conversion_date']
        for col in date_cols:
            if col in df_db.columns:
                df_db[col] = df_db[col].fillna('').astype(str)

        print(f"✅ 資料準備完成，共 {len(df_db)} 筆")

        # 4. 寫入 SQLite
        conn = sqlite3.connect(DB_PATH)
        print(f"🔌 連接資料庫: {DB_PATH}")
        
        # 使用 'replace' 模式：每次全量覆蓋，保證與 ETL 結果一致
        # 如果您想要保留歷史紀錄，可以改用 'append' 配合 transaction_id 去重，
        # 但既然 etl.py 是全量跑，這裡 replace 是最乾淨的。
        df_db.to_sql(TABLE_NAME, conn, if_exists='replace', index=False)
        
        # 5. 建立索引 (Optimization)
        cursor = conn.cursor()
        print("🔧 建立索引中...")
        # 針對常用查詢欄位建索引
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_txn_date ON {TABLE_NAME} (transaction_date)")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_merchant ON {TABLE_NAME} (merchant_name)")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_card_no ON {TABLE_NAME} (card_no)")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_txn_id ON {TABLE_NAME} (transaction_id)")
        
        conn.commit()
        
        # 6. 驗證
        cursor.execute(f"SELECT count(*) FROM {TABLE_NAME}")
        count = cursor.fetchone()[0]
        print(f"📊 驗證: 資料表 [{TABLE_NAME}] 目前共有 {count} 筆資料")
        
        conn.close()
        print("👋 資料庫作業完成")

    except Exception as e:
        print(f"❌ 發生錯誤: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    load_csv_and_save_to_db()