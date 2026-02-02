import pandas as pd
import sqlite3
import hashlib
import os

# 設定檔案路徑
CSV_FILE = 'data/refined_all_banks.csv'
DB_FILE = 'Bills.db'

def generate_transaction_id(row):
    """
    建立唯一的交易 ID (Hash)，用於去重。
    組合：日期 + 商家 + 金額 + 卡號 + 授權碼(若有)
    """
    # 將關鍵欄位串接成字串，處理潛在的 float 誤差
    unique_str = (
        str(row['Transaction_Date']) +
        str(row['Merchant']) +
        str(row['Card_No']) +
        str(row['Payment_Amount']) + 
        str(row['Transaction_Type'])
    )
    # 回傳 MD5 Hash
    return hashlib.md5(unique_str.encode('utf-8')).hexdigest()

def init_db(conn):
    """
    初始化資料庫 Schema
    """
    cursor = conn.cursor()
    
    # 1. 建立 Fact_Transaction (交易事實表)
    # 使用 OR IGNORE 避免重複建立，schema 變更需手動處理或使用 migration 工具
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Fact_Transaction (
        transaction_id TEXT PRIMARY KEY,
        transaction_date TEXT,
        posting_date TEXT,
        conversion_date TEXT,
        bank_name TEXT,
        card_name TEXT,
        card_no TEXT,
        merchant_name TEXT,
        merchant_location TEXT,
        consumption_place TEXT,
        transaction_type TEXT,
        mobile_payment TEXT,
        currency_amount REAL,
        payment_amount REAL,
        currency_type TEXT,
        payment_currency TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # 2. 建立 Dim_Category (商家分類維度表)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Dim_Category (
        category_id INTEGER PRIMARY KEY AUTOINCREMENT,
        merchant_pattern TEXT NOT NULL,
        main_category TEXT,
        sub_category TEXT,
        priority INTEGER DEFAULT 0
    );
    """)

    # 3. 建立 Dim_Card_Reward (信用卡權益維度表 - SCD Type 2)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Dim_Card_Reward (
        rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
        card_name TEXT NOT NULL,
        reward_type TEXT, -- e.g., 'Cashback', 'Points'
        reward_rate REAL, -- e.g., 0.03
        effective_date TEXT, --生效日 YYYY-MM-DD
        expiry_date TEXT,    --失效日 YYYY-MM-DD
        condition_logic TEXT -- JSON 或描述，例如 {"mobile_payment": "Line Pay"}
    );
    """)
    
    conn.commit()
    print("資料庫 Schema 初始化完成。")

def load_data():
    if not os.path.exists(CSV_FILE):
        print(f"錯誤：找不到檔案 {CSV_FILE}")
        return

    print(f"正在讀取 {CSV_FILE}...")
    df = pd.read_csv(CSV_FILE)

    # 1. 產生 Primary Key (Transaction_ID)
    print("正在生成交易雜湊 ID...")
    df['transaction_id'] = df.apply(generate_transaction_id, axis=1)

    # 2. 欄位映射 (Mapping) - 確保 DataFrame 欄位名稱與 Table 對齊
    # 這裡假設 CSV 欄位名稱已經在之前的 ETL 階段標準化，
    # 若有差異，需在此 rename。例如: 'Merchant' -> 'merchant_name'
    df_db = df.rename(columns={
        'Transaction_Date': 'transaction_date',
        'Posting_Date': 'posting_date',
        'Conversion_Date': 'conversion_date',
        'Bank_Name': 'bank_name',
        'Card_Type': 'card_name',
        'Card_No': 'card_no',
        'Merchant': 'merchant_name',
        'Merchant_Location': 'merchant_location',
        'Consumption_Place': 'consumption_place',
        'Transaction_Type': 'transaction_type',
        'Mobile_Payment': 'mobile_payment',
        'Currency_Amount': 'currency_amount',
        'Payment_Amount': 'payment_amount',
        'Currency_Type': 'currency_type',
        'Payment_Currency': 'payment_currency'
    })

    # 選取對應資料庫的欄位
    db_columns = [
        'transaction_id', 'transaction_date', 'posting_date', 'conversion_date',
        'bank_name', 'card_name', 'card_no', 'merchant_name', 
        'merchant_location', 'consumption_place', 'transaction_type', 
        'mobile_payment', 'currency_amount', 'payment_amount', 
        'currency_type', 'payment_currency'
    ]
    
    # 防呆：確保欄位存在
    df_final = df_db[db_columns].copy()

    # 3. 寫入資料庫
    with sqlite3.connect(DB_FILE) as conn:
        init_db(conn)
        
        # 使用 executemany 與 INSERT OR IGNORE 進行增量寫入
        # 轉為 list of tuples
        data_to_insert = df_final.to_dict('records')
        
        cursor = conn.cursor()
        
        # 動態產生 SQL
        placeholders = ', '.join(['?'] * len(db_columns))
        columns_str = ', '.join(db_columns)
        sql = f"INSERT OR IGNORE INTO Fact_Transaction ({columns_str}) VALUES ({placeholders})"
        
        print(f"正在寫入 {len(data_to_insert)} 筆資料到 SQLite...")
        
        # 提取數值進行批次寫入
        values = [tuple(x[col] for col in db_columns) for x in data_to_insert]
        cursor.executemany(sql, values)
        
        # 獲取寫入統計
        changes = conn.total_changes
        print(f"寫入完成。本次新增/變更筆數 (含初始化)：{changes}")
        
        # 驗證查詢
        cursor.execute("SELECT Count(*) FROM Fact_Transaction")
        total_rows = cursor.fetchone()[0]
        print(f"目前資料庫總筆數：{total_rows}")

if __name__ == "__main__":
    load_data()