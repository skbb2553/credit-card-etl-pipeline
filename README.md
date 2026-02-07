# 💳 Credit Card Transaction ETL Pipeline

> **Automated Data Cleaning & Standardization for Personal Finance Analytics**
> *從「Garbage In, Garbage Out」到「精準數據決策」的自動化旅程。*

![Python](https://img.shields.io/badge/Python-3.10%2B-blue) ![Pandas](https://img.shields.io/badge/Library-Pandas-green) ![Status](https://img.shields.io/badge/Status-Active-success)

## 📖 專案背景 (Project Context)

在導入此系統前，我的個人財務分析面臨嚴重的 **"Garbage In, Garbage Out" (GIGO)** 問題。

原本依賴 Excel 手動整理與 Power BI 直連，但遭遇以下結構性挑戰：
* **非結構化數據：** 同一商家名稱混亂（如 `7-ELEVEN`, `7-11`, `統一超商`），導致消費類別定位不易，分析容易失真。
* **重複勞動：** 需要重複跨帳單的公式的一致性，而且公式加減項、回饋係數檢查也要不斷重複比對。
* **公式維護：** 只能依據帳單CSV內的消費明細做條件計算，跨帳單計算出現困難，缺乏效率且容易出錯。
* **分析受限：** 缺乏標準化欄位，無法進行深度的 **RFM 模型分析** 與 **現金回饋 (Cashback) 精算**。

本專案旨在構建一個自動化的 **ETL (Extract, Transform, Load)** 管道，將雜亂的銀行帳單轉化為高價值的分析型數據庫。

---

## 🚀 核心功能 (Key Features)

### 1. 智慧清洗與標準化 (Data Sanitization)
* **Regex 商家識別：** 利用正則表達式 (Regular Expressions) 自動歸一化商家名稱（例如：將所有 `Uber*Eats` 變體統一為 `Uber Eats`）。
* **日期格式統整：** 處理不同銀行 (國泰、玉山等) 不一致的日期格式字串。

### 2. 資料隱私優先 (Privacy First Architecture)
* **去敏化處理：** 程式邏輯與敏感數據 (如卡號、個資) 完全分離。
* **環境變數管理：** 使用 `.gitignore` 與 Config 檔案管理敏感設定，確保上傳至雲端的程式碼不含任何個資。
* **本地端密鑰管理 (Himitsu)**： 支援本地端卡號映射 (Himitsu.py)，將真實卡號轉換為虛擬代碼（如將真實卡轉為 Mock國泰CUBE卡），確保 Git Repo 內不含任何敏感個資。

### 3. 商業邏輯增強 (Business Logic Enrichment)
* **RFM 準備：** 保留交易時間與頻率特徵，為後續的 **Recency, Frequency, Monetary** 分析建立基礎。
* **RFM 分析：** 提供四個RFM分析邏輯：基本商家、支付方式(第三方支付)、信用卡使用RFM。
* **回饋關聯鍵：** 保留卡號特徵碼 (Last 4 digits)，作為後續計算「現金回饋率」的關聯鍵 (Foreign Key)。

---

## 🛠️ 開發方法論 (Development Methodology)

本專案採用 **AI 輔助開發 (AI-Assisted Development)** 模式，結合人類架構師的邏輯與 LLM 的算力。

* **Architecture (人類主導):** 定義資料流 (Data Flow)、Schema 設計、隱私邊界與商業目標。
* **Implementation (AI 加速):** 利用 Vibe Coding 模式快速生成繁瑣的 Regex 規則與 Pandas 語法。本專案使用Gemini Pro模型生成。
* **Verification (嚴格審查):** 所有生成代碼皆經過人工 Code Review，並通過真實數據的邏輯驗證。透過提示詞要求變數命名不可任意變動。

---

## 📂 檔案結構 (File Structure)

```text
.
My-Credit-Card-ETL/
│
├── .gitignore             
├── README.md              
├── requirements.txt            # 專案依賴 (pandas, numpy, pyyaml...)
│
├── etl.py                      # 主程式 (Extract)
├── refine.py                   # 主程式 (Transform/Refine)
├── load_to_db.py               # 主程式 (新增：交易明細轉檔至資料庫)
├── db_to_RFManalysis.py        # 主程式 (新增：商家地點RFM分析)
├── db_to_Payment_RFM.py        # 主程式 (新增：電子支付RFM分析，分析電子支付頻率)
├── db_to_card_RFM.py           # 主程式 (新增：信用卡RFM分析，分析卡片使用狀況)
├── generate_mock.py            # 主程式 範例展示
├── configs/                    # [設定檔資料夾] 
│   ├── cards.csv               # [設定檔] 真實卡號放置地點
│   ├── bank_config.yaml        # [設定檔] 銀行用資料設定
│   ├── transaction_types.yaml  # [設定檔] 銀行交易類別，排除持卡人跟銀行的交易像繳款、折抵/回饋、費用(手續費/服務費)
│   ├── merchants.csv           # [設定檔] 真實交易地點，使用Regex(正則表達式)-Replacement來清洗消費明細
│   └── payment_gateway.csv     # [設定檔] 電子支付平台，使用Regex(正則表達式)-Replacement來整理支付通路
├── data/                       # [帳單csv放置處] 真實的 CSV 帳單放這邊。
│   └── (各銀行帳單)
│
└── examples/                   # [公開展示] 由腳本生成的範本區
    ├── configs/                # 範例的設定檔
    ├── example_raw.csv         # 範例的髒資料
    └── example_refined.csv     # 範例的乾淨資料

```


---


## 🚀 專案路線圖與待辦 (Roadmap)

目前的開發重點在於擴充支援的銀行數量與優化 Regex 準確度。
以及各種輸入方式的改良。

### 支援銀行擴充
- [x] **玉山銀行**：已完整支援 (含 e.Point 折抵處理)
- [x] **國泰世華**：已完整支援 (含 Cube 卡多卡號歸戶邏輯)
- [x] **中國信託**：已完整支援
- [x] **華南銀行**：已完整支援 (含 副檔名 偽裝)
- [ ] **永豐銀行**：(下次更新時新增)
- [ ] **台新銀行**：徵求 CSV 格式樣本 (Help Wanted)
- [ ] **台北富邦**：徵求 CSV 格式樣本 (Help Wanted)

## 📅 開發日記 (Dev Log)

* **2026-02-07**
    * 建立 Mock Data Generator (generate_mock.py) 與隱私分流架構 (Himitsu.py)。
    * 重構專案檔案命名 (merchants.csv, payment_gateway.csv) 以符合工程慣例。
    * RFM記錄邏輯上傳
    * 支付規則(Regex)已上傳，整理商家規則(Regex)中

* **2026-02-02**
    * 開始分離EXCEL回饋紀錄邏輯跟跟RFM紀錄邏輯

* **2026-02-01**
    * 完成 `refine.py` 第一版。
    * 完成 自動降級機制（找不到真實檔時自動讀取範本）。

* **2026-01-28**
    * 重構了 `refine.py` 的邏輯。遇到一個 Bug：有些卡號末四碼會重複，後來決定加入「卡片名稱」作為第二鍵值來解決。
    * 新增了國泰 Cube 卡的雙號自動歸戶功能。
    * 消費明細關鍵字表(Regex)定稿

* **2026-01-20**
    * 專案初始化。完成第一版 ETL 架構 (`etl.py`)。
    * 變更資料流處理模式，從原本寫在Excel的回饋相關資料跟RFM關資料開始形成專案。

