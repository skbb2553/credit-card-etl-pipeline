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

### 3. 商業邏輯增強 (Business Logic Enrichment)
* **RFM 準備：** 保留交易時間與頻率特徵，為後續的 **Recency, Frequency, Monetary** 分析建立基礎。
* **回饋關聯鍵：** 保留卡號特徵碼 (Last 4 digits)，作為後續計算「現金回饋率」的關聯鍵 (Foreign Key)。

---

## 🛠️ 開發方法論 (Development Methodology)

本專案採用 **AI 輔助開發 (AI-Assisted Development)** 模式，結合人類架構師的邏輯與 LLM 的算力。

* **Architecture (人類主導):** 定義資料流 (Data Flow)、Schema 設計、隱私邊界與商業目標。
* **Implementation (AI 加速):** 利用 Vibe Coding 模式快速生成繁瑣的 Regex 規則與 Pandas 語法。
* **Verification (嚴格審查):** 所有生成代碼皆經過人工 Code Review，並通過真實數據的邏輯驗證。

---

## 📂 檔案結構 (File Structure)

```text
.
├┬─ configs
│├─ banks_configs_example.yaml  #銀行設定檔_範本，檔名請修正成banks_config.yaml配合etl.py執行
│└─ mapping_rules.yaml  # 跟銀行關聯的交易關鍵字撈取規則
├── etl.py              # 主要 ETL 邏輯核心
├── refine.py           # 針對特定欄位的精細化清洗
├── load_to_db.py       # 清洗完之後存入資料庫
├── init_db.py          # 從資料庫中提取RFM規則、回饋規則
├── .gitignore          # 定義隱私過濾規則
└── README.md           # 專案文件

```


---


## 🚀 專案路線圖與待辦 (Roadmap)

目前的開發重點在於擴充支援的銀行數量與優化 Regex 準確度。
以及各種輸入方式的改良。

### 支援銀行擴充
- [x] **玉山銀行**：已完整支援 (含 e.Point 折抵處理)
- [x] **國泰世華**：已完整支援 (含 Cube 卡多卡號歸戶邏輯)
- [x] **中國信託**：已完整支援
- [ ] **台新銀行**：徵求 CSV 格式樣本 (Help Wanted)
- [ ] **台北富邦**：徵求 CSV 格式樣本 (Help Wanted)

### 功能改善
- [x] 實作 Excel to CSV 設定檔轉換器 (`convert.py`)
- [ ] 增加更多第三方支付的前綴識別 (目前支援 LinePay, 街口)
- [ ] 實作自動視覺化報表 (預計使用 Streamlit 或 Plotly)

## 📅 開發日記 (Dev Log)

* **2026-02-01**
    * 完成 `refine.py` 第一版。
    * 完成 `convert.py` 的自動降級機制（找不到真實檔時自動讀取範本）。

* **2026-01-28**
    * 重構了 `refine.py` 的邏輯。遇到一個 Bug：有些卡號末四碼會重複，後來決定加入「卡片名稱」作為第二鍵值來解決。
    * 新增了國泰 Cube 卡的雙號自動歸戶功能。
    * 消費明細關鍵字表(Regex)定稿

* **2026-01-20**
    * 專案初始化。完成第一版 ETL 架構 (`etl.py`)。
    * 變更資料流處理模式，從原本寫在Excel的回饋相關資料跟RFM關資料開始形成專案。

