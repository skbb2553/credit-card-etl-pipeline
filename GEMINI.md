# CardFlow Analytics 專案執行指令集 (GEMINI.md)


## 1. 編碼處理規範 (Encoding)
- **讀取階段**：處理台灣銀行之 CSV 或 PDF 匯出檔時，依序嘗試使用`UTF-8`→`Big5`→`cp950` 編碼，並在讀取到亂碼時更換指定的編碼。
- **寫入階段**：所有輸出的結果檔案（CSV、JSON 等）與專案內的設定檔更新，統一使用 `UTF-8` 編碼（含 BOM 或不含，視具體需求而定，預設不含）。

## 2. 架構維護與資料流程原則 (Architectural Integrity)
- **服務化架構 (Service-Oriented)**：核心邏輯應封裝於 `services/` 資料夾中（如 `etl_service.py`, `analysis_service.py`），以供 CLI 與 API 共同呼叫。
- **商家名稱清洗與合併規範**：
    *   **欄位 SSOT 定義**：
        *   `merchant`: 銀行原始名稱（不變）。
        *   `merchant_display`: 最終顯示名稱。
        *   `normalized_merchant`: 正規化後商家名稱。
        *   `payment_process`: 支付管道名稱。
        *   `ec_platform`: 電商平台名稱。
    *   **合併公式**：`[支付前綴]－[電商平台]－[正規化商家]`
    *   **處理順序**：
        1.  識別電商平台並標記於 `ec_platform`。
        2.  識別支付管道並暫存前綴。
        3.  執行商家正規化 (Merchant Normalization)。
        4.  執行最終合併，自動處理空值與全形連字號 `－`。
- **變數命名保護**：變數命名時應注意後續處理的一致性。
- **檔案編碼設定**：所有附檔名是.py跟特定資料夾(如output、configs)內的csv檔案，編碼均使用 `UTF-8` 編碼（預設不含BOM）。
- **資料修改範圍最小化原則**：
    1. 資料和程式邏輯修改範圍限制在使用者指定的範圍內，任何大範圍變更都先跟使用者確認。
    2. 不涉及任何改變程式邏輯的單純程式碼移動時，應注意在移動前後檔案與程式碼邏輯要完全一致，並在移動前後注意對應路徑調整。
- **目錄結構與層數規範**：
    1. **建議目錄層數上限**：以功能模組根目錄起算，最多放置兩層資料夾（例如：`etl/parsers/`）。超過三層（如 C# 專案深層目錄或特殊模組）需先與使用者確認。
    2. **API 介面與測試範本**：每個獨立功能模組（如 ETL、RFM）需提供統一的 API 介面說明與調度進入點（如 `api/` 或 `main_entry.py`），並於內部的測試目錄提供公開/脫敏的測試範本資料。
    3. **資料進出抽象化 (Loader Pattern)**：凡涉及資料庫或外部檔案讀寫的功能區塊，應設立 `loaders/` 模組或統一檔名，將資料讀寫邏輯與業務計算徹底分離。
- **.env檔案讀取邏輯設計**：
    1. 最後保底應以.env.example的設定為準，確保所有功能正常運作。
    2. 存在.env檔且.env檔內的設定符合.env.example的結構，則以.env檔為準。
    3. 不存在.env檔或.env檔內的設定不符合.env.example的結構，則讀取.env.example。
- **原則性禁止程式碼向後相容**
    1. 本專案性質偏向個人使用的應用程式，而且設計原則會傾向減少程式碼之間的引用。
    2. 此狀況下如果保留舊檔案會反向增加技術債跟認知混淆。

## 3. 核心變更驗證規範 (Refactoring Protocol)
針對影響資料處理流程（Data Pipeline）的核心更動（如：重構、配置分離、Parser 邏輯更新、`const.py`更動），必須遵循以下「雙軌驗證」流程：
1. **並行實作**：新邏輯應先以「註解狀態」加入 `main.py` 或核心模組，保持舊邏輯為 Active。
2. **基準測試 (Baseline)**：執行舊邏輯，記錄或備份輸出結果（如 `result_old.csv`）。
3. **對比測試 (Comparison)**：交換註解狀態，讓新邏輯 Activebing 並執行，記錄結果（如 `result_new.csv`）。
4. **內容比對**：使用比對工具（如 PowerShell `Compare-Object` 或內容雜湊值）確認兩份結果是否 100% 完全相同。
5. **決策原則**：
    - **完全一致**：正式移除舊邏輯與註解，完成變更。
    - **不一致**：重複進行修正與比對測試。
    - **中斷討論**：若嘗試 **3次** 後仍無法達成完全一致，必須立即停止操作，保留現狀並回報問題進行討論。

## 4. Git 操作前置檢查 (Git Push Checklist)
- [ ] 檢查編碼：確保沒有意外將 `UTF-8` 的設定檔轉存為其他編碼。
- [ ] 驗證設定：確保 `configs/` 內的規則檔在分離後仍能被正確讀取。
- [ ] 流程測試：執行 `main.py` 確保 `refiner` 邏輯運作正常且未被破壞。
- [ ] 排除敏感資訊：確保 `data/` 或 `output/` 目錄下的個人帳單資訊未被列入 stage。
- [ ] 注意：隱私相關檔案應被 `.gitignore` 排除，請再次確認。
- [ ] 注意：隱私相關檔案應被 `.dockerignore` 排除，請再次確認。
- [ ] 每一個issues處理完一個項目之後請提示進行一次關聯檔案的git add跟git commit

## 5. 待辦與待改善項目 (Backlog)
- [ ] 整理日後待更新的解析器 (Parsers) 清單。
- [ ] 建立自動化單元測試以覆蓋 `Append/Replace` 讀取邏輯。
- [ ] 前後端資料結構改善。
- [ ] **[核心] 回饋引擎開發**：開發具備「跨月份、上限控管、動態規則載入」功能的計算引擎，取代原本 Excel 難以維護的公式邏輯。
- [ ] **[結帳日管理] 建立 `configs/dim_billing_history.csv`**：記錄每張卡片各月份的「實際結帳日 (actual_closing_date)」，以應對假日浮動與歷史變更。
- [ ] **[引擎優化] `BILLING_CYCLE` 邏輯對接**：讓 `RewardsCalculator` 能根據事實表精確劃分消費所屬的帳單月份。

## 6. 回饋計算引擎實作規範 (Rewards Engine Implementation)
為解決 Excel 公式管理規則引用與「Before-After」對照困難的痛點，實作應遵循以下邏輯：
1.  **資料源讀取**：一律從資料庫 `all_transactions` 讀取，並在 SQL 階段利用 `WHERE` 排除 `繳款`、`紅利折抵`、`各項費用`。
2.  **瀑布式回饋引擎 (Waterfall Engine)**：
    *   **priority 排序**：由 `priority` 欄位決定執行順序（數字越小越優先）。
    *   **計算截斷 (Early Break)**：若規則 `reward_cal_break` 為 `TRUE`，匹配後即停止該筆交易的後續計算。
    *   **條件比對順序**：在單條規則內，比對順序為：**日期/卡別 -> 支付方式 (mobile_payment) -> 商家名稱 (merchant_display)**。
3.  **計算條件與日期處理**：
    *   **日期交集 (Date Intersection)**：規則最終適用區間 = `max(分配表起始, 定義表起始)` 至 `min(分配表結束, 定義表結束)`。
    *   **外部名單關鍵字**：支援在 `merchant_display` 欄位填入 `nccc_listed_merchant` 或 `general_reward_exclusion` 以自動引入對應 YAML 商家清單。
4.  **欄位對齊 (Schema)**：
    *   `bridge_reward_rules.csv` 關鍵欄位：`rules_reward_program`, `mobile_payment`, `merchant_display`, `start_date`, `end_date`, `merchant_rate`, `priority`, `reward_cal_break`。
    *   交易資料比對優先採用 `card_type` 作為卡片識別。

## 7. 內容輸出規範
1.  **README.md修改權限**：整個README.md的修改要事先詢問，並且僅提供文字跟修改建議。
2.  **暫時資料的輸出位址**：需要檢查的暫時資料請以csv格式並明確命名後輸出至 `output\` (該資料夾已被.gitignore忽略)，並提示使用者去該資料夾查看。 

## 8. 開發文件撰寫模式
1.在對話框中輸入提示詞「進入開發文件撰寫模式」開啟本模式，並在Gemini.md的第一行新增「目前為開發文件撰寫模式」。
2.在對話框中輸入提示詞「結束開發文件撰寫模式」來結束本模式，並移除Gemini.md的第一行「目前為開發文件撰寫模式」。
3.Gemini.md的第一行存在「目前為開發文件撰寫模式」時，對所有markdown以外的檔案均採取唯讀模式，僅能檢視，不做任何修改。
4.開發文件的輸出資料夾在docs/，輸出格式是markdown檔案。
5.路徑提示時請使用專案的相對路徑，確保所有檢視專案的視角一致。

