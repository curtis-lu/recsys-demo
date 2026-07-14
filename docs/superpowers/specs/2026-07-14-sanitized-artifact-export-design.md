# Spec：model/evaluation 產出物去識別化匯出 script

- 日期：2026-07-14
- 分支：`feat/pii-export`
- 落檔：`scripts/export_sanitized_artifacts.py`（＋ `tests/scripts/test_export_sanitized_artifacts.py`）
- 狀態：設計已與使用者確認，待實作

## 1. 目的與情境

把某個 model version 的訓練／評估產出物整包複製到一個「去識別化輸出夾」，過程中把個資（客戶識別碼＝**台灣身分證字號**）遮罩掉，讓這包能安全通過公司 VDI 的傳輸平台、交給公司的 AI agent 做**模型優化方向診斷**。

診斷需要的是聚合指標、特徵重要度、SHAP 貢獻、診斷圖與參數——**不需要客戶身分**。因此個資可以無損地移除。

## 2. 範圍

**做**：複製 `data/models/<version>/` ＋ `data/evaluation/<version>/`（所有 snap_date，或指定一個）到輸出夾，並遮罩身分證字號。

**不做**：不動原始 `data/`（唯讀來源）；不處理 cust_id 以外的欄位（人口特徵 age/gender/income 等**原樣送出**，診斷需要）；不上傳、不接觸傳輸平台本身（人工在平台操作）。

## 3. 使用者已確認的決策

| 項目 | 決定 |
|---|---|
| 安全策略 | **Blocklist**：先全複製，再刷除已知個資 |
| cust_id 處理 | **遮罩**（非移除、非假名化） |
| 個資範圍 | **只處理 cust_id**（＝身分證字號） |
| id 格式 | 台灣身分證：**1 碼英文字母 ＋ 9 碼數字**（`[A-Z]\d{9}`） |
| 遮罩粒度 | **預設全遮**（`--mask-keep 0`）——身分證的字母洩漏縣市、首位數字洩漏性別，故預設不保留任何碼 |

## 4. 來源與輸出結構

- 來源：
  - `data/models/<version>/**`（json、`model.txt`、`.pkl`、`diagnostics/*.png`、`diagnostics/*.json`…）
  - `data/evaluation/<version>/<snap_date>/**`（`manifest.json`、`report.html`…）
- 輸出：`--out/<version>/...`，保留原相對結構；同層放一份 `SANITIZATION_REPORT.json`。

## 5. 去識別化規則（核心）

1. **整棵複製**來源到輸出（`shutil`），絕不修改來源。
2. **判定 text/binary 再刷除**：
   - **已知二進位副檔名**（`.png .jpg .jpeg .gif .pkl .parquet .bin .zip .gz .pdf .xlsx`）→ 走 §5.3 原樣複製。
   - **其餘一律當文字處理**（含**未知副檔名**）——fail-safe：沒見過的新產出型別預設會被刷，符合「別讓沒預期到的個資漏出」。文字檔讀入內容，用 `--id-pattern`（預設 `\b[A-Z][0-9]{9}\b`）做 regex 取代成遮罩值，寫回**輸出檔**。
   - 「掃值」而非「掃欄名」：因此 `cases_manifest` 的 id 值、`report.html` 表格內嵌的 id、任何嵌入列的 id 都會被抓到；而設定裡的欄名字串 `"cust_id"`（不符 pattern）**原樣保留**（那不是個資）。
   - 遮罩規則：`--mask-keep N`（預設 0）保留前 N 碼，其餘以 `--mask-char`（預設 `*`）等長替換。例：預設下 `A123456789 → **********`。
   - `--id-pattern` 可**重複給多個**（例：再加舊式居留證 `[A-Z]{2}[0-9]{8}`）。
3. **二進位檔**：**原樣複製**（byte-for-byte），無法 regex（殘餘風險見 §8）。另：非已知二進位副檔名但**無法以 UTF-8 解碼**的檔，退回當二進位原樣複製，並在報告標注「無法解碼、未刷、請人工確認」。
4. **兜底掃描**：刷完後**重掃全部輸出文字檔**找殘留的 `--id-pattern` 命中。
   - 有殘留 → 印出 `檔案:行號` 並 **exit≠0**（避免把漏網的一包送出關）。
   - `--dry-run`：只做掃描與報告、**不寫輸出**。

## 6. 稽核報告 `SANITIZATION_REPORT.json`

至少含：來源 version、輸出路徑、時間戳（`datetime.now(timezone.utc)`；測試不斷言確切值）、複製檔數、**每檔遮罩命中數**、使用的 id-pattern 與遮罩規則、殘留掃描結果（應為 0）。出關前有一份可稽核紀錄。

## 7. 約束

- **純 Python 標準庫**（`argparse json re shutil pathlib sys`）——不依賴 repo 的 `.venv`／pandas，才能在鎖死的 VDI 直接跑。
- No network、no extra packages（合乎專案生產鐵則）。
- 非破壞性：只寫 `--out`，來源唯讀。

## 8. 殘餘風險與已知取捨（誠實標註）

- **Blocklist 的本質限制**：不符 `--id-pattern` 的識別碼會漏。整個 script 的成敗綁在 pattern 正確——預設 `[A-Z]\d{9}`（台灣身分證），與使用者確認。
- **外籍居留證**：舊式為 2 英文＋8 數字（`[A-Z]{2}[0-9]{8}`），不符預設。若客群含外籍人士需 `--id-pattern` 補上；預設會漏。
- **`.png` / `.pkl` 不刷**：診斷圖（shap_summary、waterfall）標題不含 id，但無法程式化保證圖內未渲染 id；`.pkl`（calibrator）為模型參數、不含 id 但也不可 regex。列為殘餘風險——若擔憂可用 `--exclude-binaries` 排除（實作時提供）。
- **遮罩的可還原性**：全遮下無殘留身分資訊；若使用者改用 `--mask-keep N` 保留前綴，會洩漏縣市／性別，且配合其他準識別碼有被反推風險。預設全遮即為此。
- **false positive**：`[A-Z]\d{9}` 可能誤中非 id 的字母＋9數字字串。對這批產出物（指標／SHAP／參數）幾乎不可能，且「誤遮」比「漏遮」安全，接受。
- **檔名不刷**：只刷檔案「內容」，不改**檔名／路徑**。本產出物檔名為索引／象限式（如 `waterfall_high_0.png`），不含 id；且 `\b` 錨定對底線相連的 id（`x_A123456789`）本就不可靠。若未來有以 id 命名的產物，需另行處理——目前列為已知邊界。
- **報告自身**：`SANITIZATION_REPORT.json` 內的 `residual[].match` 在寫檔時已遮罩，報告本身不夾帶原始 id；原始值僅在 stderr（VDI 上、短暫）印出供排查。

## 9. 測試計畫（TDD）

`tests/scripts/test_export_sanitized_artifacts.py`（`from scripts.export_sanitized_artifacts import ...`）：

- fixture 造含假身分證（如 `A100000001`）的 `.json` / `.txt` / `.html`，斷言值被遮成 `**********`。
- 斷言設定欄名字串 `"cust_id"` 原樣保留。
- 斷言二進位檔（造一個假 `.png` bytes）原樣複製、內容 byte-for-byte 相同。
- 斷言故意在輸出塞一個漏網 id 時，兜底掃描能抓到並 exit≠0。
- 斷言 `SANITIZATION_REPORT.json` 的每檔命中數正確。
- 斷言 `--dry-run` 不產生輸出檔。
- 斷言來源檔未被修改（唯讀）。
- 斷言 `--mask-keep 2` 時 `A123456789 → A1********`（可調性）。
- 斷言多個 `--id-pattern` 同時生效（身分證＋舊式居留證）。

## 10. 待確認 / 開放項

- 真實 VDI 上 cust_id 是否確為 `[A-Z]\d{9}`（已與使用者確認為台灣身分證格式）。
- 是否需要涵蓋外籍居留證格式（預設不含，`--id-pattern` 可補）。
