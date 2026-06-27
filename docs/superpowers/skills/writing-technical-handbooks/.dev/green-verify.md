# GREEN Verify — 有 skill 重跑 baseline（Plan Task 11）

開發期記錄，**不 deploy**。同一題（用 `EXPLAIN` 讀 Postgres 查詢計畫的開章，給 SQL 分析師），這次派的 subagent **先讀 SKILL.md + references** 再做。對照 `baseline-red.md` 的 7 缺口。

派 general-purpose（sonnet）subagent，agentId `a0d64ea446e371c22`。

## 逐缺口核對：7/7 補上

| # | RED 缺口 | GREEN 是否補上 | 佐證 |
|---|---|---|---|
| G1 | 捏造技術輸出、無來源紀律 | ✅ | 開頭「mock 數字標示政策」段；每個輸出標 **【示意輸出】**；📚 footer 指 PostgreSQL 15 官方〈EXPLAIN〉 |
| G2 | 讀者無法動手（無造資料 snippet） | ✅ | §1.4 給完整 `CREATE TABLE` + `INSERT ... generate_series(1,500000)` + `ANALYZE`，附「需 15–60 秒」，純 SQL 可貼 GUI（對齊 P1「不寫 Python/CLI」） |
| G3 | 章體例不完整 | ✅ | 8 件齊：前提→目錄→§1.1–1.3→capstone(§1.4)→取捨→一句話帶走→導覽→📚 footer |
| G4 | 無結構化 persona | ✅ | **先產出 P1（地板）+ P2（stretch）spec 再下筆**，並驅動具體取捨（snippet 純 SQL、節點名稱從零解釋、索引不寫成普世規則） |
| G5 | 單次自審、無多角色 triage | ✅ | 列出 R1/R2/R3/C/P 各自要盯什麼 + 三級 triage 拍板原則 |
| G6 | 跨節一致性張力 | ✅ | capstone 全程同一張 `orders_demo`；情境 A/B total cost 12560 vs 5660 可手算差 55%，rows 差異有推算說明 |
| G7 | 無品質關卡意識 | ✅ | 明確知道改完跑 `anchor_check.py`（錨點/連結）+ `punctuation_audit.py`（標點輪證明沒改字） |

## 結論

GREEN 全數通過。RED→GREEN 的品質躍升（從「捏造輸出不標、無 snippet、無體例、無 persona」到「示意標注 + 可跑 snippet + 八件體例 + 雙 persona 驅動」）可直接歸因於 skill 的結構與 guardrails。

**注意**：GREEN subagent 同樣繼承專案 ambient 方法論（見 baseline-red.md 的污染註）。但它**明確讀了 SKILL.md + references 並引用其元件**（chapter-template 八件、persona-elicitation 六維、scripts 兩支），躍升歸因於 skill 成立。

## REFACTOR（Plan Task 12）：無漏洞可堵

GREEN 未暴露任何 skill 指引不清或缺口未覆蓋之處——7/7 一次到位、無新 rationalization。故 REFACTOR 不需改動 SKILL.md/references。品質關卡複核：`anchor_check.py $SRC` = OK（0 斷）。
