---
status: accepted
date: 2026-09-28
---

# training pipeline 第二輪整理：模型接縫說實話、評分與 inference 共用、可以跳過 HPO

> **判準不在這裡。** 形狀判準的唯一真實來源是 [`pipeline-node-design.md`](../agents/pipeline-node-design.md)（本份稱「node 規則 N」），流程判準是 [`pipeline-refactor-process.md`](../agents/pipeline-refactor-process.md)（「flow 規則 N」）。本份只記：把判準再套一次到 training 時，每條岔路選了哪一邊、為什麼。
>
> - **[ADR-0014](0014-training-modules-split-by-role.md) 大部分仍然有效。** 被本份取代或關閉的是決定 6（診斷 node 留在原地），以及〈刻意不做〉第 1、7、8 條的觸發條件；細節見文末〈對既有 ADR 的修訂〉。
> - **證據在另一份。** 程式形狀的逐條盤點、效能數字與它們過期了沒有、未結事項、跨 pipeline 共用的現況，都在 [`docs/notes/2026-09-27-training-pipeline-inventory.md`](../notes/2026-09-27-training-pipeline-inventory.md)（與本份同一批進 main）。
> - 下文的「這次」指「這一次執行 pipeline」；指這份文件時寫「本份」。
> - **這份會被修正。** 它寫在實作之前。實作時發現某條站不住，照 flow 規則 2 回頭改這裡，不能只寫在 commit message。
> - **行號會腐爛**，所以只給檔名與函式名。現況核對於 2026-09-27，main @ `77adca4e`。

---

## 這份在解什麼問題

ADR-0014 那一輪（#222，2026-08 底）把 `pipelines/training/nodes.py` 的 node body 整理好了：node 寫決策、機制進 `steps/`。之後 training 又長了一個月（HPO 目標可切換、排序目標、樣本權重不再烤進 `.bin`、test 指標由設定決定、拿掉校準……）。2026-09-27 的盤點看到的問題有六種形狀：

1. **模型接縫是假的。** `ModelAdapter`（`models/base.py`）本來該是「換演算法只換這一層」的地方，但呼叫端直接傳 LightGBM 的原生資料集、直接讀 `.booster`，HPO 與重訓也在 adapter 外面自己組 LightGBM 的參數。結果是 LightGBM 的名字散在約 13 個模組裡：加一個別的演算法，至少要改 13 處。
2. **決策沉在 node 以外的三個地方。**
   - `LightGBMAdapter.prepare_train_inputs`：311 行，裡面有快取格式、丟組政策、權重不進 `.bin`、「快取還能不能用」的檢查。呼叫它的 node `prepare_lgb_train_inputs` 只有 27 行轉手（node 規則 3 的反例形狀）。
   - `diagnosis/model/` 的 7 個診斷 node：最長的 `compute_shap_diagnostics` 223 行，只標了 1 個 `# Decision`。
   - `__main__.py` 的 `training()`：260 行，全是 training 專屬的開跑檢查與版本解析。
3. **同一件事，兩份做法。**
   - training 在 test 上預測（`predict_and_write_test_predictions`）和 inference 替整批 entity 評分（`predict_and_write_scores` 一帶），做的是同一件事：輸出表的組法各寫一份；特徵清單一邊由設定推出、一邊問模型；inference 寫出前每一塊都檢查，training 不檢查。
   - 把 parquet 列變成特徵矩陣有兩套：訓練走 `io/extract.py` 的 `_stream_matrix`，預測與 SHAP 走 `pdf_to_X`。類別欄的編碼各寫一份，`pdf_to_X` 還用了同檔已知慢上千倍的挑欄寫法（#418 第 2 項）。
4. **版本號看不見程式的改動。** `model_version` 與 HPO 的 `search_id` 只雜湊設定。程式改了、訓練出來的模型變了、設定沒動時：預測會把舊月份當成「已經寫過」而跳過，新舊模型的預測混在同一個版本底下；HPO 續跑會接上舊程式跑出來的 trial。`.bin` 快取也有同一個問題：今天在 `prepare_train_inputs` 中間用兩段檢查、三個條件擋（#418 第 1 項），格式每變一次就再加一段。
5. **讀程式就看得到的浪費。**
   - HPO 先把 val 讀成磁碟映射矩陣（程式註解：生產 37–89 GiB），**之後**才問 study 還剩幾個 trial。
   - 預測為了列出「有哪些（月份, item）要寫」，把全部 test 列的兩個分區欄拉進 driver（程式註解：約 2.2 億列）；每個分區也讀全部欄。
   - 建 `.bin`（排序目標）與 `refit_on_full` 重訓時，各把整個訓練矩陣再複製一份，峰值約兩倍。
   - `select_shap_population` 讀預測表與 `test_model_input` 時沒有月份篩選，成本跟著預測表的歷史長。
6. **加一種新診斷要改 5–7 處，而且有一處靠運氣。** `log_experiment` 把整個 `diagnostics/` 資料夾上傳 MLflow。`gain_ledger.json` 不是它的輸入，能在上傳前寫好，是拓撲排序剛好把 `compute_gain_ledger` 排在前面。框架傳參數只看位置，所以每加一個診斷，`log_experiment` 的參數清單就要在最後面多一格。

另外有兩個新需求：

- **可以跳過 HPO**，直接用使用者給的超參數訓練。
- **要能擴充**：兩階段建模（先把資料分組、每組一個第一階段模型，再用一個第二階段模型合併排序。設計檔 `docs/superpowers/specs/2026-07-23-staged-modeling-design.md`，寫本份時在分支 `feat/staged-modeling`，重寫時照它。設計檔把這種分組叫「分群」，跟 `CONTEXT.md` 的 **分群** 撞名，本份暫稱「分組」，正式名字留給兩階段 spec 定）、其他演算法、新的診斷、交接包（#382）。這些本份都不實作，但新形狀不能擋住它們。

**效率的範圍刻意收窄。** training 的時間幾乎都在 HPO 的 `lgb.train`（2026-09-07 本機合成資料量測，約 99%；秒數已因之後的改動過期，比例大致仍成立，見盤點筆記第二節）。本份的效率決定只收「讀程式就確定是浪費、而且不改變輸出」的；會改變挑出哪個模型的加速（提早喊停差的 trial、少試幾組、改 `max_bin`、同時試好幾組）另寫一份 spec，要先在公司環境量。

---

## 決定總表

| # | 決定 | 會改到的東西 | 落地產物會不會變 |
|---|---|---|---|
| 1 | `ModelAdapter` 說實話：training 用到的模型操作都進介面，包含診斷要用的 | `models/`、`steps/hpo_scoring.py`、`steps/refit.py`、`nodes.py`、診斷機制 | 不變 |
| 2 | 評分入口在 adapter：吃一張表、回分數 | 同上＋inference 評分 | 不變 |
| 3 | 演算法專屬的設定規則由 adapter 宣告，開跑前的檢查去問 adapter | `core/group_utils.py`、`core/consistency.py`（A7 與新的 `training.algorithm` 檢查） | `training.algorithm` 打錯改在 Spark 啟動前被擋 |
| 4 | 模型做不到的診斷：跳過並警告；畫圖失敗：只警告；其他錯誤：整條停下 | 7 個診斷 node、`diagnosis/metric/model_capacity/` | 今天被吞掉的診斷錯誤（畫圖以外）會讓 training 停下；診斷產物多一種「模型做不到」的形狀 |
| 5 | 寫出評分結果前的守衛與組表，放一個頂層模組共用；只共用機制 | 新模組 `recsys_tfb/score_output.py`；training 與 inference 的評分 node | training 的預測寫出前多了檢查；分數欄名改照 `schema` |
| 6 | 7 個診斷 node 搬回 `nodes.py`，判斷寫在 node，機制進 `pipelines/training/steps/` | `diagnosis/model/`（`paths.py` 除外） | 不變 |
| 7 | 診斷的圖交給 catalog 存 | 新的 catalog dataset 型別 | 不變（圖檔同名同位置） |
| 8 | `Node` 可以照名字傳參數；每個診斷 node 都接成 `log_experiment` 的輸入 | `core/node.py`、`core/runner.py`、`pipeline.py`、架構稽核測試 | MLflow 收到的檔案一定齊全 |
| 9 | 兩個格式版本號：模型的進 `model_version` 與 `search_id`；預測的記在預測的 manifest | `core/versioning.py`、`steps/predict_months.py`、catalog（讀回 manifest 的條目） | **每個部署的 `model_version` 變一次** |
| 10 | `.bin` 快取：決定內容的東西全進路徑，加快取格式版本號，刪掉過期檢查 | `.bin` 的快取邏輯 | 第一次跑重建一次 `.bin`；模型不變 |
| 11 | 可以跳過 HPO：`training.hpo_enabled` ＋ `training.fixed_params`，流程圖照設定長 | `pipeline.py`、新 node、`core/consistency.py`、`core/versioning.py` | 新設定鍵進雜湊（見〈版本與順序的約束〉） |
| 12 | 四件讀程式看得到的浪費；象限診斷只讀這次的 test 月份 | HPO、預測、`.bin`／重訓、`pdf_to_X`、`select_shap_population` | 四件浪費：輸出不變；象限診斷的母體與同分規則會變 |
| 13 | training 根層一個開跑契約模組 | `__main__.py` 的 `training()`、新 `pipelines/training/run_contract.py` | 不變 |
| 14 | 新增機械檢查：pipeline 之間不互相 import，函式庫不 import pipeline | 架構稽核測試、`architecture-constraints.md` | — |
| 15 | 拆掉 `io` 與 `models` 之間的 import 循環 | `io/__init__.py` | 不變 |
| 16 | 收尾清理 | 見該節 | 不變 |

**涵蓋的票**：
- #418：第 1 項由決定 10 解決（不是照原案整理成檢查清單，理由見該節）；第 2 項由決定 12 解決；第 3 項由決定 6 解決；第 4 項已由 #451 解決。
- #387：只有 `training.algorithm` 那一列由決定 3 解決。版本參數與 `catalog.yaml` 格式在其他 pipeline 擋得太晚的部分，留在 #387。

---

## 決定 1　`ModelAdapter` 說實話

**規則**：training 對模型做的每一件事，都經過 `ModelAdapter` 的介面；`pipelines/training/` 底下（`nodes.py` 與 `steps/`）不 import `lightgbm`、不讀 `.booster`，也不用 shap 的 Explainer 算歸因。用 `shap.summary_plot` 畫圖不在此限：畫圖只吃算好的歸因值，跟模型無關。介面要說出這幾件今天在用、但 ABC 沒寫的事：

- 把陣列建成這個演算法的原生訓練資料，並存到磁碟、從磁碟讀回（今天的 `.bin`）。
- 用給定的參數訓練，帶早停，拿 train_dev 當早停的依據；回傳最佳迭代數。
- 評分（決定 2）。
- 存檔、讀檔。
- **診斷要用的模型操作**，這幾件是選用能力，做不到就丟決定 4 的專用例外：
  - 特徵歸因（今天 `diagnosis/model/attribution.py` 用 `shap.TreeExplainer` 對 booster 算 SHAP）。
  - 樹的切點與 gain（今天 `diagnosis/model/gain_ledger.py` 讀 `booster.trees_to_dataframe()`，並解析 LightGBM 的類別切點格式）。
  - 內建的特徵重要度（今天 `feature_importance(kind="split"|"gain")`，`split`／`gain` 是 LightGBM 的字彙）。

LightGBM 專屬的解析（TreeExplainer、`trees_to_dataframe`、類別切點格式）搬到 adapter 那一側（`models/`）；診斷 node 與 `steps/` 只拿到跟演算法無關的結果，在上面做「依 item 彙整」這類診斷自己的決定：
- 每列每個特徵的歸因值。
- 樹的切點結構：每棵樹的節點、父子關係、深度、切點用的特徵與 gain、類別切點把哪些類別分到哪一邊。gain 帳本要沿著樹走，才算得出 evaluation 讀的巢狀帳本；只給「切點用了哪個特徵、多少 gain」的扁平清單不夠。

**`.bin` 的決策上浮到 node。** `prepare_train_inputs` 裡「快取能不能用」「排序目標要丟哪些組」「權重為什麼不進 `.bin`」這些是 pipeline 的決策，搬回 `prepare_lgb_train_inputs`（node 名可以改，不再帶 `lgb`）；機制進 `steps/`；adapter 只留「把陣列建成原生資料」，不再自己讀 parquet。

**用假模型驗證接縫。** 本份不做第二個演算法（見〈考慮過、沒選的做法〉）。接縫是不是真的，用測試裡的一個假 adapter 驗：它實作同一個介面、宣告自己的規則（決定 3）、對樹的切點這類能力丟「做不到」，training 的 node 要能照常跑完。

**兩階段建模的 12 個需求當檢查表。** 設計檔要求 training 提供的接縫有 12 條（逐條現況在盤點筆記第三節）。本份的新形狀要逐條確認「不擋住」，不要求「提供」。12 條裡有 2 條今天已經支援（分組鍵掛進 `dataset.carry_columns`、新的設定鍵自動進 `model_version` 的雜湊）；本份會直接提供 3 條：可替換的模型 adapter（決定 1、2、3）、照設定長出不同流程圖的做法（決定 11）、診斷能分辨「這個模型做不到」（決定 4）。其餘 7 條（多模型訓練、一個版本存多個模型檔、各組自己的 HPO、訓練時的分組資料閘……）留給兩階段 spec。

**為什麼**：一個 adapter 的接縫只是假設，兩個 adapter 才是真的接縫。第二個會是兩階段的**組合模型**：把各組的第一階段模型與第二階段模型包成一個 adapter；在它來之前先讓介面說出 training 真正在用的操作，組合模型來時才是「加一個 adapter」，而不是「加一個 adapter，再改 13 個模組」。

> **實作註記（2026-09-29，#482）**：
> - 介面的名字：`build_train_data`（陣列變成原生訓練資料，還沒分箱）、`save_train_data`、`load_train_data`（讀回時帶上這次的權重；權重筆數跟資料列數對不上就丟 `ValueError`）、`train(train_data, params, *, num_iterations, early_stopping_rounds, valid_data)`、`best_iteration`。都在 `models/base.py` 的 `ModelAdapter`。
> - `best_iteration` 在沒有早停的訓練（沒給 `valid_data`，或 `early_stopping_rounds` 是 0）時是 0。這是 LightGBM 自己的慣例，ABC 的 docstring 寫明了。
> - `build_train_data` 回傳**還沒分箱**的原生資料，跟 refit 以前自己建的 Dataset 一樣：由 `lgb.train` 連同訓練參數一起分箱，refit 的建法不因為多了這個方法而改變。（會影響分箱的參數，例如 `max_bin`，今天其實到不了 refit：trial 讀的是已經分好箱的 `.bin`，LightGBM 在第一個 trial 就會拒絕它。這是改動前就有的限制，本票沒動。）
> - `save_train_data` 拒絕帶權重的資料（丟 `ValueError`），把「權重只在讀取時套上、不存進檔案」從說明變成擋得住的規定。理由：LightGBM 讀回時若給全 1 的權重，不會蓋掉檔案裡存的權重，之後一次不加權的執行會拿舊權重訓練（#318 的形狀）。#483 把建 `.bin` 搬進 node 時，照抄 refit 那段 `weight=` 的寫法就會撞上。
> - `feature_pre_filter: False` 收成 LightGBM adapter 裡一個常數，建資料、讀資料、訓練三處都用它。訓練參數裡它改由 adapter 最後蓋上去，不再排在 trial 參數前面。兩種寫法只在「搜尋空間或 `algorithm_params` 自己寫了 `feature_pre_filter`」時結果不同，而那種設定今天在 trial 裡就會被 LightGBM 拒絕（已分箱的 `.bin` 不接受別的值）。
> - 疊參數的順序收成 `pipelines/training/steps/fit_params.py` 的 `fit_params`，trial 與 refit 共用。`num_iterations`、`early_stopping_rounds` 改成 `train()` 的參數，不再放進參數 dict；`params` 裡若還留著這兩個鍵，LightGBM adapter 會拿掉，照樣以 `training.num_iterations`／`early_stopping_rounds` 為準，跟以前一樣。
> - `LgbDatasetHandle.load()` 拿掉，讀回 `.bin` 改由 adapter 做；handle 只剩路徑與旁邊的 sidecar。
> - **更正〈為什麼〉**：上文說第二個真的 adapter 會是兩階段的組合模型。照兩階段設計檔現在的寫法（§7），組合模型只實作 predict／save／load，訓練照舊逐組用單一模型的 adapter。所以本決定的**訓練那一半**，第二個實作是測試裡的假 adapter（`tests/fake_adapter.py`）；組合模型驗到的是評分那一半（決定 2）。要不要把介面拆成「只能評分」與「可以訓練」兩層，留給兩階段 spec 決定。拆之前的已知後果：一個只能評分的 adapter 註冊進登記表，會通過 A57，並在 A7 因為沒有 `rules` 而出錯。
> - `.bin` 快取的判斷（快取能不能用、丟哪些組、權重為什麼不進 `.bin`）照票的範圍**沒有動**，仍在 `LightGBMAdapter.prepare_train_inputs` 裡，只是它內部改用 `build_train_data`／`save_train_data` 建檔；搬到 node 是 #483。診斷還在讀的 `.booster` 也還在，是 #485。

## 決定 2　評分入口在 adapter：吃一張表、回分數

**規則**：「拿一張表，用這個模型打分數」是 adapter 的事：挑模型自己要的特徵（以模型為準，`models/feature_view.py` 的 `model_feature_view`）、把類別換成編號、建矩陣、預測。training 的 test 預測、inference 的評分、SHAP 類診斷要「模型眼中的矩陣」時，都走這個入口。預設實作共用 `io/extract.py` 的編碼路徑（決定 12 第 4 件）；組合模型覆寫它。

adapter 也說出「評分要讀哪些欄」：模型的特徵，加上組合模型路由要用的分組鍵。決定 12 第 2 件用它決定每個分區讀哪些欄。

**為什麼放在 adapter 後面，不放在一個共用的評分層**：單一模型與組合模型之間真正不一樣的，就是「表 → 矩陣 → 分數」這一段。兩階段設計檔的 D2 規定分組鍵取自 `sample_pool` 的欄，§5 與 D9 讓它掛進 `dataset.carry_columns`，所以分組鍵可以不在特徵裡；第二階段的矩陣裡還有第一階段的分數。「這個模型要看什麼樣的矩陣」只有 adapter 自己答得出來。把建矩陣放在 adapter 前面，組合模型一來就得同時改 adapter 與那個共用層。

**翻盤條件**：如果兩階段 spec 決定分組鍵只能是本來就在特徵裡的欄（例如 item），而且 adapter 維持只收矩陣，分組資訊就已經在矩陣裡，「改兩處」的問題消失。那時開一個頂層評分模組、adapter 只收矩陣，是比較簡單的答案。

> **實作註記（2026-09-29，#484）**：
> - 名字：`ModelAdapter.score(table, preprocessor, parameters)` 吃一張表、回每列一個分數；`ModelAdapter.scoring_columns(preprocessor)` 回「評分要從表讀哪些欄」。兩個都在 `models/base.py`，**有預設實作、不是 abstract**：`scoring_columns` 就是 `models/feature_view.py` 的 `model_feature_columns`（模型自己的特徵清單，對不上前處理產物時照舊丟錯）；`score` 是 `predict(pdf_to_X(table, model_feature_view(self, preprocessor), parameters))`。組合模型覆寫這兩個。
> - 兩個方法裡的 `io.extract`、`models.feature_view` 都在函式內 import：後者本來就 import `models.base`；前者放模組頂層會讓單獨 `import recsys_tfb.io.model_adapter_dataset` 失敗，理由同 `models/lightgbm_adapter.py` 開頭那段（決定 15 的註記）。`tests/test_models/test_cold_imports.py` 照樣綠。
> - training 的 test 預測、inference 的評分都改走這個入口；`tests/test_models/test_adapter_contract.py` 在 LightGBM 與測試用的假 adapter 上各跑一遍評分合約（分數等於對原陣列 `predict`、欄順序打亂與多餘的欄不影響分數、`scoring_columns` 等於模型的 `feature_names()`）。
> - **跟上文字面不同**：SHAP 類診斷（`diagnosis/model/shap_per_item.py`、`shap_cases.py`）**沒有**改走這個入口，仍直接呼叫 `pdf_to_X`——不在 #484 的範圍。
> - inference 以前在迴圈外建一次模型的特徵 view，現在每個 `(桶, item)` 呼叫 `score` 時都重建一次（清單運算，跟預測比可以忽略）。「模型沒宣告 `feature_names()`」的 INFO log 因此每個 chunk 印一次；只有測試替身會走到那條路。

## 決定 3　演算法專屬的設定規則由 adapter 宣告

**規則**：每個 adapter 宣告自己的規則：支援哪些 objective、哪些算排序目標、排序目標能配哪些 metric、沒寫 metric 時用哪個、排序目標要不要丟整組沒有正例的 query group。今天這些是 LightGBM 的知識，住在 `core/group_utils.py`（`RANKING_OBJECTIVES` 與幾個 `objective_*` 函式）和 `core/consistency.py`（A7 的 `RANKING_METRICS`），本份把它們搬進 LightGBM 的 adapter。

開跑前的檢查在函式內 import `models` 的登記表（`ADAPTER_REGISTRY`），去問 adapter。`training.algorithm` 沒有註冊的檢查（#387）也照這個做：檢查與實際分派讀同一份登記表，在 Spark 啟動前擋下。

**登記表要先填好。** LightGBM 的註冊寫在 `models/lightgbm_adapter.py` 最後一行，靠 `models/__init__.py` import 那個模組才會執行。檢查若只 import `models.base`，登記表可能是空的，會把合法的 `lightgbm` 擋掉（見決定 15）。

**為什麼可以讓 `core/` 在函式內問 `models/`**：這是 repo 已經在用的形狀。`core/consistency.py` 檢查 `training.hpo_objective` 時，就在函式內 import `evaluation/metric_registry.py` 的 `METRIC_NAMES`，理由相同：檢查與分派要用同一份清單，清單屬於上層。函式內 import 不會在載入 `core` 時把上層拉進來。

**為什麼這一輪就搬**：搬的是**已經存在**的 LightGBM 規則，不是替還不存在的演算法蓋東西；而且會碰到的地方（`nodes.py` 改成經過 adapter）決定 1 本來就要改。留到下一次，repo 會同時有兩種寫法。測試裡的假 adapter 宣告自己的規則，順便驗證「檢查真的有去問 adapter」。

> **實作註記（2026-09-29，#482）**：
> - 規則是 `models/base.py` 的 `AlgorithmRules`，四項：排序目標、排序 metric、預設排序 metric、哪些目標丟掉整組沒有正例的 query group。每個 adapter 用類別屬性 `rules` 宣告；LightGBM 的是 `models/lightgbm_adapter.py` 的 `LIGHTGBM_RULES`。建的時候會自己檢查：預設 metric 必須是排序 metric，會丟組的目標必須是排序目標。
> - 「支援哪些 objective」落地成「支援哪些**排序** objective」，**沒有**另加一份所有 objective 的白名單。框架從來沒擋過非排序的 objective（例如 `regression`、`cross_entropy` 今天都能跑），加白名單會擋下今天能跑的設定；而決定總表寫本決定只改「`training.algorithm` 打錯什麼時候被擋」。
> - `training.algorithm` 的檢查代號是 **A57**（`core/consistency.py` 的 `training_algorithm_errors`），接在 training 指令上、Spark 啟動前，不進每個指令都跑的聚合檢查，理由同 A24：只有 training 讀這個鍵。A7 在聚合檢查裡，遇到沒註冊的名字時：metric 那一半跳過（哪些 metric 合用要問那個不存在的 adapter），交給 A57 報；query group 那一半照查，只要任何一個已註冊的 adapter 把這個 objective 當排序目標，`schema.entity` 就不能是空的。後者是為了不讓 algorithm 打錯字時，dataset 先白建一整個版本。
> - **更正上文「登記表要先填好」一段**：「檢查若只 import `models.base`，登記表可能是空的」不成立。Python 載入任何 `models` 的子模組之前，一定先跑完 `models/__init__`；而且載入 `core` 本身就會經 `core.catalog` → `io.model_adapter_dataset` 把 `models` 帶進來。真正會讓登記表變空的，是 `models/__init__` 拿掉對 LightGBM adapter 的 import；`tests/test_models/test_cold_imports.py` 在獨立行程裡守這件事（拿掉那行 import，測試會轉紅，已實測）。
> - `training.algorithm` 的預設值收成 `models/base.py` 的 `DEFAULT_ALGORITHM` 與 `configured_algorithm()`，training 的 node 與 A57 都讀它。`io/model_adapter_dataset.py` 在沒有 `model_meta.json` 時退回 LightGBM 的那一處沒動：那是讀舊模型檔的相容處理，不是設定的預設值。
> - `conf/base/parameters_training.yaml` 有一段註解還指向 `core/group_utils.py` 的 `objective_drops_zero_positive_groups`（函式已搬走）。本票要求 `conf/` 對 main 的 diff 為空，所以沒改；留給下一張會改那個檔的票（決定 11 的 #487 要在同一個檔加新鍵）。

## 決定 4　模型做不到的診斷跳過並警告，其他錯誤整條停下

**今天的行為**：
- 吞掉所有例外、印警告、繼續跑的有 3 個：`select_shap_population`、`compute_quadrant_profiles`、`compute_quadrant_cases`。
- `compute_shap_diagnostics` 沒有外層的吞錯，但有兩處局部吞錯：`background: per_item` 的能力探針（註解寫明：目前的 shap 版本解析不了 LightGBM 的類別切點，真的模型上一定失敗，失敗就降級成 `global`），以及每一張圖的繪製。
- `compute_feature_statistics`、`compute_feature_importance`、`compute_gain_ledger` 出錯就停。

**規則**：
- adapter 對它做不到的事丟一種專用的例外（決定 1 的選用能力）。shap 在某些模型上會丟任意型別的例外（例如解析不了類別切點），由 adapter 的歸因能力接住、轉成這種專用例外；診斷不直接接 shap 的例外。
- 診斷只接住這一種。整個診斷做不到就跳過；只有某個選項做不到就降級（`per_item` 背景探針屬於這一類：保留降級與警告，改成接專用例外，不接所有例外）。
- 其他例外一律往上拋，training 停下。
- **唯一的例外是畫圖**：某一張圖畫不出來，印警告、跳過那一張，其他照常（使用者 2026-09-28 裁決）。理由：圖只給人看，不影響任何數字，而且少了一張圖在 MLflow 上看得出來，不是靜默的錯。這個 best-effort 由決定 7 的圖存檔負責，不在 node 裡吞例外。
- **跳過時寫出的產物，要說得出「是模型做不到」。** 例：evaluation 讀 `gain_ledger.json` 今天有三態（檔案不存在＝evaluation 單獨跑、`{"enabled": False}`＝訓練側關掉、完整內容，見 `diagnosis/metric/model_capacity/_compute.py` 的 docstring），沒有「模型做不到」這一種。照舊寫 `{"enabled": False}`，evaluation 會把原因報成「訓練側關掉」；回傳 `None` 會被存成 `null`，報成「evaluation 單獨跑」。兩種都不會報錯。所以加第四種形狀（例如 `{"enabled": True, "supported": False}`，確切寫法實作時定），`diagnosis/metric/model_capacity/` 的讀法跟著多一種原因。其他會被別的 pipeline 讀的診斷產物照同一個做法。

**為什麼不是「全部吞掉」**：「做不到」是正常狀況，「出錯」是 bug。吞掉的警告出現在一個跑了好幾小時的 log 尾端，實務上沒人看見（ADR-0014〈刻意不做〉第 7 條已記過）。停下的代價小：`model` 在診斷之前就落地了，修好之後 `--from-node <出錯的診斷>` 接著跑，HPO 與重訓都不用重來。

**已知的後果**：`select_shap_population` 對排名結果 `persist` 的峰值，在生產資料量下有多大由 #238 追蹤。撐不住時，今天只會警告，本份之後會停下；遇到時先把 `diagnostics.shap.quadrant_enabled` 設成 `false` 跑完，再照 #238 量。

## 決定 5　寫出評分結果前的守衛與組表，放一個頂層模組；只共用機制

**規則**：新開頂層模組 `recsys_tfb/score_output.py`（名字是品味題，實作時可換，但要用領域命名，不叫 shared／common，node 規則 12）。它只放跟模型無關、兩條 pipeline 都成立的機制：
- 一次 save 只寫一個分區的守衛（今天的 `require_single_partition`，在 `pipelines/inference/steps/partitions.py`）。
- 逐塊的通用後置條件：列數對得上、entity 與分數沒有空值（今天 `validate_scored_chunk` 的一部分，在 `pipelines/inference/steps/validation.py`）。
- 輸出表的組裝機制。

**決策留在各自的 node**。兩邊看起來一樣、答案不同的至少有這些，共用時不得合併成一個帶旗標的函式（node 規則 5）：

| 題目 | training | inference |
|---|---|---|
| 重複列怎麼算（identity） | 含選用角色欄；宣告 `occasion` 或 `event` 時，一個 `(time, entity, item)` 本來就可以有多列 | 刻意不含選用角色欄（ADR-0025 決定 1） |
| 選用角色欄的型別 | 不轉字串（`event` 可能是時間戳記，同分規則照它自己的型別比） | 不寫出 |
| item 值域檢查 | 不需要：item 來自資料分區 | 需要：item 來自迴圈變數 |
| 要帶的欄、分區欄 | label、零正例組權重；分區 `(time, item)` | `entity_bucket`；分區 `(time, item, bucket)` |
| 續跑規劃 | 以月為單位、設定是權威 | 以塊為單位 |

`validate_scored_chunk` 的重複列檢查照原樣拿給 training 用，在宣告了選用角色的部署上會誤報。所以共用的是「給定 identity 欄，檢查有沒有重複」這個機制，identity 由呼叫的 node 傳入。

**分數欄名一律照 `schema`**。training 今天在兩個地方寫死 `"score"`：寫出 test 預測的組表，以及 `select_shap_population` 讀預測表時的排名與取極值的 window。inference 照 `schema` 讀。今天沒出事，是因為設定剛好也叫 `score`；改名那天 training 會寫錯欄，而且決定 4 之後 `select_shap_population` 會停下。兩處都改成照 `schema`。`select_shap_population` 輸出給象限案例（`compute_quadrant_cases`）的欄名 `score` 是兩個模組之間的約定，不是預測表的欄，不跟著改。

catalog 條目宣告的欄名由部署跟著 `schema` 寫。本份不加「catalog 宣告的分數欄跟 `schema` 一致」的檢查：inference 的表有同一個缺口，要加就兩邊一起加，不在本份範圍。

**為什麼不讓 training 直接 import inference 的模組**：repo 今天沒有任何一條 pipeline import 另一條。兩條 pipeline 是平輩，各自演進；training 依賴 inference 的內部，inference 一次重構就可能打壞 training。共用的東西一律往下搬到兩者之下，這是 ADR-0008 以來每一次的做法（`preprocessing.py`、`models/feature_selection.py`、`models/feature_view.py`），它的理由本身站得住，不只是慣例。

**共用之後多了一個觸發來源**：為了 inference 改評分程式，會改變 training 的預測。怎麼讓已寫過的月份重寫，見決定 9 的預測格式版本號。

> **實作註記（2026-09-29，#484）**：
> - 模組照上文叫 `src/recsys_tfb/score_output.py`，只 import pandas、numpy 與標準庫（`tests/test_score_output.py` 釘住 import 清單）。內容：`require_single_partition`（從 `pipelines/inference/steps/partitions.py` 原樣搬來，原檔的已刪）；`scored_chunk_failures(out_pdf, source_pdf, *, entity_cols, identity_cols, not_null_cols)` 回 0～3 個失敗，檢查名稱與訊息逐字沿用 inference 的 `chunk_row_count`／`no_missing`／`no_duplicates`；`require_scored_chunk` 是「有失敗就丟 `ScoredChunkError`」的版本；輸出表組裝是 `ScoredFrameLayout`（`entity_cols` 寫出時轉字串、`score_col`、`carried_cols` 照原型別帶、`null_cols` 整欄 NULL；`source_columns()` 說要從來源讀哪些欄，`build()` 組表）。
> - `null_cols` 是上文沒列的一項：寫出目標宣告了、這次卻沒有值的欄。今天只有一種——catalog 宣告了零正例組權重欄而 `test_zero_positive_group_ratio` 是 0。
> - inference 的 `validate_scored_chunk` 改成組合：前三條呼叫 `scored_chunk_failures`（identity 傳 `scored_row_columns`，不可為 NULL 的欄傳 identity 的非 entity 欄加分數），`item_values_are_known` 留在原處；丟的仍是 inference 自己的 `ValidationError`，`CHUNK_CHECKS` 不變。
> - training 寫出前的兩個選擇：重複列以 training 自己的 `identity_columns` 判斷（含選用角色欄，理由即上表第一列）；不可為 NULL 的只列這個 node 自己算出或賦值的欄——分數與兩個分區值（entity 在讀進來的列上查，因為轉字串會把 NULL 變成 `"None"`）。label、選用角色欄、權重是從 dataset 原樣帶來的，能不能是 NULL 是 dataset 的契約，這個 node 不替它擔保。失敗時丟 `ScoredChunkError`、該分區不寫出。**這是行為變更**：#484 以前 training 寫出前什麼都不查。
> - 分數欄名照 `schema.score`：training 寫出 test 預測那一處已改（#484 以前寫死 `"score"`）。**跟上文字面不同**：上文說的第二處——`select_shap_population`（`diagnosis/model/population_spark.py`）讀預測表時寫死的 `"score"`——#484 沒有改，不在這張票的範圍，仍待處理。
> - 兩邊都用 `ScoredFrameLayout` 組表，欄順序是 entity、分數、`score_uncalibrated`、帶的欄、NULL 欄、分區欄。宣告了選用角色時 training 的欄順序跟以前不同（以前選用角色欄排在分數前面）；`HiveTableDataset.save` 照表宣告的欄序寫，落地的表不受影響。

## 決定 6　7 個診斷 node 搬回 `nodes.py`；機制進 `pipelines/training/steps/`

**規則**：
- `compute_feature_statistics`、`compute_feature_importance`、`compute_gain_ledger`、`compute_shap_diagnostics`、`select_shap_population`、`compute_quadrant_profiles`、`compute_quadrant_cases` 的 `def` 搬進 `pipelines/training/nodes.py`。
- 搬法是**決策上浮**，不是照搬成轉手殼：預算閘、背景樣本降級、正例抽樣這類判斷寫進 node body，掛 `# Decision —`；跟演算法無關的機制進 `pipelines/training/steps/`；碰模型內部的部分走決定 1 的 adapter 能力。
- `diagnosis/model/paths.py` 的 `diagnostics_dir` 留在函式庫：HPO 的搜尋診斷（`diagnosis/hpo/paths.py`）也用它，搬進 `steps/` 會讓函式庫反過來依賴 pipeline，`architecture-constraints.md` 的 S3（pipeline 以外的模組不得 import 該 pipeline 的 `steps/`）也會轉紅。
- 搬的時候順便把這些模組的中文 docstring 與註解改成英文（體例），跨模組 import 的私有名改成公開名（node 規則 12）。

**為什麼**：使用者 2026-09-20 決定診斷不獨立成一條 pipeline，2026-09-27 再決定 7 個診斷全部保留，也不搬去 evaluation。`diagnosis/model/` 在 `src/` 裡只有 training 在用，留在頂層就是一個只有一個消費者的函式庫。搬進 `steps/`，目錄列表就說實話。哪天真的有第二個消費者，再往下搬是機械式的搬家。

**為什麼決策上浮，不照搬**：照搬會生出 7 個轉手殼，node 規則 3 的違例只是從「位置」換成「形狀」。使用者要能加新診斷，兩階段建模也要診斷分辨組合模型，這一塊之後會一直長，現在做對比較划算。

## 決定 7　診斷的圖交給 catalog 存

**規則**：`compute_shap_diagnostics` 與 `compute_quadrant_cases` 不自己 `savefig`。新增一種 catalog dataset 型別，負責把「一疊圖」存成 PNG（Kedro 的 matplotlib dataset 是同一個概念）。

node 回傳的是**每張圖的畫法**（一張圖一個函式），不是畫好的圖：catalog 存的時候才逐張畫、存、關。今天是畫完一張就關；一次持有全部的圖，以 22 個 item 算有一百多張，記憶體會一路累積。

某一張畫不出來時，這個 dataset 印警告、跳過那一張、繼續存其他的（決定 4 的畫圖例外），跟今天的行為相同。這是這個 dataset 型別明文的行為，只適用於診斷圖。

**為什麼**：node 不碰磁碟，測試可以直接呼叫畫法檢查圖；新診斷照同一個做法。另一條路是讓 node 自己存、兩個 node 登記進 R4（`architecture-constraints.md` A1 例外二），那要使用者簽核，而且把「自己寫檔」這個例外擴大。

## 決定 8　`Node` 可以照名字傳參數；每個診斷 node 都接成 `log_experiment` 的輸入

**規則**：`core/node.py` 的 `inputs` 除了清單，也接受 `{參數名: dataset 名}` 的 dict，Runner 照名字傳（Kedro 的 dict inputs 同一個語意）。清單寫法照舊能用。`Node.inputs` 這個屬性仍然是 dataset 名的清單（拓撲排序、切片、catalog 檢查都靠它），「參數名 → dataset 名」的對應另外存。`log_experiment` 改成照名字收每一個診斷 node 的產物，每個診斷 node 到 `log_experiment` 都有一條流程圖上的邊。

HPO 的搜尋診斷不在此列：它由 `tune_hyperparameters` 自己寫，而 `log_experiment` 已經經由 `best_params` 排在它之後。跳過 HPO 時沒有這份診斷，`log_experiment` 要允許它不存在。

**實作要注意的兩個陷阱**：
- **今天 `Node` 收到 dict 不會報錯**：`_normalize` 把 dict 轉成它的鍵的清單，再照位置綁。`log_experiment` 的參數名剛好等於 dataset 名，只做一半的實作會假綠。證明照名字綁的測試，要用「參數名不等於 dataset 名、而且順序故意打亂」的 node。
- **架構稽核讀 `inputs` 的方式看不到 dict**（`tests/test_core/test_architecture_constraints.py` 的 `_literal_names` 只認字串與清單），改成 dict 的 node 會被稽核靜默跳過。同一個 PR 要補，並照 flow 規則 6 拿 dict 寫法試它會紅。

**為什麼**：
- 今天 `gain_ledger.json` 能在上傳前寫好，靠的是拓撲排序剛好把它排在前面，中間沒有邊。
- 今天只能照位置傳，新的選用輸入只能加在最後、預設 `None`。`architecture-constraints.md` A1 例外一記過這種寫法的陷阱（它說的是 `writes` 為什麼照名字綁，同一個陷阱對選用輸入一樣成立）：位置錯了，尾端的 `=None` 會把錯誤吞掉。
- 加一個新診斷，從改 5–7 處降到約 4 處：node、`pipeline.py`、catalog、`log_experiment` 多一個具名參數。不再有「只能加在最後」的位置限制，也不再靠排序。

**連帶**：`architecture-constraints.md` 的 F4（Node 極薄）與 A1 例外一（「`inputs` 位置對應」）要跟著改寫。動 `core/` 之前照路由表先讀那一份。

## 決定 9　兩個格式版本號：模型的、預測的

「同一個 `model_version` 的預測不會變」是 test 預測跳過已寫月份的前提（`steps/predict_months.py`）；「同一個 `search_id` 的 trial 可以接著用」是 HPO 續跑的前提。程式改了、設定沒動時，這兩個前提都會靜默破掉。但兩種改動的代價差很多：重跑 HPO 在生產是小時級，重寫預測只是評分。所以分成兩個整數常數：

| | 什麼時候加 1 | 進哪裡 | 加 1 之後 |
|---|---|---|---|
| **training 模型格式版本** | 程式改了，會讓 HPO 的 trial 分數或訓練出來的模型不同 | `model_version` 與 `search_id` 的雜湊（`core/versioning.py`） | 版本號換，HPO 從頭搜、重訓、預測全部重寫 |
| **training 預測格式版本** | 程式改了，會讓寫出的 test 預測不同，但模型不變（例如決定 5 的共用評分程式為了 inference 改了） | 記在 test 預測的 manifest（`predict_manifest`） | 版本號不變、不重訓；下次跑時，記錄的值跟程式不同，所有月份的預測重寫 |

**上一次記的值怎麼讀回來**：`predict_manifest` 是 predict node 自己的輸出，不能同時當它的輸入（A6 在建構時就會擋）。另開一個指向同一個檔的 catalog 條目，給 predict node 讀；前例是 `preprocessor_on_disk`（ADR-0029 決定 13）。

加 1 的判準只看「輸出會不會變」，不看「程式有沒有動」，寫法照 node 規則 18（dataset 那邊的 `DATASET_ARTIFACT_FORMAT_VERSION`）。實作時把規則 18 擴充成涵蓋所有這類常數（dataset 產物格式版本、這兩個、決定 10 的 `.bin` 快取格式版本），或在 node 規則裡另加一條。

**為什麼現在加**：inference 還沒部署，現在加最便宜。

**為什麼分兩個**：只有一個的話，為了 inference 改一行評分程式，每個部署都要從頭重跑 HPO。預測格式版本不進 `model_version`，所以也不動 inference 的版本。inference 每個月各跑一次，不回頭重寫舊月份；它的續跑只接同一個月裡中斷的塊。評分程式在某個月跑到一半時換了，那個月要整月重跑。這對任何程式升級都成立，本份不另加機制。

## 決定 10　`.bin` 快取：決定內容的東西全進路徑

**規則**：`.bin` 快取的資料夾路徑，要包含所有決定它內容的東西：今天已有的 `base_dataset_version`、`train_variant_id`、objective 分段、特徵選擇分段（`fs_` 加雜湊），再加上 `training.sample_weight_keys` 的欄位組合（比照特徵選擇分段，用雜湊的前 8 碼，避免路徑過長或欄名含特殊字元），以及一個**快取格式版本號**。路徑存在、而且裡面有 `_SUCCESS`，就一定能用。今天那兩段檢查（三個條件）全部刪掉：

| 今天的條件 | 在抓什麼 | 之後由誰擋 |
|---|---|---|
| 沒有 `group_filter_counts.json` | #315 之前建的 `.bin`（沒丟零正例組） | 快取格式版本號：舊格式的路徑不一樣，找不到 |
| 沒有權重鍵 sidecar，或 sidecar 沒記權重鍵 | #318 之前建的 `.bin`（權重烤在裡面） | 同上 |
| sidecar 記的權重鍵跟今天的設定不同 | 改了 `training.sample_weight_keys` | 權重鍵進路徑：設定不同，路徑不同 |

**快取格式版本號與決定 9 的兩個常數分開。** 快取格式改了（例如多一個 sidecar）不一定改變模型；共用一個常數會逼所有部署重訓。

**為什麼不照 #418 第 1 項的原案**（把檢查整理成一份清單）：原案讓「格式再變一次」變成「清單多一個函式」，檢查還是會越疊越多。放進路徑之後，格式再變只要把版本號加 1。

**代價**：第一次跑會重建一次 `.bin`（比 HPO 便宜得多）；舊格式的快取留在磁碟上，要手動刪。

> **實作註記（2026-09-29，#483）**：
> - 路徑長這樣：`<cache.root>/<base_dataset_version>/train_variants/<train_variant_id>/train_data_v<N>/<algorithm>/<objective>/features_<hash8>/weight_keys_<hash8>/`。組法在 `pipelines/training/steps/train_data_cache.py` 的 `cache_dir`。快取格式版本號是同檔的 `TRAIN_DATA_CACHE_FORMAT_VERSION`，從 1 起；它不進任何版本 ID、只進路徑，所以放在用它的地方，不放 `core/versioning.py`。
> - **跟上文不同的兩處**。(1) 特徵分段改成**永遠存在**的 `features_<hash8>`（雜湊 node 拿到的特徵欄清單），不再是「特徵選擇打開時才有」的 `fs_<hash8>`。理由：只在某些設定才出現的分段，會讓一個快取目錄巢在另一個裡面；清掉外層建到一半的目錄時，內層建好的會一起被刪。格式版本那一段已經讓所有舊路徑失效，也就沒有「保留舊路徑不變」的理由。(2) 多了演算法名（`training.algorithm`）一段，取代原本寫死在 adapter 裡的 `lgb/`：組路徑的人從 adapter 變成 node，node 替每個 adapter 組，而兩個 adapter 的檔案格式不同。
> - 「路徑存在而且有 `_SUCCESS`」跟 parquet 快取共用 `steps/local_cache.py` 的 `is_partial_cache`／`cache_is_complete`／`mark_cache_complete`；`_SUCCESS` 這個名字收成 `io/handles.py` 的 `SUCCESS_MARKER`（決定 16 列的 3 處）。`cache.root` 只在 `resolve_cache_path` 讀一次，`.bin` 的目錄從它的 train 路徑往上一層組出；兩處預設值不同的問題因此消失，剩下的預設是原本 parquet 那邊的 `/tmp/recsys_cache`。
> - 三個條件連同 `io/handles.py` 的 `WEIGHT_KEYS_META`（sidecar 記「建的時候要的權重鍵」，只有被刪的檢查在讀）一起刪掉。grep 證據在 PR 說明。
> - 建到一半的目錄刪掉重建（`shutil.rmtree`）與建目錄（`mkdir`）跟著決策進了 node，架構稽核 (d) 的集合多一筆 `prepare_train_inputs`，使用者 2026-09-29 批准。這兩個寫入以前就有，只是在 `models/` 裡、掃描看不到。
> - node 改名 `prepare_train_inputs`（決定 1 說的「不再帶 `lgb`」）。LightGBM adapter 不再 import `io/` 的任何東西，決定 15 實作註記說的那幾處函式內 import 已經不存在。
> - 寫 sidecar 的兩個函式（`write_weight_keys_sidecar`、`write_group_filter_counts`）放在 `io/handles.py`，跟讀它們的 `LgbDatasetHandle` 同一個檔，沒有照「機制進 steps」放：檔案格式的寫與讀在同一處，改一邊時看得到另一邊。
> - **更正：上文「路徑存在、而且裡面有 `_SUCCESS`，就一定能用」說太滿。** 它管得到設定與程式（兩者都在路徑裡），管不到三件事，都不是本票造成的，本票只寫進文件：
>   - 上游回補。`train_variant_id` 從抽樣設定算，不看資料列，同一份設定下回補了資料，路徑不變。旁邊的 parquet 副本本來就是同一個缺口；只刪 parquet 不刪 `.bin` 時，HPO 用舊的列、`refit_on_full` 用新的列。`docs/pipelines/training.md` §3.4 與 `pipeline-slicing.md` 寫明要刪整個 `train_variants/<id>/`。
>   - 設定跟磁碟上的 dataset 版本不一致。training 讀的是 `latest` 指到的 dataset 版本，schema 取自當下設定；改了 schema 卻沒重跑 dataset 時，路徑不變、內容會變。改動前就是這樣。
>   - LightGBM 版本。生產環境釘死 4.6.0、不能自己加套件，所以沒放進路徑。哪天升級，快取格式版本加 1。
> - **給決定 9（#488）的建議，還沒做**：審查時實測，只把 query group 裡的列換個順序，lambdarank 的預測就差到 1.5（LightGBM 4.6.0，3,000 組 × 22 列）。所以改到 `.bin` 內容的程式改動，多半也改變模型，兩個版本號都要加；只加快取格式版本時，`search_id` 不變，接續的搜尋會把新舊檔案上跑出來的 trial 混在一起。`TRAIN_DATA_CACHE_FORMAT_VERSION` 的 docstring 已經寫明。可以再補一道：把決定 9 的模型格式版本也放進快取路徑，模型版本一加，`.bin` 就重建一次（代價遠小於 HPO），「加了模型版本、忘了加快取版本」這條路就不會靜默。這個常數在 #488 才出現，本票做不了；上文「快取格式版本號與決定 9 的兩個常數分開」的理由（快取改了不一定要重訓）不受影響，因為方向相反。要不要做，由 #488 決定。

## 決定 11　可以跳過 HPO

**設定**：
- `training.hpo_enabled`（預設 `true`）。設成 `false` 就不跑 HPO。
- `training.fixed_params`：跳過 HPO 時用的超參數。空的表示「除了 `algorithm_params` 不另外指定」，不是「全部用 LightGBM 預設值」：框架自己設定的鍵照樣生效（見下）。

**參數怎麼疊**：跟 HPO 的一個 trial 完全相同（`steps/hpo_scoring.py` 的 `TrialScorer.__call__`），`fixed_params` 站在 trial 參數的位置：`algorithm_params` → 框架設定的 `seed`、`feature_pre_filter` → `fixed_params` → `training.num_iterations`、`training.early_stopping_rounds`。所以迭代數上限與早停一樣由 `training.*` 決定，用 train_dev 早停，`final_model_strategy` 的兩種策略照常可用。

> **實作註記（2026-09-29，#482）**：上一段寫的「疊參數在 `TrialScorer.__call__`」已過時。#482 把疊法收成 `pipelines/training/steps/fit_params.py` 的 `fit_params(parameters, rules, chosen)`：`algorithm_params`（排序目標沒寫 metric 時補上 adapter 規則的預設 metric）→ `seed` → `chosen`；trial 與 refit 都用它，`fixed_params` 就是 `chosen`。另外兩層不在這個 dict 裡了：`feature_pre_filter` 由 LightGBM adapter 在訓練時最後蓋上，`num_iterations`、`early_stopping_rounds` 是 `ModelAdapter.train()` 的參數。效果跟上一段寫的順序相同，只差在 `feature_pre_filter` 由 adapter 最後蓋、不能被蓋掉（見決定 1 的實作註記）。

`fixed_params` 裡不得寫由別的設定鍵或框架決定的鍵（`objective`、`metric`、`seed`、`feature_pre_filter`、`num_iterations`、`early_stopping_rounds`），開跑前在 `core/consistency.py` 擋下。理由：寫了不是被後面的層靜默蓋掉，就是反過來靜默蓋掉框架的設定（`feature_pre_filter` 被改成 `true`，重訓會丟掉搜尋時可切的特徵）；`objective` 寫在這裡更糟，`.bin` 快取與丟組政策都照 `algorithm_params.objective` 決定，兩邊會對不上。

**其他設定在這個模式下**：
- 不用的：`search_space`、`n_trials`。印一行 log；它們照樣進 `model_version` 的雜湊。這是刻意的：多重訓一次是安全的，若要排除，版本號的規則就得加條件，容易少排或多排。也因為它們還進雜湊，`search_space` 的格式檢查（A8）照擋。
- 照用的：`hpo_objective`。它不再用來挑參數，但仍是選版指標的預設，也決定 test 要算哪些指標（ADR-0028 決定 3）。所以 A54 照擋；只為 HPO 存在的 A48（HPO 目標是二元預測類時，val 要保留無正例的組）在這個模式下不擋，因為 val 不讀。
- val 不讀、不複製。
- HPO 的搜尋診斷、續跑、`search_id` 都不用。

**`search_id` 拿掉 `fixed_params`**，跟今天拿掉 `n_trials` 同一個做法：HPO 模式下用不到它，改它不該讓一個跑到一半的搜尋接不回去。

**流程圖照設定長**：`create_pipeline` 依 `training.hpo_enabled` 建不同的 DAG。跳過 HPO 時，沒有 `cache_val_model_input`；`tune_hyperparameters` 換成一個「用固定參數訓練」的 node，它交出同樣三樣東西（`best_params`、`best_iteration`、`hpo_best_model`），下游不用改。這個 node 跟 `tune_hyperparameters` 一樣，開頭先釋放 SparkSession。CLI 從設定推出模式參數再呼叫 `create_pipeline`，跟 evaluation 依設定加減 node（`create_pipeline` 的 `baseline_rate`）是同一種做法，也符合 ADR-0013：模式決定這次走哪一條路，切片決定從哪裡接著跑。`tests/test_pipelines/test_resume_contracts.py` 的 `RESUME_CONTRACTS` 要為這個模式加一組。

**跟 node 規則 15 那一句不衝突**：規則 15 否決過「沒宣告某張選用的表，就不把讀它的 node 接進 DAG」，理由是 node 裡的分支照樣在，「不同部署跑 `--list-nodes` 會看到不一樣的 DAG，什麼也沒省到」。跳過 HPO 不是這種情況：它換掉的是整條路（HPO 與 val 的複製都不跑），就像 evaluation 的各種模式；而且 `--list-nodes` 列出的正是這個部署真的會跑的步驟。

**為什麼用明確開關，不用「有寫 `fixed_params` 就跳過」**：base 設定裡一定有 `search_space`，兩者同時存在時要靠「有沒有寫」猜使用者的意思；而且「跳過 HPO、不另外指定參數」寫不出來。

**為什麼流程圖要變，不是每個 node 自己看開關**：不會有一堆「跑了但什麼都沒做」的 node。兩階段建模要切「單一模型／分組模型」時，用的也是同一種做法。

**為什麼跟重構同一輪**：它改的正好是這一輪要重整的兩步（HPO、定模型）與決定 1 的「用給定參數訓練」。先把形狀做好，它只是多一條路；分開做，那兩步要改兩次。

## 決定 12　四件讀程式看得到的浪費；象限診斷只讀這次的 test 月份

驗收看「讀了幾次、拉了多少列進 driver、峰值多配了多少記憶體」，不看秒數（flow 規則 8）：本機合成資料的秒數外推不到生產。這四件都不改變輸出，但改變做法，在〈版本與順序的約束〉裡自成一類。

1. **HPO 確定用不到 val，才不讀。** 先開 study、讀 checkpoint：study 已經完成、而且 checkpoint 讀得回最佳模型，才跳過讀 val。其他路徑照舊讀，包括「study 有 trial、但 checkpoint 讀不回來，拿最佳參數重跑一次」的最後手段（`tune_hyperparameters` 裡的 last-resort）。binary objective 對 val 的前置檢查跟著讀取一起走。
2. **預測不再把全部 test 列拉進 driver。** 「有哪些（月份, item）要寫」從 parquet 的分區資訊取，不讀任何一列資料。每個分區只讀需要的欄：由輸出組裝（決定 5）與 adapter 的評分入口（決定 2）說出要哪些欄，不是在 node 裡寫一份固定清單。今天的組裝會讀零正例組權重欄，組合模型還要分組鍵；固定清單會漏。驗證要用宣告了 `dataset.test_zero_positive_group_ratio > 0` 的設定跑（base 設定是 0，驗不出來；`examples/ad` 的設定不是 0）。
3. **不再多配一份矩陣。** 建 `.bin`（排序目標的丟組與依 group 排序）與 `refit_on_full`，在串流時就依 group 順序寫入。常見的「就地」寫法（`X[:] = X[perm]`、`np.take(..., out=X)`）仍會另配一整份，所以驗收量實際的峰值配置，不是讀程式數複本。

   > **實作註記（2026-09-29，#483）**：做法是讀兩次。先只讀 label、group 欄與權重（或權重鍵）（`io/extract.py` 的 `extract_y`／`extract_y_with_groups`），node 在這些一維陣列上決定留哪些列、什麼順序；再讀特徵，每個 batch 直接寫進它最後的位置（`extract_X_rows`，refit 的兩個 split 用同一套列號疊起來）。**新增的假設只有一個**：兩次讀同一份 parquet，pyarrow 給的列順序相同。所以第二次會把 label 一起讀回來跟第一次比，對不上就停下；它看不到「只互換 label 相同的列」的重排。refit 的非排序分支也一樣處理：以前是兩份矩陣 `np.concatenate`，同樣多一整份。峰值量測見 PR 說明與 `docs/pipelines/training.md` §9.1。row-wise objective 建 `.bin` 沒有列要選，照舊一次讀完（`extract_Xy`），只有 refit 的非排序分支改成兩次讀，因為它要把兩份矩陣疊起來。
4. **`pdf_to_X` 與訓練共用同一條編碼路徑**（#418 第 2 項）。「一批列 → 矩陣列」只寫一份，挑欄改用 `_narrow_frame`。

另外兩件會改變診斷輸出，刻意列出來：
- **`select_shap_population` 只讀這次設定的 test 月份**（`dataset.test_snap_dates`），跟 `compute_shap_diagnostics` 一致（node 規則 14）。今天它讀預測表與 `test_model_input` 的全部月份。
- **它的排名改用 `utils/ranking.py` 的 `rank_by_score_then_item`。** 今天它自己寫排名，同分時只比 item、不比 `event`；宣告了 `event` 時，象限的第一名可能跟 evaluation 的排名不同。

**不收的效率項**（影響可能很小，或要先量）：診斷抽樣要整份從頭掃、`compute_test_metrics` 的中間表沒 persist、`extract_Xy` 開同一份 parquet 四五次、`persist_sample_weight_report` 每次整欄讀權重鍵。

> **實作註記（2026-09-29，#484，第 2 件與第 4 件）**：
> - 第 2 件，分區清單：`pipelines/training/steps/predict_partitions.py` 的 `partitions_from_directory_names` 走 `dataset.get_fragments()`，用 `pyarrow.dataset.get_partition_keys(fragment.partition_expression)` 取每個檔的 `(time, item)`，一列資料都不讀；`open_parquet_dataset` 回的多根 UnionDataset 也適用。測試把資料檔內容換成垃圾，清單照樣列得出來（`tests/test_pipelines/test_training/test_predict_partitions.py`；node 層另有一個：已寫完而跳過的月份有垃圾檔，run 照樣完成）。打開 dataset 本身仍會讀一個檔的 footer 來推 schema，這是 #484 以前就有的成本。它放在 `steps/` 的獨立模組，不放 `predict_months.py`：後者不 import 任何本專案或 pyarrow 的東西，有 AST 測試釘住。
> - 分區清單的兩個已知差異：只有空檔的分區目錄會被列出（讀列的寫法看不到它）——cache 由 Spark 的 `partitionBy` 寫，值沒有列就不會有目錄，所以這個 pipeline 寫出的 cache 上兩者一致；不是依 `(time, item)` 兩層分區的版面會丟 `ValueError`，而不是像以前那樣把它當資料欄照讀。
> - 每個分區只讀 `ScoredFrameLayout.source_columns()` 與 `ModelAdapter.scoring_columns()` 的聯集（node 裡沒有固定清單）。宣告了 `test_zero_positive_group_ratio > 0` 的路徑有兩層證據：單元測試（權重欄在讀取清單裡、值照寫），以及 `examples/ad` 的端到端（它的 ratio 是 0.5、宣告了 `occasion`）：`run_e2e.sh --compare` 與 `baseline_digest.json` 一致，而且 `training_eval_predictions` 跟 main 跑出來的逐欄相同（2,835 列、10 欄，分數逐位元相同，2026-09-29）。
> - 第 4 件：「一批列 → 矩陣列」收成 `io/extract.py` 的 `_write_batch_features`，`_stream_matrix` 逐批呼叫它，`pdf_to_X` 把整張表當一批（`_narrow_frame` 挑欄 → 一個 arrow `RecordBatch` → 同一個函式），延後編碼的類別欄集合同用 `_deferred_categoricals`。
> - `pdf_to_X` 的矩陣 dtype **刻意等於以前 `pdf[cols].copy().values` 的 dtype**：每欄編碼後的 dtype 交給 pandas 決定共同型別（`_flattened_dtype`）。不用宣告的 `numeric_feature_storage_type`，也不用 numpy 的 `np.result_type`——後者在「boolean 欄混數值欄」時答數值型別，pandas 答 `object`（`preprocessing.py` 的 cast 記過這個實測）。理由：dtype 不同就是矩陣不同、預測可能不同，而決定 9 的預測格式版本號還沒落地，已寫過的月份沒有辦法觸發重寫，所以這一輪不能改輸出。`tests/test_io/test_extract.py` 把舊實作當參考比對 float32、float64、混合型別、含 NaN、含未知類別、空表（dtype、shape、位元組全同）與 boolean（`object`，逐值與逐元素型別相同）。B6／B9 型別檢查照〈刻意不做〉沒有加。
> - `pdf_to_X` 的 log：`slice_features`、`to_numpy` 兩個子步驟名保留，`encode_categoricals` 子步驟沒了（編碼併進 `to_numpy`），資料量那一行從 `pdf_to_X.X_df` 改成 `pdf_to_X.X`（矩陣本身）。
> - 同一段程式順手收掉決定 16 的兩項重複：`cache_test_model_input` 的月份去重改用 `steps/predict_months.py` 的 `configured_months`；`--rebuild-dates` 的月份集合收成同檔的 `rebuild_month_keys`，快取 node 與預測 node 都呼叫它。

## 決定 13　training 根層一個開跑契約模組

**規則**：`__main__.py` 的 `training()` 裡只有 training 懂的邏輯（版本解析、manifest 附加欄位、只屬於 training 的設定檢查），搬進新的 `pipelines/training/run_contract.py`，跟 dataset 的 `run_contract.py` 同一個形狀（ADR-0029 決定 11）。`cache_sources.inject_cache_source_tables` 今天在通用的 `_execute_pipeline` 裡對每條 pipeline 都跑，改成只在 training 呼叫。

**搬的時候照 node 規則 15 重審每一項注入。** 例：`search_id` 今天由 CLI 注入，但 node 自己算得出來（`_resolve_search_id` 在沒有注入時就自己算），catalog 也用不到它，所以不再注入。

**為什麼**：training 開跑前要什麼、跑完寫什麼，可以不經 CLI 直接測。

## 決定 14　新增機械檢查：pipeline 之間不互相 import，函式庫不 import pipeline

**規則**：`tests/test_core/test_architecture_constraints.py` 加一條掃 import 的檢查：
- `pipelines/<a>/` 不得 import `pipelines/<b>/`。
- `pipelines/` 以外的 `src/` 模組不得 import `pipelines/`；唯一的例外是 CLI（`__main__.py`）。
- 測試不在掃描範圍內（同 S3 的理由）。

規則文字寫進 `docs/agents/architecture-constraints.md`，**要使用者簽核**。

**為什麼**：這兩條規矩今天只寫在三處：ADR-0008（函式庫不 import pipeline，所以 `feature_selection` 放 `models/`）、2026-07-06 的診斷整合設計（`docs/superpowers/specs/2026-07-06-diagnosis-pipeline-integration-design.md`：pipeline 之間不互相 import）、`diagnosis/__init__.py` 的 docstring（diagnosis 不得 import `pipelines/*`）。沒有任何測試在守。本份會新增不少跨目錄的呼叫（共用評分、adapter 吃整張表），正是最容易不小心打破的時候。flow 規則 6：檢查跟搬移放同一個 PR，而且要拿「新結構才有的 import 寫法」試過它會紅。

## 決定 15　拆掉 `io` 與 `models` 之間的 import 循環

**事實**：`io/__init__.py` 轉出口 `ModelAdapterDataset`（它 import `models.base`），`models/__init__.py` import `lightgbm_adapter`（它 import `io.handles`），兩個套件在載入時互相依賴。今天四個入口各自冷啟動都載得起來，但能不能載入取決於初始化順序；`lightgbm_adapter.py` 有 4 處為了避開它而寫在函式內的 `io.extract` import。

**規則**：拿掉 `io/__init__.py` 對 `ModelAdapterDataset` 的轉出口（它沒有消費者）。**不要**動 `models/__init__.py` 對 `lightgbm_adapter` 的 import：LightGBM 的註冊靠它觸發，拿掉之後 CLI 的登記表是空的；而有 10 個測試檔直接 import `lightgbm_adapter`，全量測試會照樣綠，看不出來。決定 2 的評分入口要用 `io/extract.py` 的編碼路徑，adapter 對 `io/` 的依賴會留著，所以循環要在同一輪處理。那 4 處函式內 import 拆完之後能不能搬到模組頂層，實作時實測再定。

> **實作註記（2026-09-29，#482）**：
> - 拿掉轉出口之後，`import recsys_tfb.io` 不再順帶載入 `models`（之前會）。`tests/test_models/test_cold_imports.py` 守住這件事，也守四個入口各自在新行程裡載得起來。
> - `models/lightgbm_adapter.py` 裡那幾處函式內 import **不能**搬到模組頂層，實測過：`io.extract`、`core.logging` 搬上去之後，單獨 `import recsys_tfb.io.model_adapter_dataset` 會失敗（ImportError），另外三個入口照樣載得起來。原因是 `core` 套件一載入就 import `core.catalog`，而它 import 的正是 `io.model_adapter_dataset`。理由寫在該檔開頭。

## 決定 16　收尾清理

- 盤點筆記第一節列的 10 處小重複收成一處：`feature_pre_filter: False` 字面值 5 處、metric 預設與訓練參數 dict（tune 與 finalize、trial 與 refit 各一份）、類別欄索引兩份、權重鍵與 decode map 兩份、月份去重與 rebuild 集合兩份、快取路徑兩份且預設值不同、`"_SUCCESS"` 3 處、`training.algorithm` 預設 4 處、`data/models/...` 路徑在 `steps/hpo_resume.py` 與 `diagnosis/model/paths.py` 寫死（跟 catalog 各一份）、collect-all 訊息兩套。
- 9 處跟程式對不上的 docstring 與註解（例如 `nodes.py` 開頭「21 個 node 中 14 個」、「五個 cache node」）。
- `persist_sample_weight_report` 改名：它只回傳報告，存檔是 catalog 的事（node 規則 6）；`persist_group_filter_report` 一併檢查。
- `pipeline-node-design.md`〈已登記的例外〉的第一筆（「`pipelines/training/` 的部分 node `def` 在 `recsys_tfb.diagnosis.model` 底下」）在決定 6 落地時刪除。使用者 2026-09-28 同意。
- `architecture-constraints.md` A1〈這個檢查看不到〉裡「training 的 7 個診斷 node」那一段，在決定 6、7 落地時改寫：`def` 回到 `nodes.py`、圖交給 catalog 之後，那個盲區不存在了。

> **實作註記（2026-09-29，#483）**：本票收掉第一項裡跟 `.bin` 同一段程式的四處重複，與兩處過期註解、兩個改名：
> - 類別欄索引兩份：#482 讓 refit 與 `.bin` 都走 `ModelAdapter.build_train_data` 之後已經只剩一份，本票沒有再動。
> - 權重鍵與 decode map：樣本權重報告 node 改用 `io/extract.py` 的 `weight_key_columns` 與新的 `weight_key_decode_map_from_config`，後者也是訓練加權（`_row_weights_from_pdf`）用的那一份。
> - 快取路徑兩份、`_SUCCESS` 3 處：見決定 10 的實作註記。
> - `nodes.py` 開頭的 node 數（20 個裡 13 個）、「五個 cache node」（四個，另有 `io/parquet_dataset.py` 一處）。
> - `persist_sample_weight_report` → `compute_sample_weight_report`；`persist_group_filter_report` 檢查過，同樣只回傳報告、存檔由 catalog 負責，一併改成 `compute_group_filter_report`。
> - `conf/` 有兩行註解跟著改，使用者同意：`catalog.yaml` 寫著舊 node 名的那一行、`parameters_training.yaml` 的「5 個 cache node」。所以本票 `conf/` 的 diff 只有這兩行註解；改動前後 `yaml.safe_load` 的結果相同。

---

## 版本與順序的約束

**版本號**：
- 決定 9 的模型格式版本號第一次加進雜湊時，每個部署的 `model_version` 變一次。
- 決定 11 在 base 設定加了 `training.hpo_enabled`、`training.fixed_params` 兩個鍵。`training:` 區塊整個進雜湊，新鍵預設也算，所以它也會讓 `model_version` 變一次。兩者若在同一批落地，只變一次。
- 決定 10 只改快取路徑，不動 `model_version`。
- 其他決定不改變模型，也不改變 test 預測，不動版本號。實作時若發現某一步其實會改變輸出（例如編碼路徑合併之後 dtype 不同），照決定 9 加 1，並回頭更正本份。
- `model_version` 變的那一次，`examples/ad/baseline_digest.json` 要重取（它釘住 `model_version`，`run_e2e.sh --compare` 會轉紅）。`docs/operations/user-guides/adding-an-eval-month.md` 的步驟 ③（框架升級之後、重訓之前，這套流程用不了）今天只提 dataset 產物格式版本，要把 training 模型格式版本也加進去。

**相依**：
- 決定 2、3、4、10、11 都用到決定 1 的介面（評分入口、登記表、「做不到」的例外、原生資料、用給定參數訓練），要排在它之後。
- 決定 4 要跟決定 7 同一個 PR，或排在它之後：畫圖的 best-effort 由決定 7 的圖存檔負責；決定 4 先落地的話，只能在 node 裡吞畫圖的例外，違反決定 4 自己。
- 決定 7、8 要排在決定 6 之前，不然搬好的 node 還要再改一次接法；決定 7 先落地，決定 6 才拿得到「`conf/` diff 為空」的證據（決定 7 會改 catalog）。
- 決定 12 第 2 件排在決定 2、5 之後：每個分區讀哪些欄，由它們說出。

**分類**（flow 規則 3：行為變更先做，結構重整放最後）：
- 不改輸出、改做法的：決定 1、2、3 的介面與搬移、7、8、12 的四件浪費、13、15。
- 會改變行為的：決定 3（檢查提早）、4、5（training 預測多了檢查、欄名照 `schema`）、9、10、11，以及決定 12 的象限診斷那兩件。
- 結構重整，放最後：決定 6（搬家加決策上浮，不是純搬移）與 16 的清理。

**行為不變的證據**（flow 規則 8、9）：
- 決定 6 **拿不到**「`pipeline.py` diff 為空」「AST 逐函式比對」這兩種便宜的證據：`pipeline.py` 的 import 一定要改，決策上浮也會改 node body。flow 規則 4 說過，搬移與決策上浮合在一起就只剩產物比對。所以它的證據是：`conf/` diff 為空（決定 7 已先落地）、產物跟 main 比對（扣掉 noise floor）；純搬過去、內容沒改的機制函式，另外用 AST 比對。
- noise floor：第一個宣稱行為不變的 PR 之前，先讓 main 自己跑兩次，記下「本來就會不同」的檔案（SHAP 圖、時間戳記之類）。`examples/ad` 的 e2e digest（`run_e2e.sh --compare`）兩次跑的預測是一樣的，是最便宜的產物比對，先跑它。
- 決定 14 跟第一個新增跨目錄 import 的 PR 一起進（flow 規則 6）。

---

## 考慮過、沒選的做法

| 做法 | 為什麼沒選 |
|---|---|
| 這一輪就把兩階段建模做進來 | 它是會改輸出的新功能；混進重構，就不能用「產物跟以前一模一樣」證明重構沒改壞東西。組合模型會是第二個真的 adapter，那時才驗得出接縫對不對 |
| 做一個 scikit-learn 的第二演算法 | 生產環境只裝了 LightGBM 與 scikit-learn（不能加套件），scikit-learn 沒有排序目標；沒人要用的演算法做了要一直維護。接縫改用測試裡的假 adapter 驗 |
| HPO 加速收進這一輪 | 會改變挑出哪個模型，而且要先在公司環境量；另寫一份 spec |
| 開頂層 `scoring/`，整段評分放進去，adapter 只收矩陣 | 組合模型的分組鍵可以不在特徵裡，第二階段的矩陣還有第一階段的分數；那樣組合模型要改兩處。翻盤條件見決定 2 |
| training 直接 import inference 的模組 | 會開出 repo 第一條 pipeline 之間的 import；inference 的重構會打壞 training |
| 7 個診斷照搬成轉手殼 | node 規則 3 的違例只是換位置；這塊之後會一直長 |
| 診斷機制留在頂層 `diagnosis/model/` | 只有一個消費者的函式庫；「搬去 evaluation」的方向已經關上 |
| 診斷全部吞錯、只警告 | 真的 bug 會被吞掉；停下的代價小（見決定 4）。畫圖是唯一保留只警告的，理由在決定 4 |
| 畫圖失敗也讓 training 停下 | 圖不影響任何數字，少一張圖看得出來；為它停下一個跑了好幾小時的 pipeline 不划算 |
| 兩個存圖的 node 登記進 R4、自己存檔 | 要簽核，而且擴大「自己寫檔」的例外；交給 catalog 更好測 |
| `Node` 維持只照位置傳參數 | 每加一個診斷，`log_experiment` 的參數就多一格；位置錯了會被 `=None` 吞掉 |
| 只有一個 training 格式版本號，同時管模型與預測 | 為了 inference 改評分程式，每個部署都要從頭重跑 HPO |
| `.bin` 的檢查整理成一份清單（#418 第 1 項原案） | 檢查還是會越疊越多；放進路徑之後格式再變只要加 1 |
| 快取格式版本號與模型格式版本號共用一個 | 快取格式改了不一定改變模型，共用會逼所有部署重訓 |
| 跳過 HPO 只靠「有沒有寫 `fixed_params`」 | 兩者並存時要猜意思；「不另外指定參數」寫不出來 |
| 跳過 HPO 時流程圖不變、node 自己什麼都不做 | 會有一堆跑了但什麼都沒做的 node；兩階段建模也要同一種做法 |
| 演算法專屬的設定規則留在 `core/`，等第二個演算法來再搬 | 搬的是已經存在的規則，碰到的地方本來就要改；留著會有兩種寫法並存 |
| `pdf_to_X` 也過訓練路徑的型別檢查（B6、B9） | 那兩個檢查讀的是 parquet 的 schema，`pdf_to_X` 拿到的是已經在記憶體裡的表，套不上去；而且會改變 inference 的行為，不在本份範圍 |

---

## 沒做的事，和什麼時候做

| 事 | 什麼時候 |
|---|---|
| 交接包（#382） | 本份落地之後。本份的新形狀不能擋住它 |
| 兩階段建模 | 自己一份 spec。開場先定名字：設計檔的「分群」跟 `CONTEXT.md` 的 **分群**（evaluation 依 entity 屬性切片算指標）撞名，本份暫稱「分組」 |
| HPO 加速（提早喊停、少試幾組、`max_bin`、同時試好幾組） | 自己一份 spec，先在公司環境量 |
| 同分規則落地成 `rank` 欄（ADR-0014〈刻意不做〉第 2 條、#402） | 不變 |
| HPO 同時試好幾組、checkpoint 兩個檔不是一次寫完（ADR-0014〈刻意不做〉第 3 條） | 不變 |
| `cache.root` 是相對路徑（ADR-0014〈刻意不做〉第 8 條） | ADR-0014 把它綁在「診斷獨立成 pipeline 之前」，那個前提已經不會發生；風險（跨執行靜默指到不同地方）仍在，沒有排。決定 16 只把兩處不同的預設值收成一處 |
| `predict_manifest.json` 沒記 run_id（`docs/pipelines/training.md` 說要另開票） | 不在本份；有沒有票查 `gh` |
| lambdarank 下權重整體乘常數讓指標跳動（`docs/notes/2026-09-08-lambdarank-weight-scale-anomaly.md`）、排序目標下的權重公式 | 沒有排 |
| persist 峰值實測、拿掉 `score_uncalibrated`、逐列權重 | 各自的票：#238、#412、#425 |
| A1 稽核放寬到 `steps/`（#163） | **重開條件已經達成**：2026-09-19 的裁決說「診斷簡化有了定案（拿掉，或決定保留）」就重開，而 2026-09-27 決定全部保留。本份不裁，放不放寬由使用者決定 |

---

## 對既有 ADR 的修訂

- **ADR-0014 決定 6**（7 個診斷 node 留在原地）：被本份決定 6 取代。它的三個理由裡，「未來要搬就白做」因診斷不獨立、不搬去 evaluation 而失效；「會生出轉手殼」由決策上浮解決；「多跳一次檔」在 `def` 回到 `nodes.py` 之後不存在。
- **ADR-0014〈刻意不做〉第 1 條**（診斷獨立成一條 pipeline）：關閉。診斷留在 training。
- **ADR-0014〈刻意不做〉第 7 條**（診斷失敗該不該停 pipeline）：由本份決定 4 裁決。
- **ADR-0014〈刻意不做〉第 8 條**（`cache.root` 相對路徑）：它的觸發條件綁在第 1 條，第 1 條關閉後失效；事情本身仍在，見上表。
- **ADR-0013**（模式與切片分開）：本份決定 11 讓 training 也有模式，照它的定義。
