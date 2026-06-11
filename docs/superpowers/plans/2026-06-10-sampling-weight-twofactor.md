# 雙因子權重 + 下採樣降成本 — 實作計畫

> 公式/決策見 spec `docs/superpowers/specs/2026-06-10-sampling-weight-twofactor-design.md`。
> 此計畫聚焦任務拆解、檔案、測試矩陣;TDD、逐任務 commit。inline 自行執行。

**測試指令**(worktree root):
```
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/sampling-weight-twofactor/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
tests/scripts/test_sampling_overrides_editor.py -q -m "not spark" -k <filter>
```
檔案:`scripts/sampling_overrides_editor.py`、`tests/scripts/test_sampling_overrides_editor.py`。

---

## Task 1 — 純函式 `floor_weight` / `two_factor_weights`(已實作,補測試)
函式已於預覽階段寫入模組。補單元測試:
- `floor_weight`:一般式 `n_pos·(1−t)/(t·n_neg_post)`;n_pos=0→1.0;n_neg_post=0→1.0。
- `two_factor_weights`:
  - eff_pos_rate == t(浮點近似);floor 等化(w_pos/w_neg 比 = t/(1−t)/... 即 n_pos·w_pos/(n_neg·w_neg)=t/(1−t))。
  - A:m==m_min 時 A==1;m>m_min 時 A<1;α=0 → A==1。
  - n_pos==0 → w_pos=w_neg=v=A=1.0、eff=0。
  - 回傳 keys = {w_pos,w_neg,v,A,m,eff_pos_rate}。
- 移除 legacy `suggest_weight` + `TestSuggestWeight`(現已無人用;aggregate 改雙因子)。

## Task 2 — `resolve_keys` weight 側:require label、weight_dims
- weight_keys 非空時 **require label_col ∈ weight_keys**(對稱 ratio;翻轉舊「禁止」guard)。
- 回傳新增 `weight_dims = [k for k in weight_keys if k != label_col]`。
- `union_dims = ratio_dims ∪ weight_dims`(label 已排除)。
- 測試:`[prod_name,label]`→weight_dims=[prod_name];`[seg,prod_name,label]`→[seg,prod_name];
  weight_keys 非空但無 label → raise;weight_keys 空 → weight_dims=[]、不 raise。
  更新既有 `test_rejects_label_in_weight_keys`(語意翻轉:label 現在**必須**在)。

## Task 3 — `aggregate_surfaces` weight 段改雙因子 + ratio keep-all 預設
- 簽名:weight 段參數 `alpha`、新增 `t`;移除 `w_max`。改用 `weight_dims`(取代以 weight_keys 聚合,
  因 weight_keys 現含 label;聚合維度 = weight_dims)。
- 流程:聚合到 weight_dims tuple 得 n_pos / n_neg_post → 先算各 cell `v`、`m=n_pos+n_neg_post·v`;
  `m_min = min(m for n_pos>0)`;再 `two_factor_weights(...)` 得 w_pos/w_neg/A/eff;
  每列加 `nat_logit=ln(n_pos/n_neg_post)`、`floor_logit`、`floored_neg_mass=n_neg_post·v`、`attn_mass=A·m`。
- ratio 段:`default_neg_mult` 預設語意改 keep-all(預設不下採;cost opt-in)。pure 函式層級
  保留參數,預設值由呼叫端(render_html/profile)給 keep-all。
- 測試:product-grain 對照預覽數字(insur floored_neg_mass≈305、eff≈t);多 weight_dims;
  零正樣本 cell;m_min 參照最少正樣本 cell。

## Task 4 — `grid_to_yaml` weight 段:每 cell 兩列(label 位置)
- weight_rows 改帶 `{keys:[weight_dims值], w_pos, w_neg}`。
- 重建:走訪完整 `cfg_weight`(含 label),label 位置填 `"1"`(→w_pos)/`"0"`(→w_neg),其餘填 dims 值;
  sparse:≠default_weight(1.0) 才出。
- A9b/A9c 經 probe 照常(label 分量非 item)。測試:`[prod_name,label]`→`prod|1`/`prod|0`;多維;
  label 非末位;w_pos=1.0(最冷)不出、w_neg 出。

## Task 5 — HTML/JS weight 面雙因子 + 全域 t/α + summary + render_html 簽名
- `render_html` 簽名:加 `weight_dims, t, alpha`;ratio cost R 改可選(預設 keep-all);移除 w_max。
- 模板常數:加 `WDIMS, T, ALPHA(注意力)`;移除 WMAX。
- JS:
  - `rebuildWeight`:聚合 STATS 到 WDIMS,鏡像 `two_factor_weights`(v/A/w_pos/w_neg/eff/nat_logit/
    floor_logit/floored_neg_mass/attn_mass),m_min over n_pos>0。
  - `renderWeight`:欄序比照預覽(① n_pos·n_neg原始·n_neg後·nat_logit;② v·A·w_pos·w_neg;
    ③ floored_neg_mass·eff·floor_logit·A·m)。weight 不再逐格可編輯(由 t/α 全域驅動)。
  - 頂部加 **t、α** 兩個 number input,oninput 重算 weight 面 + summary。
  - weight summary:floor 等化檢查(全 == t)、nat 差距/odds、注意力佔比比值。
  - ratio 面:預設 neg_mult 空 = keep-all(ratio 1.0);cost opt-in。
  - grain=segment-only(ratio_dims 或 weight_dims 只含 segment、無 item)→ note 警告「對組內排序無效」。
- 測試(render_html 字串斷言):`const T=`、`const WDIMS=`、雙因子函式名、t/α input、summary 函式、
  weight 匯出 `keys`+label;移除舊 cold-boost 斷言(median/n_pos、w_max)。

## Task 6 — `profile` CLI 重接 + 全套件 + 真渲染預覽
- `profile`:`--t`(預設 1/6)、`--alpha`(預設 0.5)options;移除 `--w-max`;ratio cost `--cost-neg-pos`
  可選(預設 keep-all)。傳 weight_dims/t/alpha 給 render_html;resolve_keys 取 weight_dims。
- 全 `-m "not spark"` 綠;spark TestProfileStats 綠(profile_stats 未動)。
- 用 `render_html` + 合成 stats 產生 `data/profiling/sampling_overrides_editor.html`(真互動版),
  人工開瀏覽器驗收(對照本輪預覽頁)。

## 完成
graphify rebuild;PR(base = `feat/sampling-editor-general-groupkeys` 或待 #70 併入後對 main)。
