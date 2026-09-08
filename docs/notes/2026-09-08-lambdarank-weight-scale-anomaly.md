# 未解：lambdarank 對 sample weight 的**整體縮放**不免疫（2026-09-08）

> **這是一份未結案的調查紀錄，寫給下一個接手的人（含 AI）。**
> 現象是穩定可重現的，代數說它不該發生，我把能想到的解釋都排除了，仍然沒找到原因。
> §3 是重現腳本（自足、秒級），§5 是我試過而死掉的路，§6 是還沒試的路。
> **不要重走 §5。**

環境：Python 3.10.9、LightGBM **4.6.0**（wheel 版，`score_t` 為 float32）、numpy 1.25.0、macOS。
所有數字來自 in-memory 合成 fixture，**不是本專案的資料**。

---

## 1. 現象

把一整個 `sample_weight` 向量乘上一個常數——**只改尺度，不改任何相對關係**——
lambdarank 訓練出來的模型會實質不同，而且差很多。

```
two-factor weights: mean=0.0461 min=0.00902 max=1.0000
  w and w/c differ by EXACTLY the constant 21.693

   w      (mean 0.0461)  rounds=  60  macro AP = 0.2723
   w      (mean 0.0461)  rounds=1000  macro AP = 0.2723
   w / c  (mean 1.0000)  rounds=  60  macro AP = 0.3311
   w / c  (mean 1.0000)  rounds=1000  macro AP = 0.3312
```

- 差距 **+0.059 macro per-item AP**。
- **不是收斂速度**：輪數多 16 倍，差距一動也不動。
- **不是單一 seed 的巧合**：10 個配對 seed，`w` 相對不加權是 +0.0023（95% CI **含 0**），
  `w/c` 是 +0.0514（CI `[+0.0472, +0.0557]`）。兩個 CI 不相交。

`w` 是本 repo `docs/operations/user-guides/sampling-overrides-editor.md` §3 的雙因子公式
（`w_pos = A`、`w_neg = A·v`）算出來的。**該公式的輸出天生平均遠小於 1**——`A ≤ 1` 是
定義（`m_min/m ≤ 1`），冷門 item 的 `v ≪ 1`，而負例佔絕大多數的列。所以這不是誰填錯
數字，是公式的結構。

---

## 2. 為什麼這件事重要

1. **`scripts/sampling_overrides_editor.py` 匯出的權重尺度是公式的副產品，沒有人在管它。**
   兩個人拿「同一張權重表」但尺度不同，會得到實質不同的模型。
2. 效果的量級（0.059 macro AP）**大於本輪測過的任何一個建模選擇**。
3. **推導不出正確的尺度**——見 §4，代數說尺度根本不該有影響。

---

## 3. 怎麼重現

```bash
/Users/curtislu/projects/recsys_tfb/.venv/bin/python repro.py
```

不需要 Spark、不碰 repo 任何資料、不需要 repo 的 import。全文：

```python
"""Minimal repro: LightGBM lambdarank is not invariant to a global rescale of
the sample weights, although the algebra says it must be.

Self-contained: no repo imports, no Spark, no disk. Seconds to run.
    python repro.py
"""
import numpy as np
import lightgbm as lgb

M, D = 22, 4                      # items per query, latent dims
N_TRAIN, N_EVAL = 20000, 40000
OFFSET = np.linspace(0.0, -4.4, M)     # popularity: ~16% .. ~0.15%
PARAMS = {"objective": "lambdarank", "verbosity": -1, "seed": 42,
          "num_threads": 4, "deterministic": True, "force_row_wise": True,
          "learning_rate": 0.1, "num_leaves": 31}


def make(rng, n, V):
    """One positive per query = argmax utility; cold items are learnable."""
    u = rng.normal(size=(n, D))
    util = u @ V.T + OFFSET + rng.gumbel(size=(n, M)) * 0.9
    pos = util.argmax(axis=1)
    item = np.tile(np.arange(M), n)
    X = np.column_stack([np.repeat(u, M, axis=0), item])
    y = (item == np.repeat(pos, M)).astype(int)
    return X, y, item, np.full(n, M), pos


def macro_ap(scores, pos, n):
    rank = (-scores.reshape(n, M)).argsort(axis=1).argsort(axis=1) + 1
    r = rank[np.arange(n), pos]
    per_item = [np.mean(1.0 / r[pos == j]) for j in range(M)
                if (pos == j).sum() >= 10]
    return float(np.mean(per_item))


def tree_of(Xtr, ytr, gtr, itr, w, rounds=1, **kw):
    p = dict(PARAMS); p.update(kw)
    b = lgb.train(p, lgb.Dataset(Xtr, label=ytr, group=gtr, weight=w,
                                 categorical_feature=[D], free_raw_data=False),
                  num_boost_round=rounds)
    sp, lv = [], []

    def walk(nd):
        if "leaf_value" in nd:
            lv.append(nd["leaf_value"]); return
        sp.append((nd["split_feature"], str(nd["threshold"])))
        walk(nd["left_child"]); walk(nd["right_child"])
    for tr in b.dump_model()["tree_info"]:
        walk(tr["tree_structure"])
    return b, sp, np.array(lv)


rng = np.random.default_rng(0)
V = rng.normal(size=(M, D)) * 0.6
Xtr, ytr, itr, gtr, ptr = make(rng, N_TRAIN, V)
Xev, yev, iev, gev, pev = make(rng, N_EVAL, V)

# the repo's two-factor weight table (sampling-overrides-editor.md §3)
n_j = np.maximum(np.bincount(ptr, minlength=M).astype(float), 1.0)
t, alpha = 1.0 / 6.0, 0.5
n_pos, n_neg = n_j, N_TRAIN - n_j
v = n_pos * (1 - t) / (t * n_neg)
m = n_pos + n_neg * v
A = (m.min() / m) ** alpha
w = np.where(ytr == 1, A[itr], A[itr] * v[itr])
c = w.mean()
print(f"two-factor weights: mean={c:.4f} min={w.min():.5f} max={w.max():.4f}")
print(f"  w and w/c differ by EXACTLY the constant {1/c:.3f}\n")

print("== 1. the phenomenon: a pure rescale changes the model ==")
for tag, ww in (("w      (mean %.4f)" % c, w), ("w / c  (mean 1.0000)", w / c)):
    for rounds in (60, 1000):
        b, _, _ = tree_of(Xtr, ytr, gtr, itr, ww, rounds=rounds)
        print(f"   {tag}  rounds={rounds:4d}  "
              f"macro AP = {macro_ap(b.predict(Xev), pev, N_EVAL):.4f}")

print("\n== 2. the tree learner alone IS invariant (fixed gradients, l2=0) ==")
g0 = np.random.default_rng(1).normal(size=N_TRAIN * M) * 0.01
h0 = np.random.default_rng(2).random(N_TRAIN * M) * 0.01 + 1e-4


def fixed(scale):
    return lambda preds, ds: (g0 * scale, h0 * scale)


def fixed_tree(scale, **kw):
    p = dict(PARAMS); p.pop("objective"); p.update(kw)
    b = lgb.train({**p, "objective": fixed(scale)},
                  lgb.Dataset(Xtr, label=np.zeros(len(ytr)),
                              free_raw_data=False), num_boost_round=3)
    sp, lv = [], []

    def walk(nd):
        if "leaf_value" in nd:
            lv.append(nd["leaf_value"]); return
        sp.append((nd["split_feature"], str(nd["threshold"])))
        walk(nd["left_child"]); walk(nd["right_child"])
    for tr in b.dump_model()["tree_info"]:
        walk(tr["tree_structure"])
    return sp, np.array(lv)


for l2 in (0.0, 1.0):
    base = fixed_tree(1.0, lambda_l2=l2)
    t2 = fixed_tree(1 / c, lambda_l2=l2)
    r = np.median(t2[1] / np.where(base[1] == 0, np.nan, base[1]))
    print(f"   lambda_l2={l2}:  splits identical={str(t2[0]==base[0]):5s}  "
          f"leaf ratio={r:.5f}   (invariant => True / 1.00000)")

print("\n== 3. lambdarank diverges at TREE 1, with every absolute knob off ==")
OFF = dict(min_sum_hessian_in_leaf=0.0, min_data_in_leaf=1,
           min_gain_to_split=0.0, lambda_l2=0.0, lambda_l1=0.0)
u = np.ones(len(ytr))
for tag, w1, w2, kw in (
        ("two-factor, defaults",             w, w / c, {}),
        ("two-factor, all thresholds off",   w, w / c, OFF),
        ("two-factor, thresholds off + lambdarank_norm=False",
         w, w / c, {**OFF, "lambdarank_norm": False}),
        ("UNIFORM 1.0 vs 0.0461, thresholds off", u, u * c, OFF)):
    _, s1, l1 = tree_of(Xtr, ytr, gtr, itr, w1, **kw)
    _, s2, l2 = tree_of(Xtr, ytr, gtr, itr, w2, **kw)
    r = np.median(l2 / np.where(l1 == 0, np.nan, l1))
    print(f"   {tag:52s} splits identical={str(s1==s2):5s} leaf ratio={r:.5f}")
```

---

## 4. 代數說它不該發生

GBDT 的葉值與分裂增益：

```
    葉值   = − Σg / (Σh + λ₂)
    增益   = Σg²/(Σh + λ₂) 的組合
```

權重全部乘 `k` ⇒ `g → kg`、`h → kh`。當 `λ₂ = 0`：

```
    − kΣg / kΣh  =  − Σg / Σh          葉值不變
    (kΣg)²/(kΣh) =  k · Σg²/Σh         增益全體同乘 k → argmax 不變
```

**兩者都免疫。** 而本 repo 用的是 LightGBM 預設 `lambda_l2 = 0.0`
（`include/LightGBM/config.h`；`LGBMRanker().get_params()` 印出 `reg_lambda=0.0`）。

而 LightGBM 的 lambdarank 目標函式對權重也是**嚴格線性**的
（`src/objective/rank_objective.hpp`，4.6.0 sdist）：

```cpp
// line 73：先算出這個 query 的（未加權）lambda 與 hessian
GetGradientsForOneQuery(query_index, cnt, label_ + start, ..., gradients + start, hessians + start);
// line 75-82：之後才逐列乘上權重
if (weights_ != nullptr) {
  for (data_size_t j = 0; j < cnt; ++j) {
    gradients[start + j] = static_cast<score_t>(gradients[start + j] * weights_[start + j]);
    hessians [start + j] = static_cast<score_t>(hessians [start + j] * weights_[start + j]);
  }
}
```

**所以：線性的目標函式 ＋ 免疫的樹學習器 ⇒ 整條管線應該對縮放完全免疫。實際上不是。**

---

## 5. 已經排除的假設（**不要重走**）

| # | 假設 | 怎麼測 | 結果 | 結論 |
|---|---|---|---|---|
| 1 | `lambda_l2 > 0` 破壞免疫性 | 讀 `config.h` ＋ `get_params()` | 預設 **0.0** | 排除。（用 `lambda_l2=1.0` 做正對照，確實會破壞 → 測試有鑑別力）|
| 2 | 樹學習器本身不免疫 | 用自訂 objective 回傳**與 preds 無關的固定** `g`、`h`，再整體乘常數（§3 腳本第 2 段）| `λ₂=0` 時 split 完全相同、葉值比 **1.00000** | **排除**，樹學習器無罪 |
| 3 | `min_sum_hessian_in_leaf`（預設 1e-3，**絕對**門檻，見 `feature_histogram.hpp:879/890/977/988/…`）| 設成 `0.0`；另外設成 1.0、100 確認旋鈕是活的 | 設 0 與設 1e-3 結果**逐位相同** | 排除，這個門檻沒有生效 |
| 4 | `min_data_in_leaf` / `min_gain_to_split` | 一起關掉（`min_data_in_leaf=1`、`min_gain_to_split=0`）| 第 1 棵樹仍然分岔 | 排除 |
| 5 | `lambdarank_norm` 的 `log2(1+Σλ)/Σλ` 是非線性 | ① 讀原始碼：`sum_lambdas` 累加在 line 206-263，**在 line 75 加權之前**，所以與權重無關 ② 實測 `lambdarank_norm=False` | 落差仍在（0.2482 vs 0.3294）| **排除** |
| 6 | 只是收斂比較慢，多訓幾輪會追上 | 60 / 150 / 400 / 1000 輪 | 差距 +0.0588 → +0.0590，**完全不動** | 排除 |
| 7 | 是「絕對尺度」本身的問題（與權重形狀無關）| 用**均勻**權重掃 1e-4 ~ 1e4 | macro AP 只在 0.2375~0.2657 之間動；0.0461→1.0 只差 **+0.004**（雙因子是 +0.059，**15 倍**）| **部分排除**：純尺度效應存在但很小，解釋不了雙因子的落差 |
| 8 | 小權重的 hessian 撞到數值下限被夾住 | 只把小權重往上夾（`np.maximum(w, lo)`），不動整體尺度 | 沒有恢復（0.2723 → 0.3013 → 0.2783 → 0.2888，無趨勢）| 排除 |

### 定位到哪一步

第 1 棵樹就分岔了。**那時所有分數都還是 0**，所以 `|ΔNDCG|` 與 sigmoid 項在兩組之間完全
相同，梯度只差一個常數 `c`：

```
   two-factor, defaults                                 splits identical=False leaf ratio=0.46242
   two-factor, all thresholds off                       splits identical=False leaf ratio=-0.09405
   two-factor, thresholds off + lambdarank_norm=False   splits identical=False leaf ratio=0.20451
   UNIFORM 1.0 vs 0.0461, thresholds off                splits identical=False leaf ratio=0.66620
```

**連均勻權重、所有絕對門檻關掉，第 1 棵樹都不一樣。** 而葉值比值應該是 1.00000。

---

## 6. 還沒試的路（給下一個人）

我卡住的地方：唯一剩下的候選是 **float32 捨入改變了接近平手的分裂選擇**
（`include/LightGBM/meta.h`：`SCORE_T_USE_DOUBLE` 是註解掉的，`score_t` 是 `float`）。
但 float32 的**相對**精度跟尺度無關，所以這個解釋補不上
「為什麼是系統性的、10/10 seed 同號」。

按我認為的價值排序：

1. **自己編一份 `SCORE_T_USE_DOUBLE` 的 LightGBM 跑同一個 repro。**
   最直接。如果 double 版免疫了 → 就是 float32 捨入，結案。
   （`meta.h` 把那行 `#define` 取消註解再 build。）
2. **把梯度直接印出來比對。** 用自訂 objective 包一層 lambdarank
   （或用 `init_score` ＋ 單輪），確認 `g₂/g₁` 是否逐列精確等於 `1/c`。
   若不是，問題就在目標函式；若是，就在直方圖累加。
3. **看直方圖的累加型別與 subtraction trick**（`src/treelearner/feature_histogram.hpp`、
   `histogram_16_32_64.hpp`）。LightGBM 有 16/32/64-bit 直方圖的自動選擇，
   而**選哪一種可能依賴梯度的絕對量級**——這是我最沒查的一塊，也可能是真正的答案。
4. 換 `force_col_wise=True`、關掉 `deterministic`、單執行緒，看現象是否改變。
5. 用 XGBoost 的 `rank:pairwise` 跑同一個 fixture。若它免疫，就更確定是 LightGBM 的實作。

---

## 7. 在查明之前，怎麼做決策

**權重向量的整體尺度是一個「效果真實、但推導不出正確值」的超參數。**

不是 bug、不是可以算出來的常數。所以：

| 做法 | 說明 |
|---|---|
| **把尺度放進 HPO 的 `search_space`**（建議） | 它就是一個純量。推不出來就搜出來——這是對這類旋鈕唯一誠實的處理 |
| 釘死一個值 | 可以，但它是 `model_version` 的一部分，換值要重驗 |
| ~~宣稱「正規化到平均 1 才對」~~ | ❌ **證不出來**。實測 mean ≳ 1 之後大致平坦，但那是經驗觀察，不是推導 |

**要寫進使用者文件的一句話**：`sample_weights` 的整體大小會影響模型，不只相對比例。
兩張成比例的表不是同一張表。

---

## 8. 本輪的限制

- 全部在合成 fixture 上。**沒有**在本專案的資料或 pipeline 上重現過。
- 只測了 `lambdarank`。`rank_xendcg` 與 `binary` 沒測——**`binary` 很可能免疫**
  （pointwise 的 g、h 對權重也是線性，且沒有 per-query 步驟），但我沒驗。
- 沒有讀直方圖那一段的原始碼（見 §6 第 3 點）。
- LightGBM 只測了 4.6.0 這一版。

---

## 相關文件

- [`2026-09-07-ltr-objective-support.md`](2026-09-07-ltr-objective-support.md) — 本輪 LTR 調查的主檔
- [`../operations/user-guides/sampling-overrides-editor.md`](../operations/user-guides/sampling-overrides-editor.md) §3 — 產生 `w` 的雙因子公式
- [`../agents/pipeline-performance-work.md`](../agents/pipeline-performance-work.md) 規則 9 — 負面結果要跟正面結果一起留下
