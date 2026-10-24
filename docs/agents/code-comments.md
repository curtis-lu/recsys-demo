# 程式碼註解與 docstring

寫或改 `src/`、`tests/`、`scripts/` 裡的 `#` 註解或 docstring 時照這份做。log 訊息、報表文字這類會給使用者看的字串不在範圍。

- 註解一律英文（`pipeline-node-design.md` 總表下方的體例）。
- node 的 docstring 另外要寫的三件事，見 [`pipeline-node-design.md`](pipeline-node-design.md) 規則 13。
- 不變量代號（`A24`、`B6`）直接寫；定義在 `core/consistency.py` 的模組 docstring。

## 1. 寫程式碼看不出來的事

值得寫的有三種：

- **不明顯的原因**：換成看起來也對的做法，會怎樣。
- **前提**：程式碼依賴、但沒寫出來的條件。
- **刻意不做的事**：讀者會以為漏了，其實是故意的。

做了什麼，交給函式名、變數名和簽章去說。

例（`src/recsys_tfb/pipelines/dataset/nodes.py:401`），講的是這行 `filter` 順便保證的事：

```python
# Decision — eligibility: only rows in the configured train months can be
# drawn. A month belongs to exactly one split (A24), so this is also what
# keeps train disjoint from val / test.
```

## 2. 每一句都要 durable：這段程式碼不改，這句就一直為真

- **寫現在要守的限制，用現在式。** 改動的來龍去脈寫進 commit 訊息和 PR 說明：票號、舊名字、第一版怎麼壞的、誰確認過。
- **寫這段程式碼自己保證得了的事。** 數量、呼叫者、別的檔案現在的樣子，都由別處決定，別處一改就變假。如果要靠別處的行為才講得清後果，就講後果，位置放括號當附註（`src/recsys_tfb/pipelines/dataset/nodes.py:497` 的寫法）。

改寫例：

```
src/recsys_tfb/pipelines/inference/steps/identity.py:3
  原句  One function, because three nodes need the same answer ...
  改寫  One function, because every node that names an output row must give the same answer ...

src/recsys_tfb/preprocessing.py:275
  原句  extract_Xy peak memory explodes (originally OOM-killed the val read).
  改寫  extract_Xy peak memory explodes.
        （前面的 "~70 B/value vs 4 B/float32" 已經把限制講完了）
```

**決定的出處可以寫票號。** 有些決定只記在 spec 票上、沒進 ADR，就寫成 `#NNN decision X`（例：`spec #426 decision D`）。這跟 `ADR-0025 decision 1` 一樣，是出處。

改到形狀不同的既有出處時，改寫成 `#NNN decision X`。例如：
- `implementation note of #488`（`src/recsys_tfb/pipelines/training/steps/train_data_cache.py:96`）
- `ADR-0006's #521 amendment`（`src/recsys_tfb/pipelines/dataset/pipeline.py:27`）

判準：拿掉這個票號，讀者還找得到那條規則嗎？找不到，它就是出處。

**測試**：`tests/test_core/test_code_comments.py` 把 `src/recsys_tfb` 註解裡的 `#NNN`，逐檔對照 `tests/test_core/issue_refs_in_src.txt`。紅的時候照下面處理：

- **多了一個票號**：移進 commit／PR 說明，或改寫成 `#NNN decision X`。
- **清單上有、程式裡沒有了**：刪掉清單上那一行。
- **搬了檔**：清單上那一行跟著搬。

其他的歷史寫法（舊名字、「originally」、「confirmed」）測試看不到，自己逐句檢查。

## 3. 一件事只講一次

ADR 或 `docs/` 已經論證過的事，註解只寫一句「這段程式碼不能怎麼改」，再附出處。判準：拿掉出處，改這段的人還知不知道哪裡不能動。

例：`scored_row_columns`（`src/recsys_tfb/pipelines/inference/steps/identity.py:15`）的 docstring 有 32 行，大半在重講 ADR-0025。改成這樣就夠：

```python
"""The columns that identify one row of this pipeline's output.

``identity_columns`` minus every optional role: offline inference ignores
them (ADR-0025 decision 1), so its output has no ``event`` / ``occasion``
column even when the deployment declares one. Subtract, don't rebuild from
``base_key_columns + [item]``: a new optional role must drop out here with
no edit.
"""
```

## 4. 函式 docstring 超過 10 行，就逐句過一遍規則 1–3

10 行是檢查點，不是上限。

- **拿掉**：不 durable 的句子、ADR 的副本。
- **留下**：其他句子都留，再長也留。規則 13 要的三件事都算要留的。
- **不管行數**：模組 docstring。

例：`split_train_keys`（`src/recsys_tfb/pipelines/dataset/nodes.py:432`）
- **拿掉**：「which records the distinct-and-join shape it replaced … (6.7s → 3.6s on 16M keys …)」。這是舊寫法加上一次量測。
- **留下**：entity 不跨 split 的保證、誰決定 entity 欄、兩個檢查各是哪一種、各自的成本。

## 5. 只整理這次 touched 的註解

- **你改到的函式**：照規則 1–4 整理它的註解。舊註解只靠這個方式清，不另外全面清。
- **被你的改動弄假的別處註解**：同一個 PR 改掉。它也算 touched。
- **其他函式**：註解維持原樣。
- **審查意見**：指出註解跟程式碼不符，就改正或刪掉；只是建議換個說法，原句不動。

## 6. 檢查得了的條件，寫成 assert

「這裡一定已經排序」「這個 list 不會是空的」這類條件，寫成 `assert`、`require_*` 守衛（命名見 `pipeline-node-design.md` 規則 12）或測試。註解最多留一句，說為什麼需要這個條件。
