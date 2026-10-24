# 程式碼註解與 docstring 的寫法

寫或改任何 Python 的註解、docstring 之前先讀這份。範圍是 `src/`、`tests/`、`scripts/` 裡的 `#` 註解與 docstring，不管 log 訊息、報表文字這類**會被使用者看到的字串**。

它回答三個問題：**註解寫什麼**、**什麼不寫在註解裡、該放哪**、**寫多長**。

**跟其他文件的分工**：

- node 的 docstring 還要多講三件事（為什麼不是另一種做法、選錯會不會報錯、成本量級），那是 [`pipeline-node-design.md`](pipeline-node-design.md) 規則 13；node body 裡每個步驟上面的 `# Decision —` 是同一份的規則 9。那兩條講「node 要多寫什麼」，這份講「所有註解都不寫什麼、寫多長」，兩份都要守。
- 註解與 docstring 一律英文，`docs/` 一律繁體中文——這條體例寫在 `pipeline-node-design.md` 的總表下方，這裡不重複。
- 不變量代號（A 系列、B 系列）在註解裡可以直接用。這是本檔的判斷：它們跟函式名一樣 grep 得到，定義只有一處（`core/consistency.py` 的模組 docstring，見 [`domain.md`](domain.md)），不會漂。

---

## 全部規則一覽

動手前對著這張表看一遍。編號只是指路用，規則本身寫在標題裡。

| # | 規則 | 怎麼確認 | 誰擋得住 |
|---|---|---|---|
| [1](#1-只寫看程式碼推不出來的事) | 只寫看程式碼推不出來的事 | 只看程式碼的人，寫得出這句註解嗎？寫得出就刪 | 沒人，只能人看 |
| [2](#2-不寫歷史與過程那是-commit-與-pr-的事) | 不寫歷史與過程，那是 commit 與 PR 的事 | 有沒有票號、「原本／改名前／第一版」、「誰確認過」 | **部分**：`src/` 裡的 `#NNN` 有測試，其他只能人看 |
| [3](#3-只寫這段程式碼自己保證得了的事) | 只寫這段程式碼自己保證得了的事 | 別的檔案改了，這句會不會變假 | 沒人，只能人看 |
| [4](#4-一件事只在一個地方講完整) | 一件事只在一個地方講完整 | ADR 或 `docs/` 已經講過的論證，註解是不是又講一遍 | 沒人，只能人看 |
| [5](#5-函式-docstring-超過-10-行要說得出為什麼不進-adr) | 函式 docstring 超過 10 行，要說得出為什麼不進 ADR | 數行數；超過就問多出來的是不是決策紀錄 | 沒人，只能人看 |
| [6](#6-只整理你改到的函式說錯才改說法不改) | 只整理你改到的函式；說錯才改，說法不改 | diff 裡「只改註解」的函式，是這次改到的、或被這次改動弄假的嗎 | 沒人，只能人看 |
| [7](#7-能用測試或斷言釘住的不寫成註解) | 能用測試或斷言釘住的，不寫成註解 | 這句註解描述的是不是一個可以 `assert` 的條件 | 沒人，只能人看 |

---

# 一 · 為什麼要管註解

## 註解會變假，而且沒有東西會紅

程式碼錯了，測試會紅、執行會報錯。註解錯了，什麼都不會發生。所以註解只要寫了「會隨別處改動而變的事」，遲早變假，而且只有剛好讀到它的人會發現。

## 在 agent 寫程式的 repo，變假的註解會被當成事實

agent 每次打開一個檔，會把裡面的註解全部讀進來，並且相信它。一句變假的註解，會讓下一個 agent 照著錯的前提改程式，或者花一整輪去「修正」它。註解越長，這種句子越多，真正重要的那一句也越容易被淹沒。

## 這個 repo 已經付了這個代價

`b06e5af1` 的快照，重現：`python scripts/dev/comment_churn.py b06e5af1`。量法：每個 commit 前後各把檔案解析成 AST、拿掉 docstring 後比較（註解本來就不在 AST 裡）；程式碼完全沒動、只有文字變的檔，算「只改註解」。

- `src/` 裡被改到的檔，「只改註解」的比例在 3–4 月是 0%，8–10 月是 22–28%。這是最直接的訊號：越來越多改動，程式碼一行沒動。
- 2026-08 起 `src/` 改過的非空行，48% 是註解或 docstring。這個數字要對照存量看：`src/` 本來就有 43% 的非空行是註解或 docstring。
- 2026-08 起有 158 個 commit 至少有一個 `src/` 檔只改了註解，其中 68 個的標題帶「審查」「review」或「收尾」字樣。這些 commit 不一定只改註解，多半同時修了別的問題。

只改註解的 commit 改的是什麼，沒有逐一分類。一個典型的例子是 `87b9f963`：一段註解寫著「`scripts/` 不在掃描範圍，因為它的 schema dict 只是報表 metadata」，那些腳本刪掉之後，這句就在講不存在的檔，要另開一個 commit 改成過去式。

外部來源的說法跟這個一致：

- Anthropic 的 prompting 指南建議 agent「不要替沒改的程式碼加 docstring 或註解；只在邏輯看不出來的地方加註解」（[Claude prompting best practices](https://platform.claude.com/docs/en/build-with-claude/prompt-engineering/claude-prompting-best-practices)）。
- Ousterhout：「註解應該描述程式碼看不出來的事」；「每件事只記一次，記兩份就不會有人維護」（[Stanford CS190 講義](https://web.stanford.edu/~ouster/cgi-bin/cs190-winter18/lecture.php?topic=comments)）。
- Google Python Style Guide：「不要描述程式碼」（[§3.8](https://google.github.io/styleguide/pyguide.html)）。
- Raymond Chen：PR 是你說明「為什麼要接受這個改動」的地方；程式碼註解只講程式碼本身，拿掉 PR 說明也要讀得懂（[The Old New Thing, 2026-08-12](https://devblogs.microsoft.com/oldnewthing/20260812-00?p=112607)）。
- Google 的 CL 說明指引：bug 編號、benchmark 結果放在 CL 描述裡（[CL descriptions](https://google.github.io/eng-practices/review/developer/cl-descriptions.html)）。

---

# 二 · 規則

## 1. 只寫看程式碼推不出來的事

值得寫的只有三種：

- **不明顯的原因**：為什麼是這個做法，換成看起來也對的另一種會怎樣。
- **限制**：這段程式碼依賴、但程式碼本身看不出來的前提。
- **刻意不做的事**：讀者會以為漏了、其實是故意不做的東西。

不寫程式碼在做什麼。函式名、變數名、簽章已經說了。

**不照做會怎樣**：每一句「在做什麼」的註解，程式一改就要跟著改，不改就變假。

**為什麼不是「每個函式都要有 docstring」**：Google 的 style guide 要求公開函式一定要有 docstring，但這個 repo 的讀者是 agent 和少數維護者，不是外部呼叫者。沒有東西要講的函式，硬寫的 docstring 只會複述簽章。

好例子（`src/recsys_tfb/pipelines/dataset/nodes.py:401`）——講的是「這一行還順便保證了什麼」，光看 `filter` 看不出來：

```python
# Decision — eligibility: only rows in the configured train months can be
# drawn. A month belongs to exactly one split (A24), so this is also what
# keeps train disjoint from val / test.
pool = sample_pool.filter(months_filter_as_date(time_col, train_months))
```

**誰擋得住**：沒有機械檢查。

## 2. 不寫歷史與過程，那是 commit 與 PR 的事

不寫進註解的：

- 票號（`#483`）。
- 這段程式碼以前長什麼樣、改名前叫什麼、第一版怎麼壞的。
- 誰審查過、誰重讀確認過、哪一輪找到的。

這些放 commit 訊息或 PR 說明。要追歷史的人會跑 `git log -L` 或 `git blame`，那裡的紀錄永遠對得上當時的程式碼。

**例外一：歷史是現在不能改的唯一原因。** 這時把它改寫成現在式的限制，不講故事。判準：拿掉這句，下一個人會不會改出同一個 bug？會就留，寫成限制。

**例外二：指向一個只記在票上的決定。** 有些決定沒有進 ADR，只寫在 spec 票裡，例如 `spec #426 decision D`。這種票號的角色跟 `ADR-0025 decision 1` 一樣，是「規則在這裡」的出處，不是歷史，可以寫。寫成 `#NNN decision X` 的形狀（X 是那張票裡決定的編號），測試才認得出它是出處。只寫 `(#379)` 的，分不出是出處還是歷史，一律算歷史。

壞例子（`src/recsys_tfb/pipelines/training/nodes.py:226`）——破折號後面那半句是改名史：

```python
# what lands it in the model version directory (pipeline-node-design.md
# rule 6 — before #483 both were called ``persist_*``, which they had stopped
# doing).
```

壞例子與改寫示意（`src/recsys_tfb/preprocessing.py:275`）——原句括號裡是故事，改寫成限制：

```
原句：  extract_Xy peak memory explodes (originally OOM-killed the val read).
改寫：  extract_Xy peak memory explodes.
```

括號裡那次 OOM 是一件發生過的事，不是這段程式碼保證得了的限制；同一句前面的「~70 B/value vs 4 B/float32」已經把限制講完了。

**不照做會怎樣**：歷史句子只對寫的那一刻有意義。改名、刪檔之後，它就變成在講一個不存在的東西；agent 讀到會去找那個東西，或把它加回來。

**為什麼不是「票號留著方便追」**：`git blame` 一步就追得到同一個票號，而且不會過期。註解裡的票號只告訴你「某次改動跟這行有關」，不告訴你這行現在要守什麼。

**誰擋得住**：**部分**。

- **擋得住的**：`tests/test_core/test_code_comments.py` 把 `src/recsys_tfb` 註解與 docstring 裡現有的 `#NNN`，逐檔列在 `tests/test_core/issue_refs_in_src.txt`。出現清單上沒有的，測試紅，並印出是哪個檔的哪個票號。清單上有、程式裡已經沒有的，也紅，要你把那一行刪掉，讓它回不來。逐檔比對，所以「刪三個舊的、加一個新的」照樣會紅。`#NNN decision X` 的出處不算。
- **擋不住的**：`tests/`、`scripts/` 不掃；別的寫法（`GH-483`、票的網址、`issue 483`）看不到；把舊註解連同票號搬到別的檔，測試會紅，但修法是在清單上搬那一行，這一步只能人看有沒有趁機多加。「原本／改名前／誰確認過」這類說法沒有可靠的字串可以 grep，只能人看。

## 3. 只寫這段程式碼自己保證得了的事

不寫：

- **數量**：「三個 node 用它」「62 個 node 裡 59 個」。
- **誰在呼叫它**：「給 X 用」「被 Y 呼叫」。
- **別處現在的樣子**：「`scripts/` 底下沒有這種檔」「這個設定目前沒人開」。

這些句子的真假，由**這段程式碼以外**的地方決定。別處一改，它就變假，而改別處的人不會想到要回來改這裡。

需要別處的行為才講得清後果時，講後果，位置只當附註。`pipelines/dataset/nodes.py:497` 那段就是這樣：重點是「空的 train_dev 會讓 early stopping 永遠不觸發」，括號裡的 `steps/hpo_scoring.py` 只是指路。

壞例子（`src/recsys_tfb/pipelines/inference/steps/identity.py:3`）——「三個」今天是對的，第四個呼叫端加進來時沒人會回來改：

```python
One function, because three nodes need the same answer and the answer is a
**decision** rather than something re-derivable from the schema: ...
```

改寫：`One function, because every node that names an output row must give the same answer, ...`

真實發生過的（commit `87b9f963`，`tests/test_core/test_architecture_constraints.py` 的 `#:` 註解）：

```python
#: this file exists to stop. ``scripts/`` is deliberately out -- its "schema"
#: dicts are report metadata, not parameters; see S5 in the constraints doc.
```

那些腳本刪掉之後，「its schema dicts are report metadata」就在講不存在的檔，要另開一個 commit 改成過去式。

**不照做會怎樣**：別處一改，這裡就要另開 commit 改（〈這個 repo 已經付了這個代價〉）。

**為什麼不是「寫了，改的時候記得一起改」**：改別處的人看不到這句；它不在他的 diff 裡。數量這一種，`pipeline-node-design.md` 規則 13 已經要求不寫（連 `docs/` 一起）；這條把同一個道理擴大到呼叫者與別處現況。

**誰擋得住**：沒有機械檢查。

## 4. 一件事只在一個地方講完整

ADR 或 `docs/` 已經講過的論證，註解只寫兩樣東西：

1. **一句話，說出這段程式碼不能怎麼改**。
2. 出處（ADR 編號或文件路徑）。

判準：拿掉出處之後，改這段的人還知不知道哪裡不能動？知道，這句就夠了。

壞例子：`src/recsys_tfb/pipelines/inference/steps/identity.py:15` 的 `scored_row_columns`，docstring 約 30 行，程式本體 2 行。大部分是在重講 ADR-0025 的理由，以及為什麼用減法不用加法。這個函式要守的只有一件事：

```python
"""The columns that identify one row of this pipeline's output.

``identity_columns`` minus every optional role: offline inference ignores
them (ADR-0025 decision 1), so its output has no ``event`` / ``occasion``
column even when the deployment declares one. Subtract, don't rebuild from
``base_key_columns + [item]``: a new optional role must drop out here with
no edit.
"""
```

**不照做會怎樣**：同一個理由寫在兩處，改的人只會改其中一處；另一處就成了跟現行決策對不上的說法，而且讀起來一樣權威。

**為什麼不是「全部寫在程式碼旁邊，比較好找」**：註解在這個 repo 一定會被讀（agent 每次開檔都讀），ADR 只在需要時讀。把 ADR 的論證搬進註解，是讓每一次開檔都付一次讀 ADR 的成本，而且多了一份會漂的副本。

**誰擋得住**：沒有機械檢查。

## 5. 函式 docstring 超過 10 行，要說得出為什麼不進 ADR

10 行不是上限，不是要你砍到 10 行。它是一個要停下來檢查的點：多出來的內容裡，有沒有規則 2 的歷史、規則 3 的別處現況、規則 4 的重講 ADR？有就拿掉。剩下的都是這個函式該講的事，再長也留著。

node 的 docstring 依 `pipeline-node-design.md` 規則 13 要講的三件事（為什麼不是另一種做法、選錯會不會報錯、成本量級）都算該講的事。例：`src/recsys_tfb/pipelines/dataset/nodes.py:432` 的 `split_train_keys`，docstring 將近 30 行，大部分是兩個檢查各自的成本與選錯的後果，檢查完不該砍。

模組 docstring 不設行數：它常是一整個模組負責的那件事（這個 repo 叫 concern）的地圖，長一點合理。但規則 1–4 一樣適用。

**影響面**：`b06e5af1` 的快照裡，921 個有 docstring 的函式，391 個超過 10 行；其中 36 個至少 15 行、而且是程式本體的 3 倍以上（重現：`python scripts/dev/comment_churn.py b06e5af1`）。所以這條會常常觸發，配上規則 6，等於「改到一個長 docstring 的函式，就檢查一次」。

**不照做會怎樣**：長 docstring 裡混著歷史與 ADR 的副本，函式一改，就有一大段文字要跟著核對，而且分不出哪幾句是真的要守的。

**為什麼是 10 行**：剛好裝得下「一句不明顯的地方＋限制＋刻意不做的事＋出處」。這個數字沒有外部依據，是這份文件的選擇；它只決定什麼時候要檢查，不決定留多少。

**誰擋得住**：沒有機械檢查。

## 6. 只整理你改到的函式；說錯才改，說法不改

- **改到一個函式，就照這份整理它的註解**：刪掉歷史與票號、把數量改成限制、把長論證縮成一句加出處。這是舊註解唯一的清理管道，沒有另外的全面清理。
- **你的改動讓別處的註解變假，就在同一個 PR 改掉那一句**。它算你改到的。不要留給審查去抓，那正是〈這個 repo 已經付了這個代價〉裡另開 commit 的來源。
- **其他沒改到的函式，不碰它的註解**。不順手潤稿，也不補註解。
- **審查說註解「錯了」（跟程式碼對不上），就改，或直接刪掉。審查只是建議換個說法，不改。**

例：`87b9f963` 那句「`scripts/` 的 schema dict 只是報表 metadata」，該在刪那些腳本的同一個 PR 就改掉。

**不照做會怎樣**：只改措辭的 commit 會讓 diff 變大、審查變長、`git blame` 失準，換來的只是另一種說法。

**為什麼不是「發現不好的註解就順手改」**：順手改會把 diff 擴散到這次沒動的程式碼，審查的人得多核對一批跟這次無關的文字，而那些文字又可能招來下一輪措辭意見。

**誰擋得住**：沒有機械檢查。

## 7. 能用測試或斷言釘住的，不寫成註解

如果註解描述的是一個可以檢查的條件（「這裡一定已經排序」「這個 list 不會是空的」），把它寫成 `assert`、`require_*` 守衛（命名見 [`pipeline-node-design.md`](pipeline-node-design.md) 規則 12）或測試。註解最多留一句「為什麼需要這個條件」。

**不照做會怎樣**：條件被打破時，註解不會告訴任何人；`assert` 會。

**為什麼不是「兩個都寫」**：兩個都寫時，註解就是 `assert` 的副本（規則 4）。

這條沒有從 repo 撈到好的對照組；有遇到再補。

**誰擋得住**：沒有機械檢查。
