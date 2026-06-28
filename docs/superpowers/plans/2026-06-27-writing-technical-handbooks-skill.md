# `writing-technical-handbooks` Skill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把《Spark 優化參考手冊》round-1/2/3 累積的「撰寫技術自學手冊」方法論，固化成可複用的個人 skill（SKILL.md + references/ + 2 支驗證腳本）。

**Architecture:** 依 `superpowers:writing-skills` 的 RED-GREEN-REFACTOR（按比例，technique/reference 型）。先跑無 skill 的 baseline 場景記錄缺陷（RED）→ 建腳本＋references＋SKILL.md（GREEN）→ 派有 skill 的 subagent 驗證並堵漏洞（REFACTOR）。**交付物在 worktree 內以 `docs/superpowers/skills/writing-technical-handbooks/` 為 tracked 源、每 task commit；最後一個 task 才 deploy（copy）到 `~/.claude/skills/writing-technical-handbooks/`**（spec §9 的 end state，外加版本歷史。此為對 spec §9「直接寫 untracked」的小幅 refinement，動工前若反對請提出）。

**Tech Stack:** Markdown；Python 3（純 stdlib）＋ pytest（兩支腳本的 self-test）；複用 superpowers `brainstorming` / `dispatching-parallel-agents`（軟依賴）。

**路徑變數（全程沿用）：**
- `SRC` = `/Users/curtislu/projects/recsys_tfb/.worktrees/handbook-skill/docs/superpowers/skills/writing-technical-handbooks`
- `DEPLOY` = `/Users/curtislu/.claude/skills/writing-technical-handbooks`
- `HB` = `/Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook`（素材來源，唯讀；多數素材在此分支）
- `PY` = `/Users/curtislu/projects/recsys_tfb/.venv/bin/python`（純 stdlib，亦可用系統 `python3`）

**Spec：** `docs/superpowers/specs/2026-06-27-writing-technical-handbooks-skill-design.md`（決議 D1–D10、修正 M1–M4）。

---

## Task 0: RED — 無 skill baseline 場景（先看它失敗）

**Files:**
- Create: `SRC/.dev/baseline-red.md`（開發期記錄，最後不 deploy）

定一個**固定、可重現、域無關**的微型手冊任務，派一個**沒有本 skill** 的 subagent 做，逐字記錄它漏了什麼。同一任務 Task 11 會再用（GREEN 對照）。

- [ ] **Step 1: 用固定 prompt 派 baseline subagent（無 skill、無 WebFetch）**

派 general-purpose subagent，prompt 固定為：

> 「為一本技術自學手冊寫**開章**（本章前提＋前兩節草稿），主題：**用 `EXPLAIN` 讀懂 PostgreSQL 查詢計畫**；讀者：**會寫多表 SQL、但沒看過查詢計畫的資料分析師**。寫完請自我審稿一次，列出你認為的弱點。把成品與自審都回給我。」

- [ ] **Step 2: 把回覆原樣存成 `SRC/.dev/baseline-red.md`，標注觀察到的缺口**

對照 spec 的元件清單，逐條記錄 baseline **沒做到**的（每條引 baseline 原文佐證），預期會見到：缺讀者 persona 驅動的術語 gloss、無「本章前提→目錄→capstone→取捨→一句話帶走」體例、無 capstone 貫穿範例、無官方來源紀律、無品質關卡（純標點/錨點/連結）、自審流於形容詞。

- [ ] **Step 3: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/handbook-skill
git add docs/superpowers/skills/writing-technical-handbooks/.dev/baseline-red.md
git commit -m "test(red): no-skill baseline 開章場景的缺口記錄"
```

**Acceptance:** `baseline-red.md` 列出 ≥5 條具體缺口、每條有 baseline 原文佐證——這些缺口就是後續每個元件要補的「失敗測試」。

---

## Task 1: `anchor_check.py` — slugger 錨點驗證 + 連結健檢（TDD）

**Files:**
- Create: `SRC/references/scripts/anchor_check.py`
- Test: `SRC/references/scripts/test_anchor_check.py`

- [ ] **Step 1: 寫失敗測試 `test_anchor_check.py`**

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from anchor_check import slugify, heading_slugs, check_dir


def test_slugify_known_handbook_anchors():
    assert slugify("全書地圖") == "全書地圖"
    assert slugify("場景速查：依你的工作型態") == "場景速查依你的工作型態"
    assert slugify("5.5 管好檔案大小：小碎檔問題") == "55-管好檔案大小小碎檔問題"
    assert slugify("Spark 怎麼跑你的 SQL") == "spark-怎麼跑你的-sql"
    assert slugify("用 `EXPLAIN` 找瓶頸") == "用-explain-找瓶頸"
    assert slugify("（進階）何時與如何改用 PySpark DataFrame API") == \
        "進階何時與如何改用-pyspark-dataframe-api"


def test_heading_slugs_dedup_and_skips_fenced_code():
    md = "# A\n```\n## not-a-heading\n```\n## A\n"
    assert heading_slugs(md) == ["a", "a-1"]


def test_check_dir_detects_broken_anchor(tmp_path):
    (tmp_path / "a.md").write_text("# T\n[x](b.md#missing)\n", encoding="utf-8")
    (tmp_path / "b.md").write_text("## 場景速查：依你的工作型態\n", encoding="utf-8")
    assert any("missing anchor" in e for e in check_dir(tmp_path))


def test_check_dir_passes_valid_anchor(tmp_path):
    (tmp_path / "a.md").write_text("[x](b.md#場景速查依你的工作型態)\n", encoding="utf-8")
    (tmp_path / "b.md").write_text("## 場景速查：依你的工作型態\n", encoding="utf-8")
    assert check_dir(tmp_path) == []


def test_check_dir_detects_missing_file(tmp_path):
    (tmp_path / "a.md").write_text("[x](nope.md)\n", encoding="utf-8")
    assert any("missing file" in e for e in check_dir(tmp_path))


def test_check_dir_ignores_links_in_code_fence(tmp_path):
    (tmp_path / "a.md").write_text("# T\n```\n[x](nope.md)\n```\n", encoding="utf-8")
    assert check_dir(tmp_path) == []
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `cd $SRC/references/scripts && $PY -m pytest test_anchor_check.py -q`
Expected: FAIL（`ModuleNotFoundError: anchor_check`）

- [ ] **Step 3: 寫 `anchor_check.py`**

```python
#!/usr/bin/env python3
"""Validate intra-handbook markdown anchors and relative file links.

slugify() reproduces GitHub's heading-anchor algorithm closely enough for
handbook use: lowercase; keep alphanumerics (incl. CJK), spaces, '-', '_';
drop all other characters; spaces -> '-'; NO hyphen collapsing. Validated
against real handbook anchors in test_anchor_check.py.

Usage:
    python3 anchor_check.py <dir>        # scan a directory tree of .md files
    python3 anchor_check.py --self-test  # run built-in assertions
Exit: 0 = all links resolve, 1 = broken links, 2 = usage error.
"""
from __future__ import annotations
import re
import sys
from pathlib import Path

_HEADING_RE = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$")
_LINK_RE = re.compile(r"\[[^\]]*\]\(([^)]+)\)")


def slugify(text: str) -> str:
    text = text.strip().lower()
    kept = [c for c in text if c.isalnum() or c in " -_"]
    return "".join(kept).replace(" ", "-")


def heading_slugs(md_text: str) -> list[str]:
    slugs: list[str] = []
    seen: dict[str, int] = {}
    in_fence = False
    for line in md_text.splitlines():
        if line.lstrip().startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        m = _HEADING_RE.match(line)
        if not m:
            continue
        base = slugify(m.group(2))
        if base in seen:
            seen[base] += 1
            slugs.append(f"{base}-{seen[base]}")
        else:
            seen[base] = 0
            slugs.append(base)
    return slugs


def check_dir(root: Path) -> list[str]:
    root = Path(root)
    md_files = sorted(root.rglob("*.md"))
    slug_cache: dict[Path, set[str]] = {
        f.resolve(): set(heading_slugs(f.read_text(encoding="utf-8"))) for f in md_files
    }
    errors: list[str] = []
    for f in md_files:
        in_fence = False
        for line_no, line in enumerate(f.read_text(encoding="utf-8").splitlines(), 1):
            if line.lstrip().startswith("```"):
                in_fence = not in_fence
                continue
            if in_fence:
                continue
            for target in _LINK_RE.findall(line):
                if target.startswith(("http://", "https://", "mailto:", "#!")):
                    continue
                file_part, _, anchor = target.partition("#")
                if file_part:
                    tgt = (f.parent / file_part).resolve()
                    if not tgt.exists():
                        errors.append(f"{f}:{line_no}: missing file -> {target}")
                        continue
                else:
                    tgt = f.resolve()
                if anchor:
                    slugs = slug_cache.get(tgt)
                    if slugs is None and tgt.exists():
                        slugs = set(heading_slugs(tgt.read_text(encoding="utf-8")))
                        slug_cache[tgt] = slugs
                    if not slugs or anchor not in slugs:
                        errors.append(f"{f}:{line_no}: missing anchor -> {target}")
    return errors


def _self_test() -> None:
    for text, expected in {
        "全書地圖": "全書地圖",
        "場景速查：依你的工作型態": "場景速查依你的工作型態",
        "5.5 管好檔案大小：小碎檔問題": "55-管好檔案大小小碎檔問題",
        "Spark 怎麼跑你的 SQL": "spark-怎麼跑你的-sql",
        "用 `EXPLAIN` 找瓶頸": "用-explain-找瓶頸",
    }.items():
        got = slugify(text)
        assert got == expected, f"slugify({text!r})={got!r} != {expected!r}"
    assert heading_slugs("# A\n# A\n") == ["a", "a-1"]
    print("anchor_check self-test: OK")


def main(argv: list[str]) -> int:
    if "--self-test" in argv:
        _self_test()
        return 0
    if len(argv) != 2:
        print(__doc__)
        return 2
    errors = check_dir(Path(argv[1]))
    if errors:
        print(f"FAIL: {len(errors)} broken link(s):")
        for e in errors:
            print("  " + e)
        return 1
    print(f"OK: all relative links and anchors resolve under {argv[1]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
```

- [ ] **Step 4: 跑測試＋self-test 確認通過**

Run: `cd $SRC/references/scripts && $PY -m pytest test_anchor_check.py -q && $PY anchor_check.py --self-test`
Expected: PASS（6 tests）＋ `anchor_check self-test: OK`

- [ ] **Step 5: 對真實手冊跑一次（煙霧測試，確認能掃真資料）**

Run: `$PY $SRC/references/scripts/anchor_check.py $HB/docs/handbooks/spark-tuning`
Expected: 印出 `OK:` 或具體 broken 清單（兩者皆證明腳本能運作；若報 broken 是手冊既有問題，不在本 plan 範圍，記下即可）。

- [ ] **Step 6: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/handbook-skill
git add docs/superpowers/skills/writing-technical-handbooks/references/scripts/anchor_check.py \
        docs/superpowers/skills/writing-technical-handbooks/references/scripts/test_anchor_check.py
git commit -m "feat(scripts): anchor_check.py — slugger 錨點驗證 + 連結健檢 + self-test"
```

---

## Task 2: `punctuation_audit.py` + `punctuation_set.txt` — 純標點稽核（TDD）

**Files:**
- Create: `SRC/references/scripts/punctuation_set.txt`（標點集單一來源，language-conventions.md 引用同一份）
- Create: `SRC/references/scripts/punctuation_audit.py`
- Test: `SRC/references/scripts/test_punctuation_audit.py`

- [ ] **Step 1: 建 `punctuation_set.txt`**（單行，含 em-dash、空白、全形標點、刪節號）

```
— ，：。（）；…
```

- [ ] **Step 2: 寫失敗測試 `test_punctuation_audit.py`**

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from punctuation_audit import audit, load_punctuation, _DEFAULT_PUNCT_FILE

PUNCT = {"—", " ", "，", "：", "。", "（", "）", "；", "…"}


def test_punctuation_only_true():
    diff = "-先看懂它怎麼跑——再調快。\n+先看懂它怎麼跑，再調快。\n"
    r = audit(diff, PUNCT)
    assert r["punctuation_only"] and r["symmetric"]


def test_word_change_detected():
    diff = "-先看懂它怎麼跑——再調快。\n+先看懂它如何跑，再調快。\n"
    assert not audit(diff, PUNCT)["punctuation_only"]


def test_ignores_diff_headers():
    diff = "--- a/x.md\n+++ b/x.md\n@@ -1 +1 @@\n-甲——乙\n+甲，乙\n"
    r = audit(diff, PUNCT)
    assert r["punctuation_only"]
    assert r["removed"] == ["甲——乙"]


def test_punctuation_set_file_loads():
    punct = load_punctuation(_DEFAULT_PUNCT_FILE)
    assert {"，", "—", " "} <= punct
```

- [ ] **Step 3: 跑測試確認失敗**

Run: `cd $SRC/references/scripts && $PY -m pytest test_punctuation_audit.py -q`
Expected: FAIL（`ModuleNotFoundError: punctuation_audit`）

- [ ] **Step 4: 寫 `punctuation_audit.py`**

```python
#!/usr/bin/env python3
"""Prove a unified diff changed ONLY punctuation, not words.

Strips the language's punctuation set from removed (-) and added (+) content
lines; if the resulting ordered lists are identical, the edit is punctuation-
only. Also reports insertion/deletion symmetry. The punctuation set is read
from punctuation_set.txt (single shared source, also quoted in
language-conventions.md).

Usage:
    git diff | python3 punctuation_audit.py
    python3 punctuation_audit.py --file changes.diff
    python3 punctuation_audit.py --self-test
Exit: 0 = punctuation-only, 1 = words changed, 2 = usage error.
"""
from __future__ import annotations
import sys
from pathlib import Path

_DEFAULT_PUNCT_FILE = Path(__file__).parent / "punctuation_set.txt"


def load_punctuation(path: Path = _DEFAULT_PUNCT_FILE) -> set[str]:
    return set(path.read_text(encoding="utf-8").rstrip("\n"))


def _strip(line: str, punct: set[str]) -> str:
    return "".join(c for c in line if c not in punct)


def audit(diff_text: str, punct: set[str]) -> dict:
    removed: list[str] = []
    added: list[str] = []
    for line in diff_text.splitlines():
        if line.startswith(("+++", "---", "@@", "diff ", "index ",
                            "new file", "deleted file", "rename ", "similarity ")):
            continue
        if line.startswith("-"):
            removed.append(line[1:])
        elif line.startswith("+"):
            added.append(line[1:])
    rs = [_strip(l, punct) for l in removed]
    as_ = [_strip(l, punct) for l in added]
    return {"punctuation_only": rs == as_, "symmetric": len(removed) == len(added),
            "removed": removed, "added": added}


def _self_test() -> None:
    punct = load_punctuation()
    r = audit("-甲怎麼跑——再調快。\n+甲怎麼跑，再調快。\n", punct)
    assert r["punctuation_only"] and r["symmetric"], r
    r2 = audit("-甲怎麼跑——再調快。\n+甲如何跑，再調快。\n", punct)
    assert not r2["punctuation_only"], r2
    print("punctuation_audit self-test: OK")


def main(argv: list[str]) -> int:
    if "--self-test" in argv:
        _self_test()
        return 0
    punct = load_punctuation()
    if "--file" in argv:
        diff_text = Path(argv[argv.index("--file") + 1]).read_text(encoding="utf-8")
    else:
        diff_text = sys.stdin.read()
    r = audit(diff_text, punct)
    if r["punctuation_only"]:
        print(f"OK: punctuation-only edit (-{len(r['removed'])}/+{len(r['added'])} "
              f"lines, symmetric={r['symmetric']}).")
        return 0
    print("FAIL: non-punctuation change detected. First differing pair:")
    for a, b in zip([_strip(l, punct) for l in r["removed"]],
                    [_strip(l, punct) for l in r["added"]]):
        if a != b:
            print(f"  - {a!r}\n  + {b!r}")
            break
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
```

- [ ] **Step 5: 跑測試＋self-test 確認通過**

Run: `cd $SRC/references/scripts && $PY -m pytest test_punctuation_audit.py -q && $PY punctuation_audit.py --self-test`
Expected: PASS（4 tests）＋ `punctuation_audit self-test: OK`

- [ ] **Step 6: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/handbook-skill
git add docs/superpowers/skills/writing-technical-handbooks/references/scripts/
git commit -m "feat(scripts): punctuation_audit.py + punctuation_set.txt — 純標點稽核 + self-test"
```

---

## Task 3: `references/language-conventions.md`（繁中專屬，D6）

**Files:**
- Create: `SRC/references/language-conventions.md`
- Read source: `$HB/docs/handbooks/handbook-writing-guide.md`（§8 符號紀律可借）；memory `feedback_traditional_chinese`、`feedback_handbook_writing_style`。

- [ ] **Step 1: 寫 `language-conventions.md`**，必含下列段落與要點（**繁中專屬**，語言無關原則歸 writing-style.md）：

1. **繁體中文**：一律繁體、不得出簡體字。
2. **專有名詞翻譯政策**（grilling 新增）：有公認慣用譯法才譯；**沒有慣用譯法就用原文**，不硬翻、不自創譯名；首見可「原文（簡短說明）」。給 ≥3 個正例（如 shuffle/executor/predicate pushdown 用原文）與 1 反例（硬翻成生造詞）。
3. **全形標點**：中文行內用全形標點；中英混排時的空白慣例。
4. **稽核標點集（與腳本同源）**：明列 `punctuation_set.txt` 的內容並標注「**此為 `references/scripts/punctuation_set.txt` 的同步引用；要改標點集改那個檔，本段同步**」。
5. **破折號 `——` 紀律**：只保留「術語定義」「平行標籤」，其餘以逗號/句號替代（連結 round-3 教訓）。

- [ ] **Step 2: 一致性檢查**

Run: `grep -F "— ，：。（）；…" $SRC/references/scripts/punctuation_set.txt`
並人工確認 `language-conventions.md` §4 引用的標點集字元與該檔逐字相同。
Expected: grep 命中（兩處同源）。

- [ ] **Step 3: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/handbook-skill
git add docs/superpowers/skills/writing-technical-handbooks/references/language-conventions.md
git commit -m "docs(ref): language-conventions.md — 繁中慣例 + 術語翻譯政策 + 同源標點集"
```

**Acceptance:** 含上述 5 段；§4 標點集與 `punctuation_set.txt` 逐字一致；術語翻譯政策有正反例。

---

## Task 4: `references/writing-style.md`（語言無關散文原則，D6）

**Files:**
- Create: `SRC/references/writing-style.md`
- Read source: `$HB/docs/handbooks/handbook-writing-guide.md`（§1–9、§12）——**泛化、去數學味**（保留「抽象主張要具體數字落地」原則，但抽掉 GBDT 專屬例子）。

- [ ] **Step 1: 寫 `writing-style.md`**，必含（語言無關；繁中專屬不放這）：

1. **簡潔但夠描述性**（grilling 新增）：不為精簡犧牲讓讀者能動手的必要描述；給正反例各一。
2. **宣稱與新名詞要有前後脈絡**（grilling 新增）：任何宣稱先給為什麼/在什麼條件下成立；新名詞首見當場 gloss（白話一句）再用。
3. **抽象主張要具體落地**（泛化自 guide §3）：用具體可驗的數字/實例支撐，不只形容詞。
4. **一個貫穿範例（capstone）前後呼應**（guide §4）：問題處與解法處用同一組設定。
5. **結論誠實、與內文一致**（guide §5）：不寫漂亮話、不過度宣稱；承認沒有萬靈丹。
6. **不洩漏寫作鷹架**（guide §6）：不對讀者講「本節暫緩/撰寫中」、不用代號代稱概念、標題不放給自己的備註。
7. **符號/術語紀律**（泛化自 guide §8）：首次定義、不一符二義、跨章一致。
8. **通用原理先於具體案例、兩者分節**（guide §2，連結 memory `feedback_general_before_specific`）。

每條 1–3 句＋（可選）一個極短正/反例。**交叉引用**：繁中用字規範見 `language-conventions.md`；章結構見 `checklists/chapter-template.md`。

- [ ] **Step 2: 自查無數學專屬殘留**

Run: `grep -nE "GBDT|方差|梯度|LightGBM|機率校準" $SRC/references/writing-style.md`
Expected: 無輸出（已泛化）。

- [ ] **Step 3: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/handbook-skill
git add docs/superpowers/skills/writing-technical-handbooks/references/writing-style.md
git commit -m "docs(ref): writing-style.md — 語言無關散文原則(泛化自 handbook-writing-guide + 補 3 條)"
```

**Acceptance:** 含 8 條；grep 無數學殘留；新增的「簡潔但描述性」「宣稱/新名詞要脈絡」明確在列。

---

## Task 5: `references/persona-elicitation.md`（D10，最高槓桿）

**Files:**
- Create: `SRC/references/persona-elicitation.md`
- Source: spec §6.2（六維表、引導腳本、非專家 fallback、persona spec 模板逐字採用）。

- [ ] **Step 1: 寫 `persona-elicitation.md`**，必含：

1. **六維具體錨點表**（背景知識地板/天花板、job-to-be-done、深度期待、工具/介面、環境/約束、反例），每維附「具體錨點 ＞ 形容詞」的正例（採 spec §6.2 範例）。
2. **環境/約束維度註**：環境限制（No UDF/無外網之類）的正當歸宿在 persona，不是手冊普世規則。
3. **引導腳本**（複用 brainstorming、一次一問）：① 先要真實 proxy 人物 → ② 逐維逼具體錨點 → ③ 問第二讀者（雙 persona 上限 2–3）。
4. **非專家 fallback**：使用者不熟主題時，skill 派研究步驟（官方來源）擬「前置知識 ladder」，使用者只標「會/半懂/不會」；翻轉負擔（只需懂讀者、不需懂主題分類學）。
5. **兩個減壓閥**：proxy 人物法降焦慮；persona 不必一次到位、靠 R2/R3＋Follower review loop 校準。
6. **persona spec 產出模板**（逐字採 spec §6.2 的 code block）＋「注入每個 reviewer prompt（R2/R3/F 依此扮演）」。

- [ ] **Step 2: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/handbook-skill
git add docs/superpowers/skills/writing-technical-handbooks/references/persona-elicitation.md
git commit -m "docs(ref): persona-elicitation.md — 六維錨點 + proxy 法 + 非專家 fallback + spec 模板"
```

**Acceptance:** 六維齊、含非專家 fallback 與 persona spec 模板；標明產物注入 R2/R3/F。

---

## Task 6: `references/checklists/`（chapter-template / reader-gotchas / reviewer-checklist）

**Files:**
- Create: `SRC/references/checklists/chapter-template.md`
- Create: `SRC/references/checklists/reader-gotchas.md`
- Create: `SRC/references/checklists/reviewer-checklist.md`
- Source: `$HB/docs/handbooks/handbook-writing-guide.md`（§10 體例、§11 reader 清單、§12 進階校驗）；memory `feedback_handbook_writing_style`、`project_handbook_writing_skill` §7。

- [ ] **Step 1: `chapter-template.md`** — 每章骨架（體例）：本章前提 → 本章目錄（GitHub 錨點）→ 內文（§x.y）→ capstone 貫穿範例 → 取捨 → 一句話帶走 → 上下章導覽 → 每段 📚 來源 footer；缺章 forward-ref 用軟指標不下 404 硬連結。每段一句話說它的作用。

- [ ] **Step 2: `reader-gotchas.md`** — 讀者反覆會卡的點清單：術語/縮寫首見沒當場 gloss；缺前置概念錨點（如無背景者缺某前提詞）；只給原則不給可操作的橋；工具型範例沒釘版本。每條附「徵兆 → 修法」。

- [ ] **Step 3: `reviewer-checklist.md`** — 定稿前自審清單（融 guide §12 進階校驗）：貫穿範例參考點一致（沒偷換）、必然 vs 巧合標明、簡化斷言對規模壓測、方向性宣稱先算正負號、引入新機制回頭校舊章、旋鈕給起手值+調法、引用作者/年份正確、軟建議 vs 硬限制分清、鷹架/後設旁白零殘留。

- [ ] **Step 4: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/handbook-skill
git add docs/superpowers/skills/writing-technical-handbooks/references/checklists/
git commit -m "docs(ref): checklists — chapter-template / reader-gotchas / reviewer-checklist"
```

**Acceptance:** 三檔齊；chapter-template 的體例順序與 spec §5 一致；reader-gotchas 每條有徵兆→修法。

---

## Task 7: `references/landing-page-recipe.md`

**Files:**
- Create: `SRC/references/landing-page-recipe.md`
- Source: `$HB/docs/handbooks/spark-tuning/index.md`（成品範例）；spec §5 landing-page 條目；memory `project_spark_tuning_handbook`（index 迭代教訓）。

- [ ] **Step 1: 寫 `landing-page-recipe.md`**，必含：

1. **讀者視角動線**：鳥瞰（概念地圖 mermaid）→ 怎麼用（依即時需求/依工作型態場景表）→ 章節導覽（分段標題＋各段小表）→ 讀者環境置底。
2. **反面教訓**：landing page 易**過瘦失脈絡**（只剩表格）——要保留「全書一句話主軸」「概念圖導讀」「章節分段」；表格別把 band 標籤塞進儲存格（醜），用粗體段標題＋獨立小表。
3. 一個**精簡 worked 範例骨架**（mermaid + 場景表 + 分段導覽），文字指路 `spark-tuning/index.md` 為完整實例（非依賴）。

- [ ] **Step 2: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/handbook-skill
git add docs/superpowers/skills/writing-technical-handbooks/references/landing-page-recipe.md
git commit -m "docs(ref): landing-page-recipe.md — 概念地圖式首頁 + 過瘦失脈絡教訓"
```

---

## Task 8: `references/reviewer-prompts/` 五個必備角色（R1/R2/R3/C/P，M2 填好的具體範例）

**Files:**
- Create: `SRC/references/reviewer-prompts/R1-technical.md`
- Create: `SRC/references/reviewer-prompts/R2-reader-novice.md`
- Create: `SRC/references/reviewer-prompts/R3-reader-advanced.md`
- Create: `SRC/references/reviewer-prompts/C-architecture.md`
- Create: `SRC/references/reviewer-prompts/P-pedagogy.md`
- Source: `$HB/docs/superpowers/plans/2026-06-22-spark-handbook-round2-revision-plan.md`、`…/2026-06-14-spark-tuning-handbook-plan.md`、`$HB/docs/superpowers/specs/2026-06-22-spark-handbook-round2-review-design.md`（5 角色 prompt 與審稿框架）。

每檔是**填好的具體範例**（用 Spark 手冊 persona/主題實填，非 `{slot}` 抽象挖空，M2），並在開頭標一行「**改用法**：把下列 persona/主題換成你 `persona-elicitation.md` 產出的 spec」。

- [ ] **Step 1: `R1-technical.md`** — 技術正確性：版本釘死、**只認官方來源**（列出可接受來源類型）、逐條輸出 ✅/❌（正解＋出處）/⚠️、與 persona 無關。
- [ ] **Step 2: `R2-reader-novice.md`** — 初階讀者 persona（填入具體人設），問「讀得懂嗎＋我這份工作做得了嗎」，術語首見沒定義就標。
- [ ] **Step 3: `R3-reader-advanced.md`** — 進階讀者 persona，問「深度夠營運嗎、哪裡過深可標進階可跳」。
- [ ] **Step 4: `C-architecture.md`** — 跨全書里程碑審（非逐章）：能力地圖→缺口、章序/依賴、深度、跨章一致性（交叉引用/術語/notation/config 值/footer）。
- [ ] **Step 5: `P-pedagogy.md`** — 教學法：對標名著編排＋Diátaxis 四象限（抓 tutorial/how-to/reference/explanation 模式混淆）＋只看教學法。
- [ ] **Step 6: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/handbook-skill
git add docs/superpowers/skills/writing-technical-handbooks/references/reviewer-prompts/
git commit -m "docs(ref): reviewer-prompts R1/R2/R3/C/P — 填好的具體範例(M2)"
```

**Acceptance:** 五檔齊；皆為填好的具體範例＋頂部「改用法」一行；R1 明列只認官方來源。

---

## Task 9: `references/reviewer-prompts/F-follower.md`（optional 第 6 角色，D9）

**Files:**
- Create: `SRC/references/reviewer-prompts/F-follower.md`
- Source: spec §6.1（gating rubric、環境三層、兩條護欄逐字採用）。

- [ ] **Step 1: 寫 `F-follower.md`**，必含：

1. **角色定位**：只照手冊文字實作，產出脈絡充足性探針（卡在哪/被迫猜/得動用手冊沒給的知識），非「跑成功沒」。
2. **gating rubric**：程序具體性 × 環境可達性 × 對 R2/R3 的邊際增益，三者皆過才派；附環境三層表（Tier 0 紙上 / Tier 1 本機沙盒 / Tier 2 真實環境搆不到→標待驗）。
3. **兩條護欄**：只准用手冊文字（伸手到文字外要記一筆）；環境失敗 vs 脈絡缺口**分欄**回報。
4. **輸出格式**：表格欄＝步驟｜卡點類型(環境失敗/脈絡缺口)｜缺什麼脈絡｜建議補在哪。
5. 頂部「**改用法**」一行（換 persona/主題/環境層）。

- [ ] **Step 2: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/handbook-skill
git add docs/superpowers/skills/writing-technical-handbooks/references/reviewer-prompts/F-follower.md
git commit -m "docs(ref): F-follower.md — optional 動手實作脈絡探針(gating+三層+兩護欄)"
```

---

## Task 10: `SKILL.md`（核心，M1/M3）

**Files:**
- Create: `SRC/SKILL.md`
- Source: spec §3/§4/§6/§7（工作流、guardrails）；`project_handbook_writing_skill` §7（guardrails）。

- [ ] **Step 1: 寫 frontmatter（M1：description 只寫「Use when…」、不摘要流程；name 僅字母數字連字號）**

```markdown
---
name: writing-technical-handbooks
description: Use when authoring or revising a multi-chapter technical self-study handbook or guide for a defined reader, including starting a new handbook, running a feedback-driven revision round, or reviewing chapters for reader comprehension and technical correctness. Not for RFCs, design docs, API references, or single how-tos.
disable-model-invocation: true
---
```

- [ ] **Step 2: 寫 SKILL.md 本體**（目標 <500 字；細節下沉 references/，用名稱交叉引用、不用 `@` 強載）。骨架與必含：

```markdown
# Writing Technical Handbooks

## Overview
固化多章節技術自學手冊的撰寫方法。核心：讀者 persona 驅動深度、每章固定體例、多角色審稿、可執行品質關卡。

## When to Use / Not
（鎖定教學型多章節手冊；排除 RFC/runbook/API ref/單篇 how-to。）

## Two Entry Points
- 從零寫新手冊 → 需求鎖定 → 分章 Step A–F。
- 回饋驅動修訂輪 → intake → 盤點(grep) → 分階段(每階段一 commit)。
（後段共用：5 審稿角色 +（optional）Follower → 三級 triage → 品質關卡 → commit。）

## Four User Gates（關卡式）
① spec 確認方向 ② 計畫過目 ③ 每章/階段 triage 拍板 ④ 風格大改先樣本+分級選項。

## Reviewer Roles（references/reviewer-prompts/）
R1 技術(只認官方) / R2 初階讀者 / R3 進階讀者 / C 架構一致性 / P 教學法 /（optional）F Follower 動手實作。

## Quality Gates（references/scripts/）
純標點稽核 `punctuation_audit.py`、錨點+連結 `anchor_check.py`，每輪改完跑 + C 一致性複核。

## Guardrails
不洩漏鷹架；審稿後加料一定補審；subagent 苦工無 WebFetch + 即時 live log；技術只認官方來源；環境約束放 persona 不放手冊普世規則；機械重排用單次掃描 re.sub + grep 驗證。

## References（漸進揭露）
persona-elicitation / writing-style / language-conventions / checklists/* / landing-page-recipe / reviewer-prompts/*。
REQUIRED（軟依賴）：superpowers:brainstorming（需求）、superpowers:dispatching-parallel-agents（批次苦工）；缺則退化為文字指引。
```

實際內文把每段補成 1–3 句可操作敘述（非僅標題），但全篇精簡。

- [ ] **Step 3: 驗 frontmatter 合法 + 字數**

Run: `cd $SRC && head -5 SKILL.md && wc -w SKILL.md`
Expected: frontmatter 有 name/description/disable-model-invocation；body 字數合理（目標本體 <500 字，含 references 清單可略超，過多則精簡）。

- [ ] **Step 4: 全 skill 錨點/連結健檢**

Run: `$PY $SRC/references/scripts/anchor_check.py $SRC`
Expected: `OK: all relative links and anchors resolve`（SKILL.md 對 references 的相對連結都成立）。

- [ ] **Step 5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/handbook-skill
git add docs/superpowers/skills/writing-technical-handbooks/SKILL.md
git commit -m "feat(skill): SKILL.md — 關卡式工作流 + guardrails + references map(M1 description)"
```

**Acceptance:** frontmatter 合法、description 不摘要流程（M1）；本體精簡；`anchor_check.py $SRC` 通過。

---

## Task 11: GREEN — 有 skill 重跑 baseline（確認它通過）

**Files:**
- Create: `SRC/.dev/green-verify.md`

- [ ] **Step 1: 派有 skill 的 subagent 做 Task 0 同一任務**

派 general-purpose subagent，prompt：先讀 `$SRC/SKILL.md` 與其 references，再做 Task 0 那題（同主題同 persona），全程留 live log 到 `$SRC/.dev/green-verify.md`。

- [ ] **Step 2: 對照 `baseline-red.md` 逐缺口核對是否補上**

把 Task 0 記的 ≥5 缺口逐條標 ✅補上/❌仍缺，引 green 成品佐證。

- [ ] **Step 3: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/handbook-skill
git add docs/superpowers/skills/writing-technical-handbooks/.dev/green-verify.md
git commit -m "test(green): 有 skill 重跑 baseline，逐缺口核對改善"
```

**Acceptance:** Task 0 的缺口多數轉 ✅；仍 ❌ 的列為 Task 12 待堵漏洞。

---

## Task 12: REFACTOR — 堵漏洞並複審

**Files:**
- Modify: 依 GREEN 發現的問題改 `SRC/SKILL.md` 或對應 `references/*`

- [ ] **Step 1: 針對 Task 11 仍 ❌ 或新發現的誤用，改對應檔**（指引不清/缺口未覆蓋 → 補明確指引）。

- [ ] **Step 2: 改完補審（審稿後加料一定補審）**

Run: `$PY $SRC/references/scripts/anchor_check.py $SRC`
並對改動跑純標點稽核（若僅標點）：`cd /Users/curtislu/projects/recsys_tfb/.worktrees/handbook-skill && git diff -- docs/superpowers/skills | $PY $SRC/references/scripts/punctuation_audit.py`（非純標點改動則人工複核）。
Expected: anchor 通過；標點稽核結果與改動性質相符。

- [ ] **Step 3: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/handbook-skill
git add docs/superpowers/skills/writing-technical-handbooks/
git commit -m "refactor(skill): 依 GREEN 堵漏洞 + 補審"
```

**Acceptance:** GREEN 的 ❌ 清空或有明確取捨理由；品質關卡複審通過。

---

## Task 13: Deploy 到 `~/.claude/skills/` + 更新 memory

**Files:**
- Create: `DEPLOY/`（copy 自 `SRC/`，排除 `.dev/`）
- Modify: `/Users/curtislu/.claude/projects/-Users-curtislu-projects-recsys-tfb/memory/MEMORY.md`
- Modify: `/Users/curtislu/.claude/projects/-Users-curtislu-projects-recsys-tfb/memory/project_handbook_writing_skill.md`

- [ ] **Step 1: Deploy（copy，排除開發期 `.dev/`）**

```bash
rm -rf /Users/curtislu/.claude/skills/writing-technical-handbooks
mkdir -p /Users/curtislu/.claude/skills/writing-technical-handbooks
cp -R /Users/curtislu/projects/recsys_tfb/.worktrees/handbook-skill/docs/superpowers/skills/writing-technical-handbooks/. \
      /Users/curtislu/.claude/skills/writing-technical-handbooks/
rm -rf /Users/curtislu/.claude/skills/writing-technical-handbooks/.dev
```

- [ ] **Step 2: 驗證部署可用**

Run: `$PY /Users/curtislu/.claude/skills/writing-technical-handbooks/references/scripts/anchor_check.py /Users/curtislu/.claude/skills/writing-technical-handbooks && head -5 /Users/curtislu/.claude/skills/writing-technical-handbooks/SKILL.md`
Expected: anchor 通過；frontmatter 正常。（skill 列表更新需新 session；本步只驗檔案就緒。）

- [ ] **Step 3: 更新 memory**

把 `project_handbook_writing_skill.md` 從「待開發藍本」改為「已實作」，記錄 skill 路徑、tracked 源在 `feat/handbook-writing-skill`、deploy 機制；MEMORY.md 指標同步。

- [ ] **Step 4: Commit（skill 源 + plan 完成標記）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/handbook-skill
git add docs/superpowers/
git commit -m "chore(skill): 完成 writing-technical-handbooks 並 deploy 到 ~/.claude/skills"
```

**Acceptance:** `DEPLOY/SKILL.md` 與 references 就緒、`.dev/` 未部署；memory 已更新；下個 session `/writing-technical-handbooks`（手動）可被 Skill 工具找到。

---

## 完成後

依 `superpowers:finishing-a-development-branch`：跑全部腳本測試（`$PY -m pytest $SRC/references/scripts -q`）綠燈、整理 commits、開 PR（只含 `docs/superpowers/` 下的 spec/plan/skill 源；`~/.claude/skills/` 為部署目標不在 PR）。HTML 轉檔等手冊下游不在本 plan。
