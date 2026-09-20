# Issue tracker: GitHub

Issues and PRDs for this repo live as GitHub issues (`curtis-lu/recsys-demo`). Use the `gh` CLI for all operations.

## Conventions

- **Create an issue**: `gh issue create --title "..." --body "..."`. Use a heredoc for multi-line bodies.
- **Read an issue**: `gh issue view <number> --comments`, filtering comments by `jq` and also fetching labels.
- **List issues**: `gh issue list --state open --json number,title,body,labels,comments --jq '[.[] | {number, title, body, labels: [.labels[].name], comments: [.comments[].body]}]'` with appropriate `--label` and `--state` filters.
- **Comment on an issue**: `gh issue comment <number> --body "..."`
- **Apply / remove labels**: `gh issue edit <number> --add-label "..."` / `--remove-label "..."`
- **Close**: `gh issue close <number> --comment "..."`

Infer the repo from `git remote -v` — `gh` does this automatically when run inside a clone.

> 本 repo 補充：工作流是 PR-driven（功能開發在 `.worktrees/<name>` + `feat/` branch，見 `CLAUDE.md`）。
> Issue 用得不密集，目前主要記錄跨 session 的待辦與已知落差（例：#63 外部 inference 特徵欄落差）。
> 開 issue 前先掃一眼既有的，避免重複開同一件事。

## 票的範圍宣告（本 repo 規則）

**只有一條規則：Out of Scope 的每一項都要附理由。**

只寫「本次不做 X」不算，要寫「因為 Y，所以本次不做 X」。理由的作用不是說服讀者，是讓日後的人能自己判斷「Y 現在還成立嗎」。判準：一個沒參與這輪討論的人，讀完能不能自行決定要不要重開這件事。

這條是 issue #123 的事後修補。那張票的 Out of Scope 有數項只是把結論換句話說、沒給理由，而其中一項（「train／val／calibration 分支的增量化」）排除掉的**正好是同一張票的 Problem Statement 已經指認為浪費的東西**——票面沒有任何一處解釋這個前後不一致。票關掉之後它就沉沒：沒有人判斷得出它是刻意排除還是漏掉，而 runbook 讀起來像是已經涵蓋，落差過了半年才被發現。

**關票時不必把 Out of Scope 搬去任何地方。** 理由寫好、留在票面上就夠了——關掉的票 `gh issue view <n>` 讀得到。

這裡曾經有第二條規則，要求「仍未解決的項目搬進 `docs/agents/deliberate-non-goals.md`」（那份「刻意不做的事」地雷圖）。2026-09-20 **那份檔整個刪除，這條規則跟著刪**。經過：

- 規則的前提是假的。它寫著「票一關，它的 Out of Scope 就不會再被任何人讀到」——而 #123 的落差正是半年後讀那張關掉的票查出來的。
- 它引用的事故也不支持它。#123 沉沒的原因是理由寫得太薄，上面那條規則就是為此訂的；搬不搬進地雷圖跟它沉不沉沒無關。
- 「仍未解決」混了「刻意不做」與「還沒決定」兩種東西，而地雷圖檔頭明寫不收後者。照字面執行連兩次導向錯的動作：2026-09-14（#339）五項全寫成「別…」條目被退回；2026-09-20（#411）改成開 follow-up issue，兩張票查證後一張第一步就開不了工、一張 premise 是錯的，當天關掉。
- 地雷圖本身也刪了。24 條裡 14 條的內容已經寫在 code 註解、ADR 或文件裡（有六條自己就寫著「權威版本在別處，別讀這裡」），其餘的使用者判定「現在自己也看不懂，代表當初決策站不住腳」，不保留、不搬移。

**現在的作法**：一個決定值不值得寫下來，看它**寫在哪裡不會腐爛**——判準屬於某段 code 就寫進那段的 docstring，屬於某個決策就寫進 ADR。**沒有一份專門收「刻意不做」的檔案**，也不要再造一份。「以後也許要做」預設不記，等真的遇到再說。

## Pull requests as a triage surface

**PRs as a request surface: no.** _(Set to `yes` if this repo treats external PRs as feature requests; `/triage` reads this flag.)_

When set to `yes`, PRs run through the same labels and states as issues, using the `gh pr` equivalents:

- **Read a PR**: `gh pr view <number> --comments` and `gh pr diff <number>` for the diff.
- **List external PRs for triage**: `gh pr list --state open --json number,title,body,labels,author,authorAssociation,comments` then keep only `authorAssociation` of `CONTRIBUTOR`, `FIRST_TIME_CONTRIBUTOR`, or `NONE` (drop `OWNER`/`MEMBER`/`COLLABORATOR`).
- **Comment / label / close**: `gh pr comment`, `gh pr edit --add-label`/`--remove-label`, `gh pr close`.

GitHub shares one number space across issues and PRs, so a bare `#42` may be either — resolve with `gh pr view 42` and fall back to `gh issue view 42`.

## When a skill says "publish to the issue tracker"

Create a GitHub issue.

## When a skill says "fetch the relevant ticket"

Run `gh issue view <number> --comments`.

## Wayfinding operations

Used by `/wayfinder`. The **map** is a single issue with **child** issues as tickets.

- **Map**: a single issue labelled `wayfinder:map`, holding the Notes / Decisions-so-far / Fog body. `gh issue create --label wayfinder:map`.
- **Child ticket**: an issue linked to the map as a GitHub sub-issue (`gh api` on the sub-issues endpoint). Where sub-issues aren't enabled, add the child to a task list in the map body and put `Part of #<map>` at the top of the child body. Labels: `wayfinder:<type>` (`research`/`prototype`/`grilling`/`task`). Once claimed, the ticket is assigned to the driving dev.
- **Blocking**: GitHub's **native issue dependencies** — the canonical, UI-visible representation. Add an edge with `gh api --method POST repos/<owner>/<repo>/issues/<child>/dependencies/blocked_by -F issue_id=<blocker-db-id>`, where `<blocker-db-id>` is the blocker's numeric **database id** (`gh api repos/<owner>/<repo>/issues/<n> --jq .id`, _not_ the `#number` or `node_id`). GitHub reports `issue_dependencies_summary.blocked_by` (open blockers only — the live gate). Where dependencies aren't available, fall back to a `Blocked by: #<n>, #<n>` line at the top of the child body. A ticket is unblocked when every blocker is closed.
- **Frontier query**: list the map's open children (`gh issue list --state open`, scoped to the map's sub-issues / task list), drop any with an open blocker (`issue_dependencies_summary.blocked_by > 0`, or an open issue in the `Blocked by` line) or an assignee; first in map order wins.
- **Claim**: `gh issue edit <n> --add-assignee @me` — the session's first write.
- **Resolve**: `gh issue comment <n> --body "<answer>"`, then `gh issue close <n>`, then append a context pointer (gist + link) to the map's Decisions-so-far.
