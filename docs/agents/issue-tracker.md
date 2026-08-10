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

這兩條是 issue #123 的事後修補。那張票的 Out of Scope 有數項只是把結論換句話說、沒給理由，而其中一項（「train／val／calibration 分支的增量化」）排除掉的**正好是同一張票的 Problem Statement 已經指認為浪費的東西**——票面沒有任何一處解釋這個前後不一致。票關掉之後它就沉沒：沒有人判斷得出它是刻意排除還是漏掉，而 runbook 讀起來像是已經涵蓋，落差過了半年才被發現。

- **Out of Scope 的每一項都要附理由。** 只寫「本次不做 X」不算，要寫「因為 Y，所以本次不做 X」。理由的作用不是說服讀者，是讓日後的人能自己判斷「Y 現在還成立嗎」。判準：一個沒參與這輪討論的人，讀完能不能自行決定要不要重開這件事。
- **關票時，Out of Scope 中仍未解決的項目要搬進 `docs/agents/deliberate-non-goals.md`。** 票一關，它的 Out of Scope 就不會再被任何人讀到；那份地雷圖才是 repo 指定收「看起來該修、但刻意不修」的地方（見 `CLAUDE.md` 路由表）。沒搬過去的排除項會從「刻意不做」退化成「無人知道」。

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
