# Worktree + virtualenv — operating SOP

Why this exists: env-path mistakes recur when working across `.worktrees/`
(wrong interpreter, wrong `src`, broken/looped `.venv`, `.venv` accidentally
committed). This is the single rule set. Follow it; don't improvise.

## Model: one real venv, worktrees symlink to it

- **Exactly one real virtualenv** lives at the main repo root:
  `/Users/curtislu/projects/recsys_tfb/.venv` — a real directory, never a
  symlink.
- Built with the **pinned interpreter** (matches `.python-version` = `3.10.9`;
  `pyproject.toml` requires `>=3.10,<3.12`):
  ```bash
  ~/.pyenv/versions/3.10.9/bin/python -m venv /Users/curtislu/projects/recsys_tfb/.venv
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pip install -e ".[dev]"
  ```
- **Each worktree's `.venv` is a symlink to that one real venv** — never a
  separate per-worktree venv (heavy; drifts):
  ```bash
  ln -s /Users/curtislu/projects/recsys_tfb/.venv <worktree>/.venv
  ```

## `.venv` must never be tracked by git

- `.gitignore` ignores both `.venv` (symlink/file form) and `.venv/` (dir
  form). A self-referential `.venv` symlink once got committed (target = its
  own path → ELOOP "too many levels of symbolic links" on every
  `python`/`pytest` after any checkout/`git worktree add`). Root cause: it was
  `git add`-ed while untracked-but-not-ignored.
- If `git status` / `git ls-files` ever shows `.venv` staged or tracked:
  **stop**, `git rm --cached .venv`, commit. A tracked file overrides
  `.gitignore`; rebuilding the venv locally will not survive the next checkout
  until it is untracked.

## Running tests / CLI inside a worktree

The editable install points at **main's `src`**. So:

- **pytest**: `pyproject.toml` has `pythonpath=["src"]`, so pytest run from a
  worktree prepends *that worktree's* `src` and tests the right code. The
  explicit, unambiguous form (also avoids the relative-symlink ELOOP from
  `.venv/bin/pytest`):
  ```bash
  PYTHONPATH=<abs-worktree>/src \
    /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <paths> -q
  ```
- **CLI** (`python -m recsys_tfb …`): bare invocation picks up **main's
  `src`** (editable-install target), silently running the wrong code. Always:
  ```bash
  PYTHONPATH=<abs-worktree>/src \
    /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb <pipeline> [--options]
  ```
  (CLI form is `python -m recsys_tfb <pipeline> [--options]` — no `run`
  subcommand, no `--pipeline` flag.)

## Cross-worktree git

Bash cwd resets after a skill/`cd`, and a relative path can read the stale
main tree. Always use absolute paths or `git -C <abs-worktree-path> …`.

## Known gotcha: graphify hook blocks (and silently fails) checkout/merge

A graphify post-checkout / post-commit hook regenerates the **tracked** file
`graphify-out/GRAPH_REPORT.md`, leaving it modified. The next `git checkout`
or `git merge --ff-only` then refuses ("local changes would be overwritten")
and leaves HEAD unmoved. If the git commands are chained with
`&&` + `set -e` + `>/dev/null`, `set -e`'s AND-list exception swallows the
failure — the script continues and looks successful while the merge never
happened.

Rules:
- Before any branch switch / ff-merge: `git -C <path> checkout --
  graphify-out/GRAPH_REPORT.md` to discard the auto-gen churn.
- Never `>/dev/null` a chained git checkout/merge; print exit codes and
  verify `git log -1` actually moved.

## Worktree data/ 隔離（重要）

每個 worktree 是**完全自足的沙盒**：所有本機狀態相對 worktree root 解析、**不 symlink 到 main**。
重建後 setup 很快（無 qemu），不需要共用 main 的 artifact。

| 狀態 | 位置（相對 worktree root） |
|---|---|
| Hive warehouse | `data/local_warehouse`（`local_spark_setup.py` 建） |
| 內嵌 Derby metastore | `data/metastore_db` |
| 檔案 artifact | `data/{models,dataset,evaluation,inference}`（pipeline 自動建真目錄） |
| training cache | `data/recsys_cache`（`cache.root` 已相對化） |

首次進 worktree 只需建 venv symlink（見下），**不需要再 symlink data/ 子目錄**；
跑 `local_spark_setup.py` 即重建本機資料。驗證隔離：`local_spark_setup.py --check-isolation`。

> 若**刻意**要拿 main 的真 artifact 測（例如評估 main 訓練好的 model），才針對該子目錄手動 symlink（opt-in）。

## Pre-flight health check (run before testing/running in a worktree)

```bash
readlink <worktree>/.venv          # -> /Users/curtislu/projects/recsys_tfb/.venv
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -V   # -> Python 3.10.9
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -c "import recsys_tfb, pyspark"
```
Any failure → repair before proceeding.

## Repair recipe (broken / looped / missing `.venv`)

1. `git ls-files | grep -x .venv` — if tracked: `git rm --cached .venv` +
   ensure `.gitignore` has `.venv` and `.venv/` + commit (must land on `main`,
   else every branch/worktree keeps inheriting it).
2. `rm -f` the broken `.venv` symlink in the main repo **and every worktree**.
3. Rebuild the one real venv at main root with the pinned interpreter
   (commands above).
4. Re-symlink every worktree's `.venv` to the main real venv.
5. Run the pre-flight health check in each worktree.
