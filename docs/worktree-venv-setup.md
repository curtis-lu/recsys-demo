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
