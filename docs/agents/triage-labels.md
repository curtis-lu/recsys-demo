# Triage Labels

The skills speak in terms of five canonical triage roles. This file maps those roles to the actual label strings used in this repo's issue tracker.

| Label in mattpocock/skills | Label in our tracker | Meaning                                  |
| -------------------------- | -------------------- | ---------------------------------------- |
| `needs-triage`             | `needs-triage`       | Maintainer needs to evaluate this issue  |
| `needs-info`               | `needs-info`         | Waiting on reporter for more information |
| `ready-for-agent`          | `ready-for-agent`    | Fully specified, ready for an AFK agent  |
| `ready-for-human`          | `ready-for-human`    | Requires human implementation            |
| `wontfix`                  | `wontfix`            | Will not be actioned                     |

When a skill mentions a role (e.g. "apply the AFK-ready triage label"), use the corresponding label string from this table.

Edit the right-hand column to match whatever vocabulary you actually use.

> 本 repo 現況（2026-07-31 建立時查證）：GitHub 上只有預設九個 label，其中 **`wontfix` 已存在**、會被直接沿用；
> 其餘四個（`needs-triage` / `needs-info` / `ready-for-agent` / `ready-for-human`）尚未建立，`/triage` 首次執行時才會建。
> 既有的 `bug` / `enhancement` / `documentation` 屬於分類標籤，與這五個「流程狀態」標籤正交，不需要對映。
