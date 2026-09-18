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
> 更新於 2026-09-18（證據：`gh label list`）：`needs-triage`、`ready-for-agent` 已建立；`needs-info`、`ready-for-human` 仍未建立。哪些已建立會再變，要用時以 `gh label list` 為準。
> 既有的 `bug` / `enhancement` / `documentation` 屬於分類標籤，與這五個「流程狀態」標籤正交，不需要對映。

本 repo 多一個不在上表的狀態標籤（2026-09-18 起）：

- **`pending`**：規格已想過，但在等**框架外的條件**（例如部署環境還沒有某種資料）。和 `needs-info` 不同：`needs-info` 等的是回報者補資訊。票面要寫明在等什麼、到了之後怎麼辦（通常換成 `needs-triage` 重新評估）。`/triage` 不要把它當成待處理。
