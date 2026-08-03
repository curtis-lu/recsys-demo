# Kedro 官方設計理由蒐證（for 手刻框架對照表）

- 建立日期：2026-08-03
- 目的：蒐集 Kedro **官方**對其架構設計的理由陳述，供「Kedro 官方原則 × 本 repo 手刻版現況」對照表使用。最終產物是給 AI coding agent 遵守的**可機械檢查約束**，因此本文優先收錄「能判定某段程式碼有沒有違反」的句子。
- 蒐證方式：下載 `docs.kedro.org/en/stable` 全站頁面 ＋ GitHub tag `1.5.0` 原始碼 tarball，在本機 grep；另補 0.19.x 舊版文件與官方部落格。所有引文皆為原文擷取，非改寫。

---

## §0 版本基準（先讀，否則下面所有引文的適用範圍會誤判）

| 事實 | 值 | 佐證 |
|---|---|---|
| PyPI 最新版 | `1.5.0`（上傳時間 2025-06-29 起算的序列中，`1.5.0` = 2026-06-29T14:53:18） | `https://pypi.org/pypi/kedro/json`（2026-08-03 查閱） |
| `docs.kedro.org/en/stable` 實際指向 | **Kedro 1.5.0**（Read the Docs API 回傳 `{"slug":"stable","ref":"1.5.0","identifier":"ad94ed3e36bfb26c803b9841ca90f0f49ba4e0fa"}`） | `https://app.readthedocs.org/api/v3/projects/kedro/versions/stable/`（2026-08-03 查閱） |
| 1.0.0 發布 | 2025-07-22 | PyPI release 時間 |
| 文件網站結構 | 1.x 已從 Sphinx 改為 MkDocs，URL 路徑全部改過（舊 `nodes_and_pipelines/nodes.html` → 新 `build/nodes/`） | `RELEASE.md`「Revamped the look and feel of the Kedro documentation … with `mkdocs`」（1.0.0 段） |

> **重要**：網路上大量 Kedro 教學引用的是 0.18/0.19 的 URL 與 API。本文所有「官方明說」若無特別標註，皆以 **1.5.0** 為準；有版本差異的地方我會標出來。

### 標籤定義（每條事實都會帶一個）

| 標籤 | 意思 |
|---|---|
| `【官方-文件】` | docs.kedro.org 上的散文（等同 repo `docs/*.md`） |
| `【官方-原始碼】` | kedro 套件的 docstring、錯誤訊息、實際控制流程 |
| `【官方-repo 非文件】` | `RELEASE.md`、`README.md`、GitHub Discussion 中 Kedro 維護者的回覆 |
| `【官方-部落格】` | kedro.org/blog（Kedro team 官方部落格） |
| `【二手】` | 非 Kedro 官方的來源 |
| `【推論】` | 我從官方證據推出來的，官方沒有這樣寫 |
| `【查不到】` | 我找過但沒有官方陳述 |

---

## §1 DataCatalog 為何存在；node 為何不該自己做 I/O

**判定：官方有明文，而且是可判定的。**

### 1.1 最強的一句（可直接當約束）

> `【官方-文件】` 「We do not recommend that you load and manipulate a data catalog directly in a Kedro node. Nodes are designed to be pure functions and thus should remain agnostic of I/O.」
>
> 出處：`docs/configure/configuration_basics.md:250`
> URL：<https://docs.kedro.org/en/stable/configure/configuration_basics/>（2026-08-03 查閱）

這句同時給了「不該做什麼」（在 node 內載入／操作 catalog）與「為什麼」（node 被設計成 pure function，因此應對 I/O 無感）。

### 1.2 DataCatalog 的職責宣告

> `【官方-文件】` 「The above definition of pipelines applies to non-stateful or "pure" pipelines that do not interact with the outside world. In practice, we would like to interact with APIs, databases, files, and other sources of data. By combining IO and pipelines, we can tackle these more complex use cases.
> By using `DataCatalog` from the IO module we are still able to write pure functions that work with our data and outsource file saving and loading to `DataCatalog`.
> Through `DataCatalog`, we can control where inputs are loaded from, where intermediate variables get persisted and ultimately the location to which output variables are written.」
>
> 出處：`docs/build/run_a_pipeline.md:226–230`
> URL：<https://docs.kedro.org/en/stable/build/run_a_pipeline/>（2026-08-03 查閱）

這段是官方對「為什麼要有一層 catalog」最完整的因果敘述：**pipeline 的抽象定義本身是純的、不碰外界；I/O 被外包（outsource）到 DataCatalog，於是「輸入從哪讀、中間變數存不存、輸出寫到哪」變成可控制的一個維度。**

> `【官方-原始碼】` `DataCatalog` 類別 docstring：「A centralized registry for managing datasets in a Kedro project. The `DataCatalog` provides a unified interface for loading and saving datasets, enabling seamless interaction with various data sources and formats. … This class is the core component of Kedro's data management system, allowing datasets to be defined, accessed, and manipulated in a consistent and reusable way.」
>
> 出處：`kedro/io/data_catalog.py:161–172`（tag 1.5.0）

> `【官方-文件】` glossary：「The Data Catalog is Kedro's registry of all data sources available for use in the data pipeline. It manages loading and saving of data. The Data Catalog maps the names of node inputs and outputs as keys in a Kedro dataset…」
>
> 出處：`docs/getting-started/glossary.md`；URL：<https://docs.kedro.org/en/stable/getting-started/glossary/>

### 1.3 程式化存取 catalog 的邊界（很少人引用，但這條最可判定）

> `【官方-文件】` 「This pattern is not recommended unless you are using a hosted notebook environment such as SageMaker or Databricks. The pattern is also acceptable when writing unit or integration tests for your Kedro pipeline. Use the YAML approach in preference.」
>
> 出處：`docs/catalog-data/advanced_data_catalog_usage.md:169`（"How to save data programmatically" 段的 warning）
> URL：<https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/>（2026-08-03 查閱）

換句話說：官方承認「在 code 裡直接 `catalog.save(...)`」是存在的能力，但**只在三種情境下可接受：hosted notebook（SageMaker/Databricks）、寫單元／整合測試**；其餘一律優先用 YAML 宣告。

### 1.4 維護者的補充（非文件，但出自官方 repo）

> `【官方-repo 非文件】` Kedro 維護者 `mzjp2` 於 2021-08-19 在官方 repo Discussion #861「Pass the catalog as a node argument」回覆：「Indeed, you aren't expected to use the catalog within the node - rather, you should specify which entries of the catalog you want available inside your node as python objects and Kedro's catalog will handle loading them in for you.」
>
> URL：<https://github.com/kedro-org/kedro/discussions/861>（2026-08-03 查閱）
>
> ⚠ 這是 GitHub Discussion 的回覆，**不是文件明文**，且時間為 0.17 時代。不得升格成官方文件陳述；它的價值是佐證 §1.1 那句的意圖。

### 1.5 官方部落格的說法（哲學層，可判定性低）

> `【官方-部落格】` 「Data access is abstracted through a data catalog. It formalizes how data is accessed, versioned, and managed across a project, **preventing file paths and storage logic from being scattered throughout the codebase**. This separation of concerns simplifies environment changes, for example, local to cloud, improves reproducibility, and creates a consistent contract between data and logic.」
>
> 出處：Kedro 官方部落格〈Kedro in the modern data and AI tooling landscape〉
> URL：<https://kedro.org/blog/kedro-in-the-data-and-ai-landscape>（2026-08-03 查閱）

粗體那半句其實**是可判定的**：「檔案路徑與儲存邏輯不得散落在 codebase 各處」可以用 grep 檢查（見 §8 C-3）。

---

## §2 Node 的純函式契約：明文要求 vs. 明文不要求

### 2.1 明文要求（散文）

> `【官方-文件】` 「Kedro expects node functions to be **pure functions**; a pure function is one whose output follows solely from its inputs, without any observable side effects. Testing these functions checks that a node will behave as expected — for a given set of input values, a node will produce the expected output.」
>
> 出處：`docs/tutorials/test_a_project.md:20`
> URL：<https://docs.kedro.org/en/stable/tutorials/test_a_project/>（2026-08-03 查閱）

> `【官方-文件】` glossary 的 Node 條目：「A Kedro node is a wrapper for a pure Python function that names the inputs and outputs of that function. (A pure function is a one whose output value follows solely from its input values, **without any observable side effects such as changes to state or mutable data**).」
>
> 出處：`docs/getting-started/glossary.md:49–51`

> `【官方-文件】` 「A node should behave consistently, repeatably, and predictably, so that a given input to a node always returns the same output. For those in the know, this is the definition of a pure function. **Nodes/pure functions should be small single responsibility functions that perform a single specific task.**」
>
> 出處：`docs/integrations-and-plugins/notebooks_and_ipython/notebook-example/add_kedro_to_a_notebook.md`
> URL：<https://docs.kedro.org/en/stable/integrations-and-plugins/notebooks_and_ipython/notebook-example/add_kedro_to_a_notebook/>（2026-08-03 查閱）

### 2.2 明文要求（框架真的會 raise 的，這些才是硬契約）

以下全部來自 `【官方-原始碼】`（tag 1.5.0），是 `Node.__init__` / `Pipeline.__init__` 實際執行的驗證，**違反就是 exception，不是建議**：

| 規則 | 錯誤訊息 | 位置 |
|---|---|---|
| node 至少要有一個 input 或一個 output | `Invalid Node definition: it must have some 'inputs' or 'outputs'.` | `kedro/pipeline/node.py:152` |
| node 名稱字元集 | `'{name}' is not a valid node name. It must contain only letters, digits, hyphens, underscores and/or fullstops.` | `kedro/pipeline/node.py:165` |
| tag 名稱字元集（同上） | `'{tag}' is not a valid node tag. It must contain only …` | `kedro/pipeline/node.py:174` |
| 同一 node 的 input 與 output 不得同名（即使 transcode） | `A node cannot have the same inputs and outputs even if they are transcoded: …` | `kedro/pipeline/node.py:719` |
| dataset 名稱不該含 `.`（`.` 保留給 namespace） | UserWarning：`One or more dataset names contain '.' characters, which is not recommended as the dot notation is reserved for automatic namespacing in Kedro.` | `kedro/pipeline/node.py:748–775` |
| pipeline 不得有循環相依 | `Circular dependencies exist among these items: …` | `docs/build/pipeline_introduction.md:220` 有對應散文 |
| transcoded dataset 不得同時用帶／不帶分隔符的名字引用 | `The following datasets are used with transcoding, but were referenced without the separator: …` | `kedro/pipeline/pipeline.py:1419–1441`（docstring 原文：「Users should not be allowed to refer to a transcoded dataset both with and without the separator.」） |
| 存 `None` 到 dataset 是被禁止的 | `Saving 'None' to a 'Dataset' is not allowed` | `kedro/io/core.py:303` |

散文版的循環相依理由：
> `【官方-文件】` 「For every two variables where the first depends on the second, the second must not also depend on the first. Otherwise, a circular dependency will prevent us from compiling the pipeline.」
> 出處：`docs/build/pipeline_introduction.md:220`（章節標題就叫 **How to avoid creating bad pipelines**，`:194`）

### 2.3 明文**不**要求 / 官方允許的 side effect（邊界在哪）

這一節是本次蒐證最有價值的部分——官方一邊說 pure，一邊留了明確的開口：

1. **框架完全沒有檢查純度。** `【官方-原始碼】+【推論】` `Node.__init__` 與 `Node.run` 只驗參數個數、名稱、型別，沒有任何 purity/determinism 檢查（`kedro/pipeline/node.py`）。所以「pure function」在 Kedro 是**慣例與設計意圖**，不是執行期契約。
2. **`confirms` 是官方認可的顯式 side effect。** `【官方-原始碼】` Node 的 `confirms` 參數 docstring：「Optional name or the list of the names of the datasets that should be confirmed. This will result in calling `confirm()` method of the corresponding dataset instance. **Specified dataset names do not necessarily need to be present in the node `inputs` or `outputs`.**」（`kedro/pipeline/node.py:84–88`）。也就是說：node 完成後對某個 dataset 觸發狀態變更（例如 `IncrementalDataset` 推進 checkpoint）是被支援的，但**必須宣告在 node 定義上，不能藏在函式體裡**。
3. **generator（`yield`）node 是官方支援的。** `【官方-文件】` 「Generator functions … are often used for lazy-loading or lazy-saving of data … In the context of Kedro, generator functions can be used in nodes to efficiently process and handle such large datasets.」（`docs/build/nodes.md:200`）。這代表 node 函式**不必**是一次性回傳、不必是無狀態的迭代。
4. **node 的輸入被防禦性複製，所以「就地修改輸入」多數時候不會污染上游。** `【官方-原始碼】` `MemoryDataset.load()` / `.save()` 都會 `_copy_with_mode`（`kedro/io/memory_dataset.py:64–70`）；`_infer_copy_mode` 對 pandas DataFrame / numpy ndarray 用 `"copy"`、對 Spark 之類的 `DataFrame` 與 ibis Table 用 `"assign"`、其餘用 `"deepcopy"`（`kedro/io/memory_dataset.py:86–113`）。
   → `【推論】` **`"assign"` 那一條是漏洞**：Spark DataFrame 走 assign，沒有複製。對本 repo（PySpark）而言，「node 不得就地修改輸入」這條在 Kedro 原生語意下也不是自動安全的。
5. **hook 也被防禦性複製。** `【官方-原始碼】` `inputs = inputs.copy()  # shallow copy to prevent in-place modification by the hook`（`kedro/runner/task.py:269–271`）。

### 2.4 明文不要求（沒有寫的事）

- `【查不到】` 官方沒有任何一句話說「node 函式不得寫檔／不得呼叫 API／不得 log」。最接近的只有 §1.1 的「不要在 node 內載入與操作 data catalog」。
- `【查不到】` 官方沒有說「node 函式不得 import 框架內部模組」。
- `【查不到】` 官方沒有給 node 函式的行數／複雜度上限，只有「small single responsibility functions」（§2.1，不可機械判定）。

---

## §3 Pipeline slicing 的設計意圖

### 3.1 官方講的動機

> `【官方-文件】` 「Sometimes it is desirable to run a subset, or a 'slice' of a pipeline's nodes.」
> 出處：`docs/build/slice_a_pipeline.md:3`
> URL：<https://docs.kedro.org/en/stable/build/slice_a_pipeline/>（2026-08-03 查閱）

> `【官方-文件】` 「Kedro can automatically generate a sliced pipeline from existing node outputs. This can be helpful if you want to **avoid re-running nodes that take a long time**」
> 出處：`docs/build/slice_a_pipeline.md:247`

> `【官方-文件】` slicing 的硬前提：「All the inputs required by the specified nodes must exist, that is, already produced or present in the data catalog.」
> 出處：`docs/build/slice_a_pipeline.md:242`

老實說：**官方對 slicing 的「為什麼」講得很淺**——只有「有時候你會想跑子集」與「避免重跑很慢的節點」兩句。沒有 RFC 級的設計論述。

### 3.2 API 名稱（1.5.0 實測，勿憑印象）

`【官方-原始碼】` `kedro/pipeline/pipeline.py` 的切片方法：

| 方法 | 行號 | docstring 語意 |
|---|---|---|
| `only_nodes(*node_names)` | 621 | 只留指定 node |
| `only_nodes_with_namespaces(node_namespaces)` | 660 | 依 namespace |
| `only_nodes_with_inputs(*inputs)` | 760 | 直接吃這些 input 的 node |
| `from_inputs(*inputs)` | 786 | 「nodes which depend **directly or transitively on** the provided inputs」 |
| `only_nodes_with_outputs(*outputs)` | 826 | 直接產出這些 output 的 node |
| `to_outputs(*outputs)` | 851 | 產出這些 output 所**需要**的 node |
| `from_nodes(*node_names)` | 890 | 「nodes which depend directly or transitively on the provided nodes」 |
| `to_nodes(*node_names)` | 911 | 「nodes required directly or transitively **by** the provided nodes」 |
| `only_nodes_with_tags(*tags)` | 932 | **any**（OR）語意；不給 tag 則結果為空 pipeline |
| `filter(...)` | 949 | 見下 |

`filter()` 的合成語意是關鍵設計決策，且官方明文寫出來了：
> `【官方-原始碼】` 「The new pipeline object is **the intersection** of pipelines that meet each filtering condition. **This is distinct from chaining multiple filters together.**」
> 出處：`kedro/pipeline/pipeline.py:956–959`

`filter()` 的參數名（就是 CLI flag 對應的內部名）：`tags`, `from_nodes`, `to_nodes`, `node_names`, `from_inputs`, `to_outputs`, `node_namespaces`。

### 3.3 CLI flag（1.x 名稱，與 0.18 不同）

`【官方-原始碼】` `kedro/framework/cli/project.py`：`--from-inputs`（:157）、`--to-outputs`（:164）、`--from-nodes`（:171）、`--to-nodes`（:178）、`--nodes` / `-n`（:181，內部變數 `node_names`）、`--tags` / `-t`（:193）、`--only-missing-outputs`（:240）。

help 原文：
- `FROM_NODES_HELP = """A list of node names which should be used as a starting point."""`（:43）
- `NODE_ARG_HELP = """Run only nodes with specified names."""`（:45）
- `ONLY_MISSING_OUTPUTS_HELP = """Run only nodes with missing outputs. If all outputs of a node exist and are persisted, skip the node execution."""`（:72–73）

> ⚠ `【官方-repo 非文件】` **版本差異**：0.19.0 移除了 `kedro run` 的 `--node`、`--tag`、`--load-version`，改為 `--nodes`、`--tags`、`--load-versions`（`RELEASE.md`，0.19.0 段的 CLI 小節）。

### 3.4 「只跑缺漏輸出」的精確語意（很可判定）

> `【官方-原始碼】` `_should_node_run` docstring：
> 「Check if a node should run based on following rules:
> 1. **Always run nodes with no outputs**
> 2. Run if node has **missing persistent** outputs
> 3. Run if node's outputs are needed by children that will run」
> 出處：`kedro/runner/runner.py:608–633`

注意第 2 條的 *persistent*：純 `MemoryDataset` 輸出永遠算「不存在」。第 1 條表示「無輸出的 node（純副作用節點）永遠會被執行」——這是官方對「side-effect-only node」存在的默認。

### 3.5 一個必須警告的文件錯誤

> ⚠ `【官方-文件】` 與 `【官方-原始碼】` **互相矛盾**：`docs/build/slice_a_pipeline.md:318` 仍然寫「by using the `Runner.run_only_missing` method」並示範 `SequentialRunner().run_only_missing(full_pipeline, io)`；但 1.5.0 的 `kedro/` 套件裡 **`run_only_missing` 已不存在**（`grep -rn "def run_only_missing" kedro/` 零命中）。
>
> 解法出處：`【官方-repo 非文件】` `RELEASE.md:215`（1.0.0 Breaking changes）：「Removed the `AbstractRunner.run_only_missing()` method, an older and underused API for partial runs. Please use `--only-missing-outputs` CLI instead.」
>
> → **stable 文件有過時 API 範例。任何從 docs.kedro.org 抄來的 API 名稱都要回原始碼驗一次。**

---

## §4 ConfigLoader / OmegaConfigLoader、環境分層、parameters 外部化

### 4.1 版本沿革（先釘住，避免引用已死的類別）

`【官方-repo 非文件】` 依 `RELEASE.md`：

| 事件 | 版本 | 佐證行 |
|---|---|---|
| 新增 `OmegaConfigLoader`（「uses `OmegaConf` for loading and merging configuration」） | 0.18.5 | `RELEASE.md:887`（0.18.5 段標題在 :882） |
| 加入 `ConfigLoader` / `TemplatedConfigLoader` 的遷移指南並標記 deprecated | 0.18.x | `RELEASE.md:713` |
| **`OmegaConfigLoader` 成為預設**；**`ConfigLoader` 與 `TemplatedConfigLoader` 移除** | **0.19.0** | `RELEASE.md:621–622`（0.19.0 段標題在 :572） |

文件側同一事實：
> `【官方-文件】` 「`ConfigLoader` and `TemplatedConfigLoader` have been removed in Kedro `0.19.0`. Refer to the migration guide for config loaders…」
> 出處：`docs/configure/configuration_basics.md`（頁首 Note）；URL：<https://docs.kedro.org/en/stable/configure/configuration_basics/>

### 4.2 base vs local 的職責（明文）

> `【官方-文件】` 「In Kedro, the base configuration environment refers to the default configuration settings that are used as the foundation for all other configuration environments. The `base` folder contains the default settings that are used across your pipelines, unless they are overridden by a specific environment.」
> 「**Warning: Do not put private access credentials in the base configuration folder or any other configuration environment folder that is stored in version control.**」
> 出處：`docs/configure/configuration_basics.md:56`

> `【官方-文件】` 「The `local` configuration environment folder should be used for configuration that is either **user-specific** (for example, IDE configuration) or **protected** (for example, security keys).」
> 「**Warning: Do not add any local configuration to version control.**」
> 出處：`docs/configure/configuration_basics.md:62`

> `【官方-文件】` 「Use the `local` subfolder for **settings that should not be shared**, such as access credentials, custom editor configuration, personal IDE configuration and other sensitive or personal content.」
> 出處：`docs/getting-started/kedro_concepts.md:130`；URL：<https://docs.kedro.org/en/stable/getting-started/kedro_concepts/>

> `【官方-文件】` 「For security reasons, we strongly recommend that you *do not* commit any credentials or other secrets to version control. Kedro is set up so that, by default, if a file inside the `conf` folder (and its subdirectories) contains `credentials` in its name, it is ignored by git.」
> 出處：`docs/configure/credentials.md:3`；URL：<https://docs.kedro.org/en/stable/configure/credentials/>

**專案樣板的實際 `.gitignore`**（`【官方-原始碼】`，這是最可機械檢查的版本）：
```
# ignore all local configuration
conf/local/**
!conf/local/.gitkeep
.telemetry

# ignore potentially sensitive credentials files
conf/**/*credentials*

# ignore everything in the following folders
data/**
```
出處：`kedro/templates/project/{{ cookiecutter.repo_name }}/.gitignore:3–13`（tag 1.5.0）

> `【官方-文件】` 教學版三條：「Do not commit data to version control. / Do not commit notebook output cells (data can sneak into notebooks when you do not delete output cells). / Do not commit credentials in `conf/`. Keep the sensitive information in the `conf/local/` folder.」
> 出處：`docs/tutorials/tutorial_template.md:49–51`（章節標題：**Configuration best practice to avoid leaking confidential data**）

### 4.3 合併規則（純機械，可寫成測試）

> `【官方-文件】` 出處：`docs/configure/configuration_basics.md`「Configuration loading」段
> - 同一環境資料夾內（例如 `conf/base/`）**兩個非 parameters 檔案有相同 top-level key** → 抛 `ValueError`（duplicates are not allowed）。
> - 不同環境資料夾（`conf/base/` vs `conf/local/`）有相同 top-level key → **後載入的（`conf/local/`）勝出**；不報錯，只發一則 `DEBUG` log。
> - 兩個 **parameters** 檔案有相同 top-level key → 會往下檢查 sub-keys，有重複才抛 `ValueError`。
> - top-level key 以 `_` 開頭者視為 hidden/reserved，會被忽略（不觸發重複錯、也不出現在結果 dict），可用於 YAML anchors 或 catalog templating。

檔名比對規則（同頁「Configuration file names」）：檔名以 `catalog` 開頭 **或** 位於名稱以 `catalog` 開頭的子目錄中，**且**副檔名為 `yaml`/`yml`/`json`。預設 `config_patterns` 原文：
```python
config_patterns = {
    "catalog": ["catalog*", "catalog*/**", "**/catalog*"],
    "parameters": ["parameters*", "parameters*/**", "**/parameters*"],
    "credentials": ["credentials*", "credentials*/**", "**/credentials*"],
    "logging": ["logging*", "logging*/**", "**/logging*"],
}
```

### 4.4 「什麼該進 config、什麼不該」

這題官方**只有部分明文**。找到的是：

> `【官方-文件】` 「If you have a group of parameters that determine the hyperparameters of your model, **define them in a single location** such as `conf/base/parameters.yml`. Keeping everything together reduces the chances of missing an update elsewhere in the codebase.」
> 「By using parameters, you can make your Kedro pipelines more flexible and easier to configure, since you can change the behaviour of your nodes by modifying the `parameters.yml` file.」
> 出處：`docs/configure/parameters.md:2, 7`；URL：<https://docs.kedro.org/en/stable/configure/parameters/>

**不該進 config 的，官方只講了環境變數這一項**（且講得很硬）：
> `【官方-文件】` 「This is an advanced feature and should be used with caution. **We do not recommend using environment variables for configurations other than credentials.**」（`docs/configure/advanced_configuration.md:304`）
> `【官方-文件】` 「You can use this resolver solely in `credentials.yml`, not in catalog or parameter files. **This restriction discourages using environment variables for anything other than credentials.**」（`docs/configure/advanced_configuration.md:327`）——即 `oc.env` resolver 預設只在載入 credentials 時啟用。

另兩條硬邊界：
> `【官方-文件】` 「`runtime_params` are not designed to override `globals` configuration. This is done **to avoid unexplicit overrides and to simplify parameter resolution**. Thus, `globals` has a single entry point — the `yaml` file.」（`docs/configure/advanced_configuration.md:229`）
> `【官方-原始碼】` 對應的錯誤訊息：`The `runtime_params:` resolver is not supported for globals.`（`kedro/config/omegaconf_config.py:234, 262`）；以及 `Keys starting with '_' are not supported for globals.`（:478）。

> `【官方-文件】` 「Custom configuration loaders that do not subclass `OmegaConfigLoader` will not include OmegaConf-specific functionalities such as interpolation, globals, runtime parameters or custom resolvers. To access these features, your loader must subclass `OmegaConfigLoader`.」（`docs/configure/advanced_configuration.md:47–50`）

- `【查不到】` 官方**沒有**一句「什麼東西不該做成 parameter」的通則（例如「路徑不該進 parameters、該進 catalog」這種常見說法，我找不到官方明文）。
- `【查不到】` 官方**沒有**說 config 不得放程式邏輯／不得放 Python 物件；`OmegaConfigLoader` 只限定副檔名 `.yml`/`.yaml`/`.json`。

---

## §5 Dataset versioning 與可重現性主張

### 5.1 版本化涵蓋什麼（明文）

> `【官方-文件】` 「Kedro enables dataset and ML model versioning through the `versioned` definition.」「In this example, `filepath` is used as the basis of a folder that stores versions of the `cars` dataset. Each time a new version is created by a pipeline run it is stored within `data/01_raw/company/cars.csv/<version>/cars.csv`, where `<version>` corresponds to a version string formatted as `YYYY-MM-DDThh.mm.ss.sssZ`.」
> 「By default, `kedro run` loads the **latest** version of the dataset.」
> 出處：`docs/catalog-data/data_catalog.md`（Dataset versioning 段）；URL：<https://docs.kedro.org/en/stable/catalog-data/data_catalog/>（2026-08-03 查閱）

> `【官方-文件】` 「A dataset offers versioning support if it extends the `kedro.io.AbstractVersionedDataset` class to accept a version keyword argument as part of the constructor. It must also adapt the `_save` and `_load` methods to use the versioned data path obtained from `_get_save_path` and `_get_load_path` respectively.」
> 出處：同上

> `【官方-原始碼】` `Version` namedtuple docstring：「If `Version.load` is None, then the latest available version is loaded. If `Version.save` is None, then save version is formatted as `YYYY-MM-DDThh.mm.ss.sssZ` of the current timestamp.」（`kedro/io/core.py:498–503`）

### 5.2 明文**不**涵蓋 / 明文限制（這節是重點）

| 限制 | 原文 | 出處 |
|---|---|---|
| HTTP(S) 不能版本化 | 「HTTP(S) is a supported file system in the dataset implementations, but **it cannot be combined with versioning**.」；程式端：「Versioning is not supported for HTTP protocols. Please remove the `versioned` flag from the dataset configuration.」 | `docs/catalog-data/data_catalog.md:232`；`kedro/io/core.py:985` |
| `PartitionedDataset` 底層 dataset 不能版本化 | 「`versioned` flag — specifying it will result in a `DatasetError` being raised; versioning cannot be enabled for the underlying datasets」 | `docs/catalog-data/partitioned_and_incremental_datasets.md:98` |
| 版本目錄不得已存在 | `Save path '{versioned_path}' for {self} must not exist if versioning is enabled.` | `kedro/io/core.py:828` |
| version 字串格式受限 | `Version strings must be a single non-empty path component with no path separators ('/' or '\\') and must not be '.' or '..'.` | `kedro/io/core.py:837–839` |
| 對已存在的非版本化檔案開版本化會爆 | 「Cannot save versioned dataset … because a file with the same name already exists in the directory. This is likely because versioning was enabled on a dataset already saved previously.」 | `kedro/io/core.py:915` |
| **不建議釘死 load/save 版本** | 「We do not recommend passing exact load or save versions, since it might lead to inconsistencies between operations.」；warning 原文：「Save version '{}' did not match load version '{}' for {}. This is **strongly discouraged** due to inconsistencies it may cause between 'save' and 'load' operations. Please refrain from setting exact load version for intermediate datasets where possible…」 | `docs/catalog-data/advanced_data_catalog_usage.md:301`；`kedro/io/core.py:508–513` |

### 5.3 官方對可重現性的主張，以及主張的**空缺**

官方的 reproducibility 陳述全部停在定位層級：
> `【官方-文件】` 「Kedro is an open-source Python framework for creating **reproducible**, maintainable, and modular data engineering and data science code. It applies software engineering best-practices to machine learning code, including modularity, separation of concerns and versioning.」（`docs/getting-started/glossary.md:15`）
> `【官方-repo 非文件】` README〈Why does Kedro exist?〉：「To address the main shortcomings of Jupyter notebooks, one-off scripts, and glue-code because there is a focus on creating **maintainable data engineering and data science code**；To enhance **team collaboration**…；To increase efficiency, because applied concepts like modularity and separation of concerns inspire the creation of **reusable analytics code**」（`README.md:80–88`，tag 1.5.0）
> `【官方-部落格】` 「Kedro is a strong fit when a project … **demands reproducibility over time**. It is designed for pipelines that are expected to evolve and persist over months or years, rather than days.」（<https://kedro.org/blog/kedro-in-the-data-and-ai-landscape>）

**`【查不到】`**：我找不到任何官方句子明說「dataset versioning **不**保證什麼」——例如「不涵蓋程式碼版本／環境／相依套件版本」這種話。官方文件沒有這一段。

但可以從原始碼給出**事實層面**的邊界（標為 `【官方-原始碼】+【推論】`）：`KedroSession.run()` 記錄的 `record_data`（就是傳給 `before_pipeline_run` hook 的 `run_params`）欄位只有：
```
run_id, project_path, env, kedro_version, tags, from_nodes, to_nodes,
node_names, from_inputs, to_outputs, load_versions, runtime_params,
pipeline_names, namespaces, runner, only_missing_outputs
```
出處：`kedro/framework/session/session.py:382–399`（tag 1.5.0）。
→ **沒有 git sha、沒有程式碼 hash、沒有相依套件版本清單。** 因此「Kedro 版本化 = 資料檔案的時間戳目錄 + 執行參數紀錄」，程式碼與環境的重現性靠使用者自己（官方把它推給 DVC / MLflow 整合文件）。歷史上舊版曾有 Journal 機制記過 `git_sha`，1.5.0 已無（`grep -rn "git_sha" kedro/ docs/` 零命中）。

---

## §6 Hooks 存在的理由與適用邊界

### 6.1 存在理由（明文，但**只在 0.19.x 文件裡**）

> `【官方-文件，Kedro 0.19.14】` 「Hooks are a mechanism to add extra behaviour to Kedro's main execution in an easy and consistent manner. Some examples might include:
> - Adding a log statement after the data catalog is loaded.
> - Adding data validation to the inputs before a node runs, and to the outputs after a node has run. This makes it possible to integrate with other tools like Great-Expectations.
> - Adding machine learning metrics tracking, e.g. using MLflow, throughout a pipeline run.」
>
> URL：<https://docs.kedro.org/en/0.19.14/hooks/index.html>（2026-08-03 查閱）
>
> ⚠ **版本差異**：1.x 的文件重構把這個 hooks 章節首頁拿掉了（1.5.0 只有 `extend/hooks/{introduction,common_use_cases,examples}.md`，`mkdocs.yml:384–387` 可驗），因此**在 1.5.0 的文件裡找不到這段「為什麼有 hooks」的陳述**。要引用得標明來自 0.19.x。

1.5.0 只剩一句較弱的：
> `【官方-文件】` 「Kedro defines Hook specifications for particular execution points where users can **inject additional behaviour**.」（`docs/extend/hooks/introduction.md:9`）

官方列出的 12 個 hook spec（`docs/extend/hooks/introduction.md:9–33`）：`after_context_created`、`after_catalog_created`、`before_pipeline_run`、`before_dataset_loaded`、`after_dataset_loaded`、`before_node_run`、`after_node_run`、`before_dataset_saved`、`after_dataset_saved`、`after_pipeline_run`、`on_node_error`、`on_pipeline_error`。命名規約也是明文：非錯誤類為 `<before/after>_<noun>_<past_participle>`，錯誤類為 `on_<noun>_error`。

### 6.2 官方列的「該用 hook 做什麼」

`【官方-文件】` `docs/extend/hooks/common_use_cases.md` 的章節標題本身就是清單：
- Use Hooks to extend a node's behaviour（:3）
- Use Hooks to customise the dataset load and save methods（:94）
- Use Hooks to load external credentials（:135）
- Use stateful Hooks to share context between Hook methods（:203）
- Use Hooks to read `metadata` from `DataCatalog`（:235）
- Use Hooks to debug your pipeline（:249）

`docs/extend/hooks/examples.md`：memory consumption tracking（:3）、data validation（:75）、observability（:235）、metrics tracking（:285）、以 `before_node_run` 改 node inputs（:352）。

### 6.3 「不該怎麼用」（可判定的）

| 規則 | 原文 | 出處 |
|---|---|---|
| hook 實作**不得**給參數預設值 | 「**Do not use default argument values in hook implementations.** Hook parameters must be defined *without default values*. Due to how `pluggy` (the underlying plugin system) passes arguments, parameters with defaults will receive the default value instead of the actual value passed by Kedro.」 | `docs/extend/hooks/introduction.md:125` |
| **不得依賴 hook 執行順序** | 「In general, Hook execution order is not guaranteed and **you should not rely on it**. If you need to make sure a particular Hook is executed first or last, you can use the `tryfirst` or `trylast` argument for `hook_impl`.」（註冊順序為 LIFO：`HOOKS = (hook_a, hook_b,)` 時 `hook_b` 先執行；plugin hooks 依字母序） | `docs/extend/hooks/introduction.md:180–185` |
| hook 實作名稱必須與 spec 同名，參數是 spec 的子集 | 「The Hook implementation should have the same name as the specification. The Hook must provide a concrete implementation with a subset of the corresponding specification's parameters (you do not need to use them all).」 | `docs/extend/hooks/introduction.md:80` |
| stateful hook 存的資料要小且視為唯讀 | 「Keep stored data small and treat it as `read-only` **to avoid surprising side effects across hooks**.」 | `docs/extend/hooks/common_use_cases.md:233` |
| **ParallelRunner 下 dataset/node hook 不會跑** | 「Some hooks will not execute when using `ParallelRunner`. Specifically, `catalog`, `context`, and `pipeline` hooks that run in the main process will execute, but **`dataset` and `node` hooks do not run in the worker processes** that run nodes in parallel. Use `SequentialRunner` or `ThreadRunner` if your project relies on these hooks.」 | `docs/extend/hooks/introduction.md:39` |
| hook body 內不要寫死驗證規則（資料驗證情境） | 「Prefer loading schemas from the `schemas` module (as shown) or from config; **avoid hard-coding rules inside hook/node bodies**.」 | `docs/integrations-and-plugins/pandera.md:314` |
| 建議把相關 hook 實作收在一個 class/namespace 裡 | 「We recommend that you group related Hook implementations under a namespace, preferably a class, within a `hooks.py` file that you create in your project.」（同頁註明模組名不限於 `hooks.py`） | `docs/extend/hooks/introduction.md:122` |

`before_node_run` 的**官方允許的改寫權限**（這是 hook 唯一能改資料流的地方，明文）：
> `【官方-原始碼】` 「Returns: Either None or a dictionary mapping dataset name(s) to new value(s). If returned, this dictionary will be used to update the node inputs, which **allows to overwrite the node inputs**.」（`kedro/framework/hooks/specs.py:72–76`）

- `【查不到】` 官方**沒有**明文說「不要把商業邏輯放進 hook」。最接近的只有上表 pandera 那條與「keep stored data small / read-only」。

---

## §7 Kedro 明文反對的做法（anti-pattern 匯總）

以下全部是我在 tag 1.5.0 的 `docs/` 與 `kedro/` 中 grep `should not|must not|do not|don't|avoid|never|not recommend|discourag` 得到、且**與框架設計相關**（已剔除安裝教學、TSC 治理、平台部署細節）的句子。除非另註，皆為 `【官方-文件】`。

### 7.1 Node / Pipeline
1. 「We do not recommend that you load and manipulate a data catalog directly in a Kedro node. Nodes are designed to be pure functions and thus should remain agnostic of I/O.」（`configure/configuration_basics.md:250`）
2. 「Nodes that are created with input or output names that contain `.` risk a disconnected pipeline or improperly-formatted Kedro structure. … **We recommend use of characters like `_` instead of `.` as name separators.**」（`build/pipeline_introduction.md:250, 254`，"Pipeline nodes named with the dot notation" 段）
3. 「For every two variables where the first depends on the second, the second must not also depend on the first.」（`build/pipeline_introduction.md:220`）
4. `【官方-原始碼】` 「Parameters should be specified in the 'parameters' argument」／「Inputs must not be outputs from another node in the same pipeline」／「All outputs must be generated by some node within the pipeline」（`kedro/pipeline/pipeline.py:60–74`，是 `Pipeline(...)` 包裝時的 safeguard）
5. 「Each pipeline should ideally be organised in its own folder, promoting copying and reuse within the project. **In short: one pipeline, one folder.**」（`build/pipeline_introduction.md:259`）

### 7.2 Namespace / Tag（分組語意）
6. 「**Defining namespaces at node level is not recommended for grouping your nodes.** The node level definition of namespaces should be used for creating collapsible views on Kedro-Viz for high level representation…」（`deploy/nodes_grouping.md:76`）
7. 「The use of `namespace` at node level is not recommended for grouping your nodes for deployment as this behaviour differs from defining `namespace` at `Pipeline` level. When defining namespaces at the node level, they behave similarly to tags and **do not guarantee execution consistency**.」（`build/namespaces.md:294`）
8. 「nodes with the same tag can exist in different pipelines. This overlap can make debugging and maintenance more challenging. **Tags also do not enforce structure** like pipelines or namespaces do.」（`deploy/nodes_grouping.md:39`）

### 7.3 Catalog / Dataset
9. 「This pattern is not recommended unless you are using a hosted notebook environment such as SageMaker or Databricks. … **Use the YAML approach in preference.**」（`catalog-data/advanced_data_catalog_usage.md:169`）
10. 「We do not recommend passing exact load or save versions, since it might lead to inconsistencies between operations.」（`catalog-data/advanced_data_catalog_usage.md:301`）
11. 「Kedro datasets should work with the `kedro.runner.SequentialRunner` and the `kedro.runner.ParallelRunner`, so **they must be fully serialisable** by the Python multiprocessing package.」（`extend/how_to_create_a_custom_dataset.md:513`）
12. `【官方-原始碼】` 「In order to utilize multiprocessing you need to make sure all datasets are serialisable, i.e. **datasets should not make use of lambda functions, nested functions, closures etc.** If you are using custom decorators ensure they are correctly decorated using `functools.wraps()`.」（`kedro/io/data_catalog.py:1275–1281`）
13. `【官方-原始碼】` 「… make sure all nodes are serialisable, i.e. **nodes should not include lambda** …」（`kedro/runner/parallel_runner.py:91–93`）
14. `【官方-原始碼】` 「If a specific dataset implementation cannot be used in conjunction with the `ParallelRunner`, such user-defined dataset should have the attribute `_SINGLE_PROCESS = True`.」（`kedro/io/core.py:111–113`）
15. `【官方-文件】` 「Raw: Initial start of the pipeline, containing the sourced data model(s) that **should never be changed**, it forms your single source of truth to work from.」（`getting-started/faq.md:85`，data engineering convention 表）

### 7.4 Config / 安全
16. 「Do not put private access credentials in the base configuration folder or any other configuration environment folder that is stored in version control.」（`configure/configuration_basics.md:56`）
17. 「Do not add any local configuration to version control.」（`configure/configuration_basics.md:62`）
18. 「We do not recommend using environment variables for configurations other than credentials.」（`configure/advanced_configuration.md:304`）
19. 「You can use this resolver solely in `credentials.yml`, not in catalog or parameter files.」（`configure/advanced_configuration.md:327`）
20. 「Do not commit data to version control. / Do not commit notebook output cells… / Do not commit credentials in `conf/`.」（`tutorials/tutorial_template.md:49–51`）
21. 「The `.telemetry` file should not be committed to `git` or packaged in deployment.」（`about/telemetry.md:52`）

### 7.5 Hooks
22–27：見 §6.3 表格（不用預設參數值、不依賴執行順序、實作名同 spec、stateful 資料小且唯讀、ParallelRunner 限制、hook body 不寫死規則）。

### 7.6 官方對自身邊界的宣告（不是 anti-pattern，但同樣是「明文不做什麼」）
28. `【官方-部落格】` 「**Kedro is intentionally not a pure orchestrator.** Instead, it focuses on how pipeline logic is structured and expressed, while remaining compatible with a wide range of execution and orchestration environments. Likewise, it is not designed to replace gen AI or modeling frameworks…」「Kedro does not replace orchestration tools. Instead, it complements them by ensuring that the pipelines they execute are well-structured, modular, maintainable, and reproducible.」（<https://kedro.org/blog/kedro-in-the-data-and-ai-landscape>）
29. `【官方-部落格】` 「defining pipelines inside an orchestrator can also tightly couple business logic to operational concerns … **Kedro takes a different approach and separates the pipeline definitions from orchestration.**」（同上）
30. `【官方-文件】` 「**Kedro is a code authoring framework, not a web application.** Unlike multi-tenant web services, untrusted users do not interact with Kedro through a UI or API.」（`about/security_model.md:10`）——這條決定了「Kedro 不對 config/程式碼做沙箱化」的信任模型。

### 7.7 官方的七條開發原則（哲學層，可判定性低但值得對照）
`【官方-部落格】`〈Seven development principles for opinionated teams〉，2023-04-26 發布（2023-05-10 更新），作者 Jo Stichbury（QuantumBlack Technical Writer），文中說明這些原則於 2021 年由 Kedro team 議定。
URL：<https://kedro.org/blog/development-principles-for-opinionated-teams>（2026-08-03 查閱）

1. **Modularity at the core** — 「we take this as our core tenet and make sure Kedro's own components are modular and independent of each other as much as possible. **Each component has clearly defined responsibilities and narrow interfaces.** We aim to make most of our components highly decoupled from each other and ensure they can be used on their own.」
2. **Grow beginners into experts**
3. **User empathy without unfounded assumptions**
4. **Simplicity means bare necessities** — 「something composed of small number of parts, with small number of features or functional branches and having very little optionality」
5. **There should be one obvious way of doing things** — 「Kedro is an opinionated framework, and this is built into its design.」
6. **A sprinkle of magic is better than a spoonful of it** — 「we have a strong preference for common sense over dark magic and making things obvious rather than clever.」
7. **Lean process and lean product**

> 其中只有第 1 條的粗體句（元件間高度解耦、可獨立使用、介面窄）勉強可判定（見 §8 C-14）；其餘屬品味宣示，對照表裡建議放「參考」欄而非「檢查」欄。

---

## §8 可判定約束清單（**主要素材**）

規則寫成「AI coding agent 讀得懂、且兩個人檢查會得到同一結論」的形式。`來源` 欄標明是官方哪一層；`可檢查方式` 是我建議的機械判定手段。
**注意：這是「Kedro 官方立場」的清單，不是「本 repo 應該照抄」的清單**——哪些要對齊、哪些刻意偏離，是下一步對照表的工作。

| # | 約束（可判定形式） | 來源 | 出處 | 可檢查方式 |
|---|---|---|---|---|
| C-1 | node 函式體內不得取得或操作 DataCatalog 物件（不得把 catalog 當參數傳入、不得在 node 模組內 import catalog 並呼叫 `load`/`save`） | 官方-文件 | `configure/configuration_basics.md:250` | grep node 模組是否 import catalog 類別或出現 `.load(`/`.save(` |
| C-2 | node 函式體內不得直接做檔案／網路 I/O（讀寫路徑、開連線）；I/O 一律由 catalog 條目宣告 | 官方-文件（由 C-1 的理由句「remain agnostic of I/O」推導的可檢查形式，**官方沒有逐條列舉禁用 API**） | 同上 + `build/run_a_pipeline.md:226–230` | grep node 模組的 `open(`/`read_csv(`/`to_parquet(`/`requests.`/`boto3` 等 |
| C-3 | 檔案路徑與儲存位置不得散落在程式碼中；必須集中在 catalog 設定 | 官方-部落格 | kedro.org/blog/kedro-in-the-data-and-ai-landscape | grep 程式碼中的字面路徑／bucket URI |
| C-4 | 程式化 `catalog.save(...)`／`catalog[...] = ...` 只允許出現在測試檔或 hosted notebook；產品程式碼一律用 YAML | 官方-文件 | `catalog-data/advanced_data_catalog_usage.md:169` | grep 非 `tests/` 路徑下的 `catalog.save(`／`catalog[` 指派 |
| C-5 | 每個 node 至少要有一個 input 或一個 output | 官方-原始碼（會 raise） | `kedro/pipeline/node.py:152` | 建構期驗證 |
| C-6 | node／tag 名稱只能含英數字、`-`、`_`、`.` | 官方-原始碼（會 raise） | `kedro/pipeline/node.py:165, 174` | 正規表達式 |
| C-7 | 同一 node 的 input 名不得與 output 名相同 | 官方-原始碼（會 raise） | `kedro/pipeline/node.py:719` | 建構期驗證 |
| C-8 | dataset 名稱不得含 `.`（`.` 保留給 namespace）；名稱分隔一律用 `_` | 官方-原始碼（UserWarning）＋官方-文件 | `kedro/pipeline/node.py:748–775`；`build/pipeline_introduction.md` | grep dataset 名稱 |
| C-9 | pipeline 不得有循環相依 | 官方-文件＋原始碼 | `build/pipeline_introduction.md:220` | 拓樸排序 |
| C-10 | pipeline 的宣告 inputs 必須是 free inputs（不得是同一 pipeline 內其他 node 的 output）；宣告 outputs 必須由 pipeline 內某個 node 產生；parameters 不得寫在 `inputs` 參數裡 | 官方-原始碼（會 raise `PipelineError`） | `kedro/pipeline/pipeline.py:60–74` | 建構期驗證 |
| C-11 | 同一 transcoded dataset 不得同時以帶／不帶分隔符的名稱被引用 | 官方-原始碼（會 raise） | `kedro/pipeline/pipeline.py:1419–1441` | 建構期驗證 |
| C-12 | 不得存 `None` 到 dataset | 官方-原始碼（會 raise `DatasetError`） | `kedro/io/core.py:303` | 執行期 |
| C-13 | 一個 pipeline 一個資料夾 | 官方-文件 | `build/pipeline_introduction.md`（"one pipeline, one folder"） | 目錄結構檢查 |
| C-14 | 框架元件之間高度解耦、介面窄、可單獨使用（例如 diagnostics 不得 import pipeline 內部函式） | 官方-部落格（原則 1） | kedro.org/blog/development-principles-for-opinionated-teams | import graph 檢查 |
| C-15 | node 級 namespace **不得**被用來做「分組執行／部署」；分組要用 pipeline 級 namespace 或 tag | 官方-文件 | `deploy/nodes_grouping.md:76`；`build/namespaces.md:294` | 檢查 grouping 邏輯讀的是哪一層 namespace |
| C-16 | 若要支援多行程執行：所有 dataset 與 node 必須可被 pickle——不得使用 lambda、巢狀函式、closure；自訂 decorator 必須 `functools.wraps` | 官方-原始碼（會 raise `AttributeError`）＋官方-文件 | `kedro/io/data_catalog.py:1275–1281`；`kedro/runner/parallel_runner.py:91–93`；`extend/how_to_create_a_custom_dataset.md:513` | `ForkingPickler.dumps` 試 dump／grep `lambda` |
| C-17 | 不能與多行程並用的 dataset 必須設 `_SINGLE_PROCESS = True` | 官方-原始碼 | `kedro/io/core.py:111–113` | 屬性檢查 |
| C-18 | credentials 一律不得進版控；`conf/base`（或任何進版控的環境資料夾）不得含機密 | 官方-文件＋官方樣板 | `configure/configuration_basics.md:56`；`configure/credentials.md:3`；樣板 `.gitignore:9–10` | `git ls-files` ∩ credentials 樣式 |
| C-19 | `conf/local/**` 不得進版控（僅放使用者專屬或受保護設定） | 官方-文件＋官方樣板 | `configure/configuration_basics.md:62`；樣板 `.gitignore:4–6` | `git ls-files conf/local` 應只剩 `.gitkeep` |
| C-20 | `data/**` 不得進版控 | 官方樣板 | 樣板 `.gitignore:12–13` | `git ls-files data` |
| C-21 | 同一環境資料夾內兩個非 parameters 設定檔不得有相同 top-level key（會 `ValueError`）；兩個 parameters 檔不得有相同的巢狀 sub-key | 官方-文件 | `configure/configuration_basics.md`（Configuration loading 段） | 載入期驗證／靜態掃 YAML |
| C-22 | 環境變數只准用於 credentials；`oc.env` resolver 不得出現在 catalog 或 parameters 檔 | 官方-文件 | `configure/advanced_configuration.md:304, 327` | grep `${oc.env:` 出現的檔案 |
| C-23 | `runtime_params` 不得覆寫 `globals`；`globals` 唯一入口是 yaml 檔；`globals` 的 key 不得以 `_` 開頭 | 官方-文件＋原始碼（會 raise） | `configure/advanced_configuration.md:229`；`kedro/config/omegaconf_config.py:234, 262, 478` | 載入期驗證 |
| C-24 | 模型超參數等同組參數要集中在單一位置定義 | 官方-文件 | `configure/parameters.md:7` | 檢查是否散落多檔 |
| C-25 | 版本化 dataset 的 save 目標路徑必須不存在；version 字串必須是單一非空路徑元件、不含 `/` 或 `\`、不得為 `.`/`..` | 官方-原始碼（會 raise） | `kedro/io/core.py:828, 837–839` | 執行期 |
| C-26 | 不得對 HTTP(S) filepath 開 `versioned: true` | 官方-文件＋原始碼（會 raise） | `catalog-data/data_catalog.md:232`；`kedro/io/core.py:985` | 靜態掃 catalog YAML |
| C-27 | `PartitionedDataset` 底層 dataset 不得開 `versioned` | 官方-文件（會 raise `DatasetError`） | `catalog-data/partitioned_and_incremental_datasets.md:98` | 靜態掃 catalog YAML |
| C-28 | 不得為中間資料集釘死精確的 load/save 版本 | 官方-文件＋原始碼（UserWarning，訊息含 "strongly discouraged"） | `catalog-data/advanced_data_catalog_usage.md:301`；`kedro/io/core.py:508–513` | 掃 `Version(load=..., save=...)` 用法 |
| C-29 | hook 實作的參數**不得**有預設值 | 官方-文件 | `extend/hooks/introduction.md:125` | AST 檢查 `@hook_impl` 函式簽章 |
| C-30 | 程式不得依賴 hook 執行順序；需要順序就用 `tryfirst`/`trylast` | 官方-文件 | `extend/hooks/introduction.md:185` | code review／grep 註解假設 |
| C-31 | hook 實作函式名必須等於 spec 名稱，參數必須是 spec 參數的子集 | 官方-文件 | `extend/hooks/introduction.md:80` | 對照 `kedro.framework.hooks.specs` 簽章 |
| C-32 | hook 間共享的狀態必須小且視為唯讀 | 官方-文件 | `extend/hooks/common_use_cases.md:233` | code review |
| C-33 | 若專案依賴 dataset/node 層級 hook，**不得**使用 `ParallelRunner` | 官方-文件 | `extend/hooks/introduction.md:39` | 檢查 runner 設定 × 已註冊 hook 種類 |
| C-34 | 驗證規則／schema 不得寫死在 hook 或 node 函式體內，應從模組或 config 載入 | 官方-文件 | `integrations-and-plugins/pandera.md:314` | code review／grep |
| C-35 | Raw 層資料不得被修改（single source of truth） | 官方-文件 | `getting-started/faq.md:85` | 檢查沒有 node 以 raw 層 dataset 為 output |
| C-36 | 自訂 config loader 若要用 interpolation/globals/runtime params/custom resolvers，必須繼承 `OmegaConfigLoader` | 官方-文件 | `configure/advanced_configuration.md:47–50` | 類別繼承檢查 |
| C-37 | `filter()` 多條件是**交集**語意，不是鏈式套用——實作切片時不得用「依序套用各 filter」代替 | 官方-原始碼 | `kedro/pipeline/pipeline.py:956–959` | 單元測試（給兩個條件，驗結果是交集） |
| C-38 | 「只跑缺漏輸出」的語意必須是：無 output 的 node 永遠跑；**持久化**輸出缺失才算缺；下游要跑則上游也要跑 | 官方-原始碼 | `kedro/runner/runner.py:608–633` | 單元測試三條規則各一 |

---

## §9 我不確定的部分（明說）

1. **`docs.kedro.org/en/stable` = 1.5.0 這件事，我是用 Read the Docs API 的 `ref` 欄推定的**，頁面本身沒有印版本號（`objects.inv` 的 Version 欄是 `0.0.0`）。若 RTD 之後把 stable 指到新版，本文的行號會失效。行號一律以 **GitHub tag `1.5.0`** 為準，那個是不會變的。
2. **§7 的 anti-pattern 清單不保證窮盡。** 我的 grep pattern 是 `should not|shouldn't|must not|do not|don't|avoid|never|not recommend|discourag|bad practice|anti-?pattern`，只掃 `docs/**/*.md` 與 `kedro/**/*.py`（tag 1.5.0），並手動剔除了部署平台教學、TSC 治理、安裝教學等與框架設計無關者。用其他措辭（例如 "we suggest…not"、"consider…instead"）表達的勸阻可能漏掉。
3. **Kedro 沒有公開的 RFC / ADR / design docs 目錄。** 我檢查了 tag 1.5.0 的完整檔案樹（710 個 blob），頂層只有 `.agents/ .github/ docs/ features/ kedro/ kedro_benchmarks/ static/ tests/ tools/`，沒有 `rfcs/`、`adr/`、`designs/`。`gh search issues --repo kedro-org/kedro "RFC"` 只回四筆且都是一般 issue。因此「官方 RFC 的設計理由」這一項的答案是 **`【查不到】`——它不存在**，不是我沒找到。
4. **`kedro_technical_charter.pdf`（repo 根目錄）我沒讀。** 本機缺 poppler/pypdf，無法解析。從檔名與 `docs/about/technical_steering_committee.md` 的內容推測是 Linux Foundation 的治理章程，**不是技術設計文件**；但這是推測，未驗證。
5. **§2.3 第 4 點關於 Spark DataFrame 走 `"assign"` copy mode 的推論**：我讀到的是 `_infer_copy_mode` 對「`type(data).__name__ == "DataFrame"`」回傳 `"assign"`（`kedro/io/memory_dataset.py:110`）。pyspark 的 `DataFrame` 類別名稱確實是 `DataFrame`，所以會命中；但我**沒有實跑驗證**。要用在對照表裡建議先跑一個三行的實驗。
6. **Hooks 的「為什麼存在」那段（§6.1）只存在於 0.19.x 文件。** 我確認了 1.5.0 的 `docs/extend/hooks/` 只有三個檔、`mkdocs.yml` 也只掛三個檔，所以是真的被移除而非移到別處；但我沒有逐一比對 0.19→1.0 的 docs diff 來確認它沒有被搬到其他頁面的某個角落。
7. **「什麼該進 config、什麼不該」我只找到環境變數那一條硬規則**（§4.4）。常見的「路徑進 catalog、超參數進 parameters」說法我在官方文件裡找不到明文；若對照表需要這條，必須標成本 repo 自訂而非 Kedro 原則。
8. **官方對 versioning 與 reproducibility 的關係，沒有邊界宣告**（§5.3）。我用 `KedroSession.run()` 的 `record_data` 欄位清單補了事實面，但那是我讀原始碼推出來的，官方沒有把它寫成「我們不保證 X」。

---

## §10 查詢紀錄（供判斷是「真沒有」還是「沒找到」）

**主要資料源（皆 2026-08-03 取得）**
- `https://docs.kedro.org/en/stable/sitemap.xml` → 取得全站 URL 清單，逐頁 `curl` 下載 40 頁後轉純文字 grep。
- `https://github.com/kedro-org/kedro/archive/refs/tags/1.5.0.tar.gz` → 完整原始碼 + `docs/*.md` 本機 grep（本文所有行號的來源）。
- `https://app.readthedocs.org/api/v3/projects/kedro/versions/stable/` → 確認 stable = 1.5.0。
- `https://pypi.org/pypi/kedro/json` → 版本與發布時間。
- `https://raw.githubusercontent.com/kedro-org/kedro/1.5.0/README.md`
- `https://docs.kedro.org/en/0.19.14/hooks/index.html`（找回 1.x 移除的 hooks rationale）
- `https://docs.kedro.org/en/0.19.14/tutorial/test_a_project.html`
- `https://kedro.org/blog/development-principles-for-opinionated-teams`
- `https://kedro.org/blog/kedro-in-the-data-and-ai-landscape`
- `https://github.com/kedro-org/kedro/discussions/861`

**用過的搜尋詞 / grep pattern**
- WebSearch：`Kedro docs Data Catalog why "node functions" should not do I/O`、`Kedro documentation node "pure function" side effects requirements`、`kedro.org blog design principles why nodes pure functions data catalog rationale`、`Kedro "design principles" OR "principles" official blog QuantumBlack framework opinionated`
- `gh search code --repo kedro-org/kedro`：`"pure function"`、`"should not"`、`"anti-pattern"`、`"not recommended"`、`"we do not recommend"`、`"avoid using"`、`"separation of concerns"`、`"single responsibility"`、`"reproducible"`、`git_sha`
- `gh search issues --repo kedro-org/kedro`：`RFC`、`git_sha`
- 本機 grep（tag 1.5.0）：`should not|shouldn't|must not|do not |don't |avoid|never |not recommend|discourag|bad practice|anti-?pattern`（`docs/**/*.md`）、`should not|must not|cannot|not allowed|not supported|deterministic|idempotent|side.effect|reproducib`（`kedro/**/*.py`）、`reproduc`（全 docs）、`run_only_missing|only_missing_outputs`、`git_sha`

**零命中的查詢（即「官方真的沒寫」的證據）**
- repo 檔案樹無 `rfcs/`、`adr/`、`design/`、`architecture/` 目錄。
- `grep -rn "git_sha" kedro/ docs/`（tag 1.5.0）零命中。
- `grep -rn "def run_only_missing" kedro/`（tag 1.5.0）零命中（但文件仍在教）。
- 找不到任何「dataset versioning 不涵蓋 X」形式的官方句子。
- 找不到「不要把商業邏輯放進 hook」的官方句子。
