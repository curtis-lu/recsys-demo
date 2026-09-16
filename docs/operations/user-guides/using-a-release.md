# 使用發布版本

這份文件給要拿這個框架跑自己排序題目的人。讀完你應該能完成四件事：取得一個固定的版本、把它裝起來、把自己的設定放在升級時不會被覆蓋的地方、以及換到下一個版本時知道要檢查什麼。

要修改框架本身的人不需要本文，從 [README](../../../README.md) 開始讀。

## 1. 版本、tag 與 branch

```
tag      v0.1.0 ──► 某一個 commit         貼上去就不會動
branch   main ──●──●──●──●──►             持續往前長，內容隨時會變
version  pyproject.toml 的 version = "0.1.0"
```

- **tag** 是釘在某個 commit 上的名字。checkout 同一個 tag，任何時候拿到的都是同一份程式碼。
- **branch** 會持續前進。追 branch 等於每次同步都可能拿到不同的行為。
- **version** 是 `pyproject.toml` 裡的字串，發布時與 tag 名稱一致：tag `v0.1.0` 對應 `version = "0.1.0"`。

**請用 tag 取版本。** `release/*` branch 是維護舊版修正的工作分支，內容會變動。

## 2. 取得一個版本

能連到 GitHub 時：

```bash
git clone https://github.com/curtis-lu/recsys-demo.git
cd recsys-demo
git tag                  # 列出可用的版本
git checkout v0.1.0
```

`git checkout <tag>` 之後 git 會說你在 detached HEAD。這是正常的，代表你停在一個固定的快照上而不是某條 branch 的最前端。要在上面改東西就自己開一條 branch：`git switch -c my-deployment v0.1.0`。

執行環境不能連網時，在能連網的機器上打包該 tag 再搬進去：

```bash
git archive --prefix=recsys_tfb-v0.1.0/ --format=tar.gz -o recsys_tfb-v0.1.0.tar.gz v0.1.0
shasum -a 256 recsys_tfb-v0.1.0.tar.gz     # 搬運後在目的端重算，比對這個值
```

`--prefix` 讓解壓縮後的檔案落在一個資料夾裡，不會散進當前目錄。

搬過去的目錄沒有 `.git`，這會影響一項紀錄：框架寫 manifest（`data/models/<model_version>/manifest.json` 這類檔案）時會呼叫 `git rev-parse` 取得 commit hash 填進 `git_commit` 欄位，見 `src/recsys_tfb/core/versioning.py` 的 `get_git_commit()`。

- 沒有 `.git` 時這一欄是 `null`。
- **更需要注意的情況**：`get_git_commit()` 沒有指定工作目錄，所以如果你把解壓出來的目錄放在**另一個 git repo 底下**，這個指令會成功，填進去的是**那個外層 repo** 的 commit——看起來完全合理，卻和實際執行的程式碼無關。

要保留「這個產物是哪一版做的」這項紀錄，最可靠的做法是把 tag 名稱記在自己的部署流程裡，不要只依賴這個欄位。

## 3. 安裝

`pyproject.toml` 要求 Python `>=3.10,<3.12`，repo 開發時固定使用 3.10.9（見 `.python-version`）。

```bash
python3.10 -m venv .venv
.venv/bin/pip install -e .
```

`pip install -e .` 會把 `src/` 底下的套件裝成可 import，並安裝 `pyproject.toml` 裡 14 個釘死版本的相依套件（pyspark 3.3.2、lightgbm 4.6.0、pandas 1.5.3 等）。repo 裡沒有 `requirements.txt`，`pyproject.toml` 就是唯一的相依清單。

裝完之後**一律用 `.venv/bin/python` 執行**，不要用系統的 `python`／`python3`：相依套件都釘死版本，跑在別的直譯器上不保證行為相同。

框架假設正式執行環境**不能安裝額外套件、不能對外連網、不能使用 Spark UDF**，CPU-only。所以上面這個安裝步驟通常由能連網的機器先做好，再把整個環境搬進去。

## 4. 執行指令的兩個前提

```bash
.venv/bin/python -m recsys_tfb <pipeline> --env <你的環境>
```

`<pipeline>` 是這八個之一：`feature_etl`、`label_etl`、`sample_pool_etl`、`inference_population_etl`、`dataset`、`training`、`evaluation`、`inference`。沒有 `run` 子指令，也沒有 `--pipeline` 旗標。

兩個容易踩到的前提：

1. **從 repo 根目錄執行。** 框架用當前工作目錄去找 `conf/`，換個目錄跑會找不到設定。
2. **`--env` 的預設值是 `local`。** 忘記帶這個旗標不會有任何錯誤，指令會安靜地用 `conf/local/` 的設定跑完。正式環境的指令一定要明寫 `--env`。

## 5. 你的設定放哪裡

設定分兩層。`conf/<env>/` 疊在 `conf/base/` 上面，同名的鍵以 `conf/<env>/` 為準：

```
conf/base/parameters.yaml      框架附的預設值  ← 升級時會被新版覆蓋
        ＋
conf/<env>/parameters.yaml     你的設定        ← 升級不會動到
        ＝
這次執行實際生效的設定
```

**不要直接修改 `conf/base/`。** 改了之後每次升級都會在框架的檔案裡產生衝突，而判斷「該留哪一邊」需要理解框架這一版改了什麼。

建立自己的環境層：

```bash
mkdir -p conf/production
```

只把要覆蓋的鍵寫進去。檔名必須和 `conf/base/` 裡的檔案相同才會疊上去——`conf/production/parameters_training.yaml` 覆蓋 `conf/base/parameters_training.yaml`，兩邊的內容逐層合併。

`--env` 指到不存在的目錄時，框架會中止並印出 `(A30)` 開頭的訊息，內容包含它找的絕對路徑，以及 `conf/` 底下現有的目錄清單。這道檢查在 `src/recsys_tfb/core/consistency.py` 的 `resolved_env_dir()`。少了它，打錯字的 `--env` 會安靜地整場只用 `conf/base/` 跑完。

### 這個分層蓋不到的兩個地方

- **`conf/sql/etl/**` 沒有環境分層。** 來源表的 SQL 只有一份固定路徑（`src/recsys_tfb/__main__.py` 的 `sql_dir = conf_dir / "sql" / "etl"`），而每個使用者都必須改它來接自己的資料表。**這是升級時唯一保證會衝突的地方。** 請把你的 SQL 另外在 repo 以外保存一份，或納入你自己的版本控制，升級後再套回去。
- **`conf/base/catalog.yaml` 可以疊。** 要換 Hive database 或表名時，在 `conf/<env>/catalog.yaml` 覆蓋即可，同樣不必改 base。

另外，`conf/<env>/` 是你自己新增的檔案，不在框架的版本控制裡。`git clean -fdx` 這類清理指令會把它刪掉，跟著 repo 一起做的備份也不會包含它。**請在 repo 以外另存一份。**

### 舊的鍵名不會報錯

框架沒有「未知鍵」檢查。`conf/<env>/` 裡留著一個新版已經改名或移除的鍵，不會有任何訊息，那個鍵只是再也沒有人讀它，該項行為會退回預設值——可能是 `conf/base/` 裡的新預設，也可能是寫在程式碼裡的預設。所以升級後如果行為變了卻找不到原因，先檢查自己的設定層有沒有對不上的鍵名。

## 6. 確認這份程式碼在你的機器上跑得起來

repo 附的不是資料，是**產生器**：`data/` 在版本控制裡是空的，`scripts/generate_synthetic_data.py` 會在需要時產生合成資料，`scripts/local_spark_setup.py` 發現缺檔時會自動呼叫它。所以你 clone 下來看到空的 `data/` 是正常的。

跑之前先確認**作業系統時區與 Spark 設定的時區一致**。`conf/spark-local/spark-defaults.conf` 把 `spark.sql.session.timeZone` 設為 `Asia/Taipei`；作業系統時區不同時，以月份為單位的日期過濾可能篩不到任何資料，而且不會報錯，只會得到 0 列。對齊的方式是執行前設好環境變數：

```bash
export TZ=Asia/Taipei
export SPARK_CONF_DIR=$PWD/conf/spark-local
.venv/bin/python scripts/local_spark_setup.py
.venv/bin/python -m recsys_tfb dataset  --env local
.venv/bin/python -m recsys_tfb training --env local
```

`local_spark_setup.py` 會建立本機 warehouse 與 metastore，並把合成資料載入成 `ml_recsys.*` 表（`ml_recsys` 是 `conf/base/parameters.yaml` 裡 `hive.db` 的值）。`conf/local/` 已經在 repo 裡，所以 `--env local` 過得了 A30 檢查。

要一次跑完整條鏈（含 inference，並在結尾斷言三張推論表的分區結構），用 `bash scripts/local_e2e.sh`。它預設使用 repo 根目錄的 `.venv/bin/python`，要指定別的直譯器就設 `RECSYS_PYTHON=<python 路徑>`；找不到可執行的直譯器時會直接中止。這支腳本開頭會 `--reset`，本機 warehouse 與 metastore 會被清掉重建。

本機環境的完整說明見 [local-spark-setup.md](../dev-setup/local-spark-setup.md)。接自己的資料從 [README §3 快速上手](../../../README.md) 開始。

## 7. 升級到下一個版本

```bash
git fetch --tags
git tag                                        # 有哪些新版本
git diff v0.1.0..v0.2.0 -- conf/base/          # 預設設定改了什麼
git diff v0.1.0..v0.2.0 -- conf/sql/           # 來源表 SQL 改了什麼
git checkout v0.2.0
.venv/bin/pip install -e .                     # 相依套件可能跟著翻版本
```

中間那兩個 `git diff` 是升級前最值得花的時間：

- `conf/base/` 的差異告訴你**設定鍵有沒有改名或搬家**，那是升級後最常見的故障來源，而且如上一節所說，對不上的鍵不會報錯。
- `conf/sql/` 的差異告訴你**框架的 SQL 範本改了什麼**，你自己那份要不要跟著改。這是分層蓋不到的地方，只能靠人比對。

另外兩件不會出現在設定差異裡、但會改變行為的事：

- 版本 ID 的計算方式若有變動（`src/recsys_tfb/core/versioning.py`），既有的資料集與模型版本不會被判定成命中，下一次執行會整批重算。
- 來源表 schema 的變動由上游決定，跟版本無關，但同樣會讓結果變化。

換版本之後先重跑 §6 確認環境仍然完整，再接自己的資料。

## 8. 舊版本的修正

- 新功能只加在最新版本。
- 舊版本的修正會放在對應的 `release/*` branch 上，修好後發成新的 patch tag，例如 `v0.1.1`。取用時一樣 checkout tag，不要追 branch。
- 回報問題時附上你使用的 tag 名稱與 `--env` 名稱，兩者一起決定了實際生效的設定。
