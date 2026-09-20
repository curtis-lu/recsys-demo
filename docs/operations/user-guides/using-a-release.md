# 使用發布版本

這份文件給要拿這個框架跑自己排序題目的人。讀完你應該能完成四件事：取得一個固定的版本、把它裝起來、把自己的設定放在升級時不會被覆蓋的地方、以及換到下一個版本時知道要檢查什麼。

要修改框架本身的人不需要本文，從 [README](../../../README.md) 開始讀。

## 1. 版本、tag 與 branch

```
tag      v0.2.0 ──► 某一個 commit         貼上去就不會動
branch   main ──●──●──●──●──►             持續往前長，內容隨時會變
version  pyproject.toml 的 version 欄位    每次發布時對齊 tag 名稱
```

- **tag** 是釘在某個 commit 上的名字。checkout 同一個 tag，任何時候拿到的都是同一份程式碼。
- **branch** 會持續前進。追 branch 等於每次同步都可能拿到不同的行為。
- **version** 是 `pyproject.toml` 裡的字串，發布時與 tag 名稱對齊：tag `v0.2.0` 對應 `version = "0.2.0"`。checkout 之後想確認自己手上是哪一版，看這個欄位。

**請用 tag 取版本。** `release/*` branch 是維護舊版修正的工作分支，內容會變動。

## 2. 取得一個版本

能連到 GitHub 時：

```bash
git clone https://github.com/curtis-lu/recsys-demo.git
cd recsys-demo
git tag                  # 列出可用的版本
git checkout v0.2.0
```

`git checkout <tag>` 之後 git 會說你在 detached HEAD。這是正常的，代表你停在一個固定的快照上而不是某條 branch 的最前端。要在上面改東西就自己開一條 branch：`git switch -c my-deployment v0.2.0`。

執行環境不能連網時，在能連網的機器上打包該 tag 再搬進去：

```bash
git archive --prefix=recsys_tfb-v0.2.0/ --format=tar.gz -o recsys_tfb-v0.2.0.tar.gz v0.2.0
sha256sum recsys_tfb-v0.2.0.tar.gz     # macOS 沒有這支指令，改用 shasum -a 256
```

`--prefix` 讓解壓縮後的檔案落在一個資料夾裡，不會散進當前目錄。把上面印出的 hash 帶到目的端重算一次比對，再解壓。

搬過去的目錄沒有 `.git`，這會影響一項紀錄：框架寫 manifest（`data/models/<model_version>/manifest.json` 這類檔案）時會呼叫 `git rev-parse` 取得 commit hash 填進 `git_commit` 欄位，見 `src/recsys_tfb/core/versioning.py` 的 `get_git_commit()`。沒有 `.git` 時這一欄是 `null`；更麻煩的是，那個指令沒有指定工作目錄，所以把解壓出來的目錄放在**另一個 git repo 底下**時它會成功，填進去的是那個外層 repo 的 commit——看起來合理，卻和實際執行的程式碼無關。

**所以請把 tag 名稱記在自己的部署流程裡，不要只依賴這個欄位。** 走 archive 這條路的人，升級程序也和 git 使用者不同，見 §7。

## 3. 安裝

`pyproject.toml` 要求 Python `>=3.10,<3.12`，repo 開發時固定使用 3.10.9（見 `.python-version`）。

```bash
python3.10 -m venv .venv
.venv/bin/pip install -e .
```

`pip install -e .` 會把 `src/` 底下的套件裝成可 import，並安裝 `pyproject.toml` 裡 14 個釘死版本的相依套件（pyspark 3.3.2、lightgbm 4.6.0、pandas 1.5.3 等）。repo 裡沒有 `requirements.txt`，`pyproject.toml` 就是唯一的相依清單。

裝完之後**一律用 `.venv/bin/python` 執行**，不要用系統的 `python`／`python3`：相依套件都釘死版本，跑在別的直譯器上不保證行為相同。

框架假設正式執行環境**不能安裝額外套件、不能對外連網、不能使用 Spark UDF**，CPU-only。所以安裝通常在能連網的機器上先做好。要注意 **venv 不能整個資料夾搬走**：`pyvenv.cfg` 與各 script 的 shebang 記的是絕對路徑，`pip install -e .` 還會寫一個指向原始碼樹絕對路徑的檔案。可行的做法是讓目的端的路徑與來源端**逐字相同**，或在目的端用離線的套件檔重建 venv。

## 4. 執行指令的兩個前提

```bash
.venv/bin/python -m recsys_tfb <pipeline> --env <你的環境>
```

`<pipeline>` 是這八個之一：`feature_etl`、`label_etl`、`sample_pool_etl`、`inference_population_etl`、`dataset`、`training`、`evaluation`、`inference`。沒有 `run` 子指令，也沒有 `--pipeline` 旗標。

兩個容易踩到的前提：

1. **從 repo 根目錄執行。** 框架用當前工作目錄去找 `conf/` 和 `data/`。`conf/` 找不到會被大聲擋下，但 `data/` 不會——它會安靜地在你當下所在的目錄底下建出一棵新的產物樹，你會以為模型沒產生。
2. **`--env` 的預設值是 `local`。** 忘記帶這個旗標不會有任何錯誤。而 repo 附的 `conf/local/` 是空的（只有一個佔位檔），所以那一場等於整場只用 `conf/base/` 的預設值跑完，你的設定一條都沒生效。正式環境的指令一定要明寫 `--env`。

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

**檔名要和 `conf/base/` 裡的檔案逐字相同**：`conf/production/parameters_training.yaml` 覆蓋 `conf/base/parameters_training.yaml`，兩邊的內容逐層合併。

檔名取錯**不會報錯，而且後果比完全失效更難發現**：框架有兩種取設定的方式，一種把所有 `parameters*.yaml` 合成一份（你那個檔名不存在於 base 的檔案也會被併進來），另一種按指定檔名取（呼叫點寫死檔名，看不到你的檔）。兩者都在 `src/recsys_tfb/core/config.py`。結果是**一部分節點吃到了你的設定、一部分沒有**；更糟的是資料集版本 ID 走的是後者，於是**行為變了、版本 ID 沒變**，下一次執行會把舊資料集判定成命中而直接跳過重算。所以檔名請照抄，不要自創。

`--env` 指到不存在的目錄時，框架會中止並印出 `(A30)` 開頭的訊息，內容包含它找的絕對路徑，以及 `conf/` 底下現有的目錄清單。這道檢查在 `src/recsys_tfb/core/consistency.py` 的 `resolved_env_dir()`。少了它，打錯字的 `--env` 會安靜地整場只用 `conf/base/` 跑完。

### 這個分層蓋不到的兩個地方

- **Spark 的連線設定不在 `conf/<env>/` 裡。** 實際生效的值來自環境變數 `SPARK_CONF_DIR` 指向的那個資料夾裡的 `spark-defaults.conf`（`conf/base/parameters.yaml` 的 `spark:` 區塊上方就是這樣註明的）。repo 附的 `conf/spark-local/` 是本機測試用的那一份，**要接自己的叢集，就自己準備一個資料夾並把 `SPARK_CONF_DIR` 指過去**，不要就地改 repo 附的那份——它在版本控制裡，升級會衝突。
- **`conf/sql/etl/**` 沒有環境分層。** 來源表的 SQL 只有一份固定路徑（`src/recsys_tfb/__main__.py` 的 `sql_dir = conf_dir / "sql" / "etl"`）。只有 `feature_etl`、`label_etl`、`sample_pool_etl`、`inference_population_etl` 這四支指令會用到它——如果你的三張來源表是別的流程準備好的，你根本不會碰這裡。但**只要你改了它，升級就一定會衝突**：請把你的 SQL 另外在 repo 以外保存一份，或納入你自己的版本控制，升級後再套回去。

相對地，**`conf/base/catalog.yaml` 是可以疊的**：要換 Hive database 或表名，在 `conf/<env>/catalog.yaml` 覆蓋即可，不必改 base。

另外，`conf/<env>/` 是你自己新增的檔案，不在框架的版本控制裡。`git clean -fdx` 這類清理指令會把它刪掉，跟著 repo 一起做的備份也不會包含它。**請在 repo 以外另存一份。**

### 舊的鍵名不會報錯

框架沒有「未知鍵」檢查。`conf/<env>/` 裡留著一個新版已經改名或移除的鍵，不會有任何訊息，那個鍵只是再也沒有人讀它，該項行為會退回預設值——可能是 `conf/base/` 裡的新預設，也可能是寫在程式碼裡的預設。所以升級後如果行為變了卻找不到原因，先檢查自己的設定層有沒有對不上的鍵名。

**例外：校準相關的退役鍵會報錯。** #411 移除機率校準機制後留下的六個設定鍵（`RETIRED_CALIBRATION_KEYS`：`dataset.enable_calibration`、`dataset.calibration_snap_dates`、`dataset.calibration_sample_ratio`、`dataset.calibration_sample_ratio_overrides`、`training.calibration`、`inference.use_calibration`），只要**出現**在設定裡就會被擋下，值設成 `false` 或 `[]` 一樣算數——因為它們位在算進 `base_dataset_version` / `model_version` 的子樹裡，留著會讓沒刪的 conf 樹算出跟已刪的 conf 樹不同的版本 ID。這是不變量 A37，predicate 是 `retired_calibration_key_errors`（`src/recsys_tfb/core/consistency.py:1637`），常數定義在同檔 `src/recsys_tfb/core/consistency.py:1610`。

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

`local_spark_setup.py` 會建立本機 warehouse 與 metastore，並把合成資料載入成 `ml_recsys.*` 表（`ml_recsys` 是 `conf/base/parameters.yaml` 裡 `hive.db` 的值）。

要一次跑完整條鏈（含 inference，並在結尾斷言三張推論表的分區結構），用 `bash scripts/local_e2e.sh`。**v0.1.1 起**這支腳本預設使用 repo 根目錄的 `.venv/bin/python`，要指定別的直譯器就設 `RECSYS_PYTHON=<python 路徑>`，找不到可執行的直譯器時會直接中止；`v0.1.0` 的版本把直譯器路徑寫死成開發者機器上的絕對路徑，在別的機器上跑不起來。腳本開頭會 `--reset`，本機 warehouse 與 metastore 會被清掉重建。

本機環境的完整說明見 [local-spark-setup.md](../dev-setup/local-spark-setup.md)。接自己的資料從 [README §3 快速上手](../../../README.md#3-快速上手) 開始。

## 7. 升級到下一個版本

用 git 取版本的人：

```bash
git fetch --tags
git tag                                        # 有哪些新版本
git diff v0.1.1..v0.2.0 -- conf/base/          # 預設設定改了什麼
git diff v0.1.1..v0.2.0 -- conf/sql/           # 來源表 SQL 範本改了什麼
git diff v0.1.1..v0.2.0 -- conf/spark-local/   # Spark 設定範本改了什麼
git checkout v0.2.0
.venv/bin/pip install -e .                     # 相依套件可能跟著翻版本
```

用 archive 搬進去的人**跑不了上面這些指令**（那個目錄沒有 `.git`）。改成這樣：在能連網的機器上對兩個 tag 產生一份差異報告帶進去，或把兩版都解壓在同一台機器上直接比對目錄：

```bash
git diff v0.1.1..v0.2.0 -- conf/ > upgrade-v0.1.1-to-v0.2.0.diff   # 能連網的機器上
diff -ru recsys_tfb-v0.1.1/conf recsys_tfb-v0.2.0/conf              # 或兩版都解壓後比對
```

那幾個差異是升級前最值得花的時間：

- `conf/base/` 的差異告訴你**設定鍵有沒有改名或搬家**，那是升級後最常見的故障來源，而且如上一節所說，對不上的鍵不會報錯。
- `conf/sql/` 與 `conf/spark-local/` 的差異告訴你**框架附的範本改了什麼**，你自己那兩份要不要跟著改。這兩處分層蓋不到，只能靠人比對。

另外兩件不會出現在設定差異裡、但會改變行為的事：

- 版本 ID 的計算方式若有變動（`src/recsys_tfb/core/versioning.py`），既有的資料集與模型版本不會被判定成命中，下一次執行會整批重算。
  **移除機率校準的那一版就是這種情況**（#411）：校準相關的設定鍵整包從 `dataset:` 子樹拿掉，而 `base_dataset_version` 是對那整個子樹取 hash，所以每個 `base_dataset_version` 都會換一個值——既有資料集要重建、模型要重訓，並重新用 `scripts/promote_model.py` 人工 promote。`train_variant_id` 的算法沒動，值不受影響。這是刻意的版本決策，不是 bug；留在設定檔裡的校準鍵會被上一節那個例外擋下。
- 來源表 schema 的變動由上游決定，跟版本無關，但同樣會讓結果變化。

換版本之後先重跑 §6 確認環境仍然完整，再接自己的資料。

## 8. 舊版本的修正

- 新功能只加在最新版本。
- 舊版本的修正會放在對應的 `release/*` branch 上，修好後發成新的 patch tag，例如 `v0.1.1`。取用時一樣 checkout tag，不要追 branch。
- 回報問題時附上你使用的 tag 名稱與 `--env` 名稱，兩者一起決定了實際生效的設定。
