# 使用發布版本

這份文件給要拿這個框架跑自己排序題目的人，回答三件事：怎麼取得一個固定的版本、自己的設定該放哪裡才不會在升級時跟框架打架、以及怎麼換到下一個版本。

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

**請用 tag 取版本。** `release/*` branch 是維護舊版修正的工作分支，內容會變動，不適合當作依賴的對象。

## 2. 取得一個版本

能連到 GitHub 時：

```bash
git clone https://github.com/curtis-lu/recsys-demo.git
cd recsys-demo
git tag                  # 列出可用的版本
git checkout v0.1.0
```

執行環境不能連網時，在能連網的機器上打包該 tag 再搬進去：

```bash
git archive --format=tar.gz -o recsys_tfb-v0.1.0.tar.gz v0.1.0
```

搬運後**逐檔核對 hash**，不要只比對檔案數量；漏檔造成的故障不會在搬運當下顯現，排查成本見 [known-pitfalls.md §13](../known-pitfalls.md)。

用壓縮檔搬過去的目錄沒有 `.git`。框架在寫模型 manifest 時會呼叫 `git rev-parse` 取得 commit hash 填進 `git_commit` 欄位（`src/recsys_tfb/core/versioning.py`），沒有 `.git` 時這一欄是空的。要保留「這個模型是哪一版做的」這項紀錄，就把 tag 名稱記在自己的部署流程裡。

## 3. 執行環境

- Python 3.10.9。PySpark 3.3.2、LightGBM 4.6.0、pandas 1.5.3 等相依套件都釘死版本，完整清單在 `pyproject.toml`。
- 執行時把 `src/` 放進 `PYTHONPATH`：

  ```bash
  PYTHONPATH=src python -m recsys_tfb <pipeline> --env <你的環境>
  ```

  `<pipeline>` 是 `feature_etl`、`label_etl`、`sample_pool_etl`、`inference_population_etl`、`dataset`、`training`、`evaluation`、`inference` 其中之一。沒有 `run` 子指令，也沒有 `--pipeline` 旗標。
- 框架假設執行環境**不能安裝額外套件、不能對外連網、不能使用 Spark UDF**，CPU-only。

## 4. 自己的設定放哪裡

設定分兩層。`conf/<env>/` 疊在 `conf/base/` 上面，同名的鍵以 `conf/<env>/` 為準：

```
conf/base/parameters.yaml      框架附的預設值  ← 升級時會被新版覆蓋
        ＋
conf/<env>/parameters.yaml     你的設定        ← 升級不會動到
        ＝
這次執行實際生效的設定
```

**不要直接修改 `conf/base/`。** 改了之後每次升級都會在框架的檔案裡產生衝突，而判斷「該留哪一邊」需要理解框架這一版改了什麼，成本遠高於一開始就把設定放對地方。

建立自己的環境層：

```bash
mkdir -p conf/production
```

只把要覆蓋的鍵寫進去，檔名必須和 `conf/base/` 裡的檔案相同才會疊上去——`conf/production/parameters_training.yaml` 覆蓋 `conf/base/parameters_training.yaml`，兩者的 `training:` 區塊逐層合併。

`--env` 指到不存在的目錄會直接中止：

```
(A30) --env='produciton' points at conf/produciton, which does not exist.
```

這道檢查來自 `src/recsys_tfb/core/consistency.py`。少了它，打錯字的 `--env` 會安靜地整場只用 `conf/base/` 跑完，不會有任何訊息。

## 5. 確認這份程式碼在你的機器上跑得起來

repo 附了合成資料與一組本機 Spark 設定，可以在完全不接自己資料的情況下跑完流程，用來確認拿到的版本是完整的：

```bash
export SPARK_CONF_DIR=$PWD/conf/spark-local
PYTHONPATH=src python scripts/local_spark_setup.py
PYTHONPATH=src python -m recsys_tfb dataset  --env local
PYTHONPATH=src python -m recsys_tfb training --env local
```

`local_spark_setup.py` 會建立本機 warehouse 與 metastore，並把合成 parquet 載入成 `ml_recsys.*` 表；`--reset` 會清掉 warehouse 與 metastore 重建。`--env local` 是本機環境的識別符，`conf/local/` 已經在 repo 裡。

要一次跑完整條鏈（含 inference，並在結尾斷言三張推論表的分區結構），用 `bash scripts/local_e2e.sh`。它預設使用 repo 根目錄的 `.venv/bin/python`，要指定別的直譯器就設 `RECSYS_PYTHON=<python 路徑>`；找不到可執行的直譯器時會直接中止，不會退回系統 python。這支腳本開頭會 `--reset`，本機 warehouse 與 metastore 會被清掉重建。

跑之前先確認**作業系統時區與 `conf/spark-local` 設定的 Spark 時區一致**。兩者不同時，以月份為單位的日期過濾可能篩不到任何資料，而且不會報錯，只會得到 0 列。

完整的本機環境說明見 [local-spark-setup.md](../dev-setup/local-spark-setup.md)。接自己的資料從 [README §3 快速上手](../../../README.md) 開始。

## 6. 升級到下一個版本

```bash
git fetch --tags
git tag                                  # 有哪些新版本
git diff v0.1.0..v0.2.0 -- conf/base/    # 預設設定改了什麼
git checkout v0.2.0
```

中間那一步是升級前最值得花的時間：**設定鍵改名或搬家是升級後最常見的故障來源**。框架不會因為 `conf/<env>/` 裡多了一個沒有人讀的鍵而報錯——那個鍵只是再也沒有作用，該項行為安靜地退回 `conf/base/` 的預設值。所以舊鍵名要自己比對出來。

`git diff v0.1.0..v0.2.0 -- src/` 可以看程式碼改了什麼，但判斷「會不會影響我」通常從設定的差異看得更快。

換版本之後先重跑 §5 確認環境仍然完整，再接自己的資料。

## 7. 舊版本的修正

- 新功能只加在最新版本。
- 舊版本的 bug 修正會放在對應的 `release/*` branch 上，修好後發成新的 patch tag，例如 `v0.1.1`。取用時一樣 checkout tag，不要追 branch。
- 回報問題時附上你使用的 tag 名稱與 `--env` 名稱，兩者決定了實際生效的設定。
