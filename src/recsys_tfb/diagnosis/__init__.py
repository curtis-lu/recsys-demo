"""診斷域 library：排序模型的「現象 → 成因」診斷。

**本套件只忠實呈現資料與其邊界，不產生判定、不給建議動作、不用門檻把連續量
切成類別——判斷是讀者的工作。** 這是設計不變量，不是風格偏好：曾經存在的
``triage``（per-item 判定＋建議槓桿）與 ``quadrant``（AUC 門檻切象限）就是
因為違反它而整層退場。新增模組前先確認自己沒有在重蹈那條路。

- ``diagnosis.model``  — 模型結構層診斷（SHAP、importance、feature stats、
  象限選樣與案例、gain ledger）。訓練 pipeline 的薄 node 呼叫它。
- ``diagnosis.metric`` — 指標層診斷，跑在共用診斷抽樣上。評估 pipeline 的薄
  node 呼叫它。（此處刻意不列舉子模組：清單增刪頻繁，列了必然與實況漂移。）

依賴方向（單向，違反即錯，見 spec §1 不變量 4）：
``pipelines/* → diagnosis → core / evaluation(僅 numpy 原語 metrics.py) / io / utils``；
本套件不得 import 任何 ``pipelines/*``。
框架方法論見 docs/ranking-diagnosis-framework.md。
"""
