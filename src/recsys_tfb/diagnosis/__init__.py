"""診斷域 library：排序模型的「現象 → 成因 → 槓桿」診斷。

- ``diagnosis.model``  — 模型結構層診斷（SHAP、importance、feature stats、
  象限選樣與案例；Phase 5 的 gain ledger）。訓練 pipeline 的薄 node 呼叫它。
- ``diagnosis.metric`` — 指標層診斷（Phase 1–5 陸續進駐：抽樣、CI、對帳、
  判別力、象限、offset sweep、成對帳本、triage）。評估 pipeline 的薄 node 呼叫它。

依賴方向（單向，違反即錯，見 spec §1 不變量 4）：
``pipelines/* → diagnosis → core / evaluation(僅 numpy 原語 metrics.py) / io / utils``；
本套件不得 import 任何 ``pipelines/*``。
框架方法論見 docs/ranking-diagnosis-framework.md。
"""
