"""config_shift：抽樣／sample weight 的理論 log-odds 偏移對排序的影響。

實作放在私有模組 ``_compute``／``_render``（與 repo 既有的 ``_common.py``／
``_spark.py`` 同慣例）。**不要**把它們命名成 ``compute.py``／``render.py``：
契約要求本套件匯出叫 ``compute``／``render`` 的**函式**，同名子模組會被
``from .compute import compute`` 直接遮蔽掉，``pkg.compute`` 從此指向函式而不是
模組——而 ``check_module`` 走 ``getattr`` 剛好拿得到那個函式，所以契約測試
**抓不到**這個遮蔽。

契約要求的五個符號在這裡集合：``NAME``／``TITLE``／``SCOPE`` 定義於本檔，
``compute``／``render`` 由私有模組實作後 re-export。``SCOPE`` 定義在這裡而不是
``_render`` 裡，是因為它屬於「這項診斷是什麼」而不是「怎麼畫」——換一套呈現
不該讓範圍說明跟著搬家。
"""
from __future__ import annotations

from recsys_tfb.diagnosis.metric.config_shift._compute import compute
from recsys_tfb.diagnosis.metric.config_shift._render import render
from recsys_tfb.report import ScopeNote

NAME = "config_shift"
TITLE = "配置引入的排序偏移"

#: 這項診斷量什麼、算在哪批列上、**不能**推論什麼。
#:
#: ``blind_to`` 是這份 SCOPE 的重點：Δ ≈ 0 會被讀成「可以把配置這個方向整個
#: 排除掉」，而下面每一條都是那個推論不成立的具體情形。
#:
#: ``sampling`` 刻意留空：它是每次執行才知道的事實，寫進模組層級的常數會讓
#: import 到的 SCOPE 帶著上一次執行的抽樣描述。填值的是**組裝層**
#: （Task 2.5 的 ``assemble_diagnosis_pages``）——五項診斷共用同一份
#: ``diagnosis_sample``、``sampling_description`` 永遠在 ``result["sample_meta"]``
#: 同一個位置，所以那裡一句 ``dataclasses.replace`` 就涵蓋全部。**不要在每項
#: 診斷各放一個填值 helper**：五份做同一件事的程式碼只會一起漂移。
SCOPE = ScopeNote(
    measures=(
        "抽樣比例與 sample weight 在理論上對每個 (context 群, item) 引入的 "
        "log-odds 偏移，以及把它扣掉之後 macro per-item mAP 的變化量 Δ。"
    ),
    population="診斷抽樣：只含有正例的 query（macro mAP 只在這些 query 上累積）。",
    blind_to=(
        "偏移是否真的被模型吸收——這裡算的是理論值，不是從模型參數量出來的。",
        "Σ Δ_j ≠ Δ：逐 item 的 Δ_j 是替換實驗，名次互相耦合，不可相加。",
        "Δ 只反映『扣掉理論 offset』這一種操作的效果，不代表配置的全部影響。",
        "當 context 欄在每個 query 內為常數（entity 級屬性）時，同一群內所有 item 的 "
        "offset 同加一個常數則 Δ 完全不變——此前提下 Δ 量不到偏移的絕對水準，只量得到 "
        "item 之間的差。context 欄取自 item 級屬性（產品層級／類別）時前提不成立，"
        "改看 query_offset_spread。",
        "Δ 的推導前提是 pointwise 機率型 objective；objective 為 lambdarank 等非機率型時"
        "分數是無界原始分數，log-odds offset 的相減沒有理論基礎（此時 notes 會點名）。",
        "沒有出現在樣本中的 (context, item) 組合——offset 矩陣只列實際觀測到的組合。",
    ),
    reference_points=(
        "群內 spread = 0 代表該 context 群內 offset 均勻，對 query 內名次零影響"
        "（可直接推導，不需估計）。",
        "Δ 的 95% CI 來自配對 bootstrap：同一組重抽的 entity 上同時算 mAP(F) 與 "
        "mAP(F−offset) 再取差。",
    ),
)

__all__ = ["NAME", "TITLE", "SCOPE", "compute", "render"]
