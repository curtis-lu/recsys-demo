"""離線重繪診斷頁：把 ``<診斷名>.json`` 就地畫成 HTML，不碰 Spark、不做計算。

為什麼有這支工具
================
版面要改對，得先看過真實資料畫出來的樣子；而真實資料只在公司環境裡。若每次
調一下欄位順序都要重跑一次公司環境的 evaluation，review 迴圈的成本會高到沒人
願意提第二輪意見。

各項診斷的 ``compute`` 輸出已經以 JSON 落在 ``diagnosis/`` 目錄裡，那份 JSON
就是計算與呈現之間的中介層。有了它，迴圈變成：**公司環境真跑一次 → 把那包
JSON 拷回本機 → 秒級重繪**（實測整包約 1.5 秒）。第一次真跑之後的所有版面
迭代都不必再進公司環境。

**這是它唯一的目的。** 它不是另一個報表產生器：不做計算、不連 Spark、不讀
predictions。輸入是 JSON，輸出是 HTML，中間只呼叫既有的組裝函式。

用法
====
::

    python scripts/render_diagnosis.py \\
        --input-dir data/evaluation/<mv>/<snap>/diagnosis \\
        --output-dir /tmp/rerender

``--input-dir`` 就是公司環境那包拷回來的目錄。缺哪一項診斷的 JSON 就跳過哪
一項（stderr 會列出來），因為公司環境可能只跑了部分診斷，使用者也可能只拷了
一部分回來——缺一項就整包失敗的話，最常見的使用情境反而不能用。

刻意不做：``display`` config 機制
=================================
計畫原稿（Task 2.7）要求另做一層 ``display`` config：用 YAML 覆寫表格的欄位
順序與欄名，並配兩條測試（``test_display_config_override_changes_column_order``、
``test_fails_loud_on_unknown_display_column``）。**這裡刻意不做，不是漏做。**

理由：中介層真正要換到的性質是「JSON 持久化、呈現隨時可改、不必重跑公司
環境」——重繪只要 1.5 秒，這個性質已經拿到了。再疊一層 display config 的話，
每個旋鈕都是一份程式碼＋一條測試＋一段文件，而且**只調得動事先想到的那幾
樣**；直接改 ``_render.py`` 再重繪則什麼都能調，成本一樣是 1.5 秒。也就是說
這層抽象付出額外維護成本，換到的能力嚴格少於它想取代的做法。

等使用者在公司環境看過真實產出、明確說出要調哪幾樣，再把那幾個旋鈕做成
config 才划算——那時才知道旋鈕該長什麼樣。

不 import pyspark
=================
重繪必須在沒有 Spark 的機器上跑得動（使用者可能在自己的筆電上調版面）。整條
import 鏈（``report_builder`` → 各診斷 ``render`` → ``report.pages``）都是純
pandas／plotly，這裡不得把 Spark 拉進來。
``tests/scripts/test_render_diagnosis.py`` 有兩條測試守這件事。
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

DEFAULT_PARAMS = Path("conf/base/parameters_evaluation.yaml")


def load_parameters(path) -> dict:
    """讀 parameters YAML；讀不到就回 ``{}``，不 raise。

    為什麼不 fail loud：``render(result, parameters)`` 的簽章要求這個參數，但
    多數 ``render`` 根本不看它（``config_shift.render`` 完全沒用到）。為了一個
    多半用不上的參數而讓整個重繪失敗，會直接抵銷「隨手可用」這個唯一賣點——
    而重繪的產物是拿來看版面的，不是拿來做決策的，讀不到 config 的下場最多是
    某段說明文字缺一個值，不是算出錯的數字。
    """
    import yaml

    try:
        text = Path(path).read_text(encoding="utf-8")
    except OSError as exc:
        print(f"[render_diagnosis] parameters 讀不到，改用空 dict：{exc}",
              file=sys.stderr)
        return {}
    return yaml.safe_load(text) or {}


def load_results(input_dir) -> tuple[dict, list[str], list[str]]:
    """依 ``DIAGNOSES`` 的順序讀 ``<input-dir>/<name>.json``。

    Returns:
        ``(results, missing, unknown)``——``results`` 直接餵給
        ``assemble_diagnosis_pages``；``missing`` 是 registry 有、目錄裡沒有的
        診斷名；``unknown`` 是目錄裡有、registry 沒有的 JSON 檔名。後兩者都由
        呼叫端印到 stderr，方向相反但目的相同：不讓「沒處理」看起來像「沒問題」。

    這裡用 ``contract.DIAGNOSES``（模組屬性）而不是 ``from … import DIAGNOSES``：
    組裝層也是在呼叫當下讀同一個屬性，兩邊看到的 registry 才保證是同一份。
    """
    from recsys_tfb.diagnosis.metric import contract

    input_dir = Path(input_dir)
    results: dict = {}
    missing: list[str] = []
    # 目錄裡有、但不在 registry 的 JSON。與 missing 是相反方向的同一件事：
    # 不要讓「沒處理」看起來像「沒問題」。使用者拷回來的是整個 diagnosis/
    # 目錄，過渡期裡面還有 metric_ci.json／offset_sweep.json／pair_ledger.json
    # 這些尚未進 registry 的既有診斷——拷了 4 份只看到 1 頁而畫面一片安靜，
    # 讀起來像工具壞了。
    unknown = sorted(
        p.stem for p in input_dir.glob("*.json")
        if p.stem not in contract.DIAGNOSES
    )
    for name in contract.DIAGNOSES:
        path = input_dir / f"{name}.json"
        if not path.exists():
            missing.append(name)
            continue
        results[name] = json.loads(path.read_text(encoding="utf-8"))
    return results, missing, unknown


def main(argv=None) -> list[Path]:
    """解析 argv、讀 JSON 與 params、呼叫組裝層、回傳寫出的檔案路徑。

    **本函式刻意不自己組 ``Page``、不自己算 slug、不自己填 ``SCOPE.sampling``。**
    那四件事 ``report_builder.assemble_diagnosis_pages`` 已經做了，pipeline 走的
    也是它。在這裡抄一份的話兩份會漂移，而漂移的症狀是「公司環境跑出來的頁面」
    與「本機重繪出來的頁面」長得不一樣——那正好摧毀這支工具的全部價值，因為
    使用者會以為自己在看同一份東西。
    ``test_output_matches_pipeline_generated_pages`` 守的就是這條。
    """
    parser = argparse.ArgumentParser(
        description="把診斷 JSON 離線重繪成 HTML（不需要 Spark）")
    parser.add_argument(
        "--input-dir", required=True, type=Path,
        help="放 <診斷名>.json 的目錄（公司環境 diagnosis/ 拷回來的那包）")
    parser.add_argument(
        "--output-dir", required=True, type=Path, help="HTML 輸出目錄")
    parser.add_argument(
        "--params", default=DEFAULT_PARAMS, type=Path,
        help=f"parameters YAML（預設 {DEFAULT_PARAMS}；讀不到則用空 dict）")
    args = parser.parse_args(argv)

    results, missing, unknown = load_results(args.input_dir)

    if unknown:
        print(f"[render_diagnosis] 目錄裡有 {len(unknown)} 份不在 registry 的 "
              f"JSON，不會產生頁面：{', '.join(unknown)}", file=sys.stderr)

    for name in missing:
        # 跳過要看得見：靜靜少一頁的話，使用者會把「這項沒拷回來」讀成
        # 「這項沒問題」——那兩件事的結論完全相反。
        print(f"[render_diagnosis] 跳過 {name}："
              f"{args.input_dir / (name + '.json')} 不存在",
              file=sys.stderr)

    parameters = load_parameters(args.params)

    from recsys_tfb.evaluation.report_builder import assemble_diagnosis_pages

    written = assemble_diagnosis_pages(results, parameters, args.output_dir)

    # 頁數與 result 數的落差＝有診斷的 render 回了空 tuple（該項停用）。這裡
    # 只報數字不點名：要點名就得在 script 裡重算 slug，而那正是上面說的
    # 「抄一份組裝邏輯」的第一步。
    n_pages = len(written) - 2 if written else 0   # 扣掉 plotly.min.js 與 index
    print(f"[render_diagnosis] {len(results)} 份 JSON → {n_pages} 頁 "
          f"（{len(written)} 個檔案）→ {args.output_dir}")
    if n_pages < len(results):
        print(f"[render_diagnosis] 有 {len(results) - n_pages} 項的 render 回空 "
              f"tuple（該項停用），沒有產生頁面", file=sys.stderr)
    return written


if __name__ == "__main__":
    main()
