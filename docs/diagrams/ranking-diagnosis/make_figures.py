# -*- coding: utf-8 -*-
"""產生 docs/ranking-diagnosis-framework.md 的 7 張概念示意圖（輸出到本腳本所在目錄）。
全部為合成示意資料，數字對齊手冊貫穿範例（s_H=2.0, s_C=1.5, s_X=0.0；H/Y 年輕退休）。
用法：.venv/bin/python docs/diagrams/ranking-diagnosis/make_figures.py
"""
import os

import numpy as np
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

plt.rcParams["font.family"] = "sans-serif"
plt.rcParams["font.sans-serif"] = ["PingFang TC", "Heiti TC", "Arial Unicode MS"]
plt.rcParams["axes.unicode_minus"] = False
plt.rcParams["figure.dpi"] = 150

OUT = os.path.dirname(os.path.abspath(__file__))


os.makedirs(OUT, exist_ok=True)
rng = np.random.default_rng(42)

POS_COLOR = "#2e7d32"  # 正例綠
NEG_COLOR = "#9e9e9e"  # 負例灰
ACC_COLOR = "#c62828"  # 強調紅
BLUE = "#1565c0"


def auc(pos, neg):
    from itertools import product

    pos = np.asarray(pos)
    neg = np.asarray(neg)
    wins = (pos[:, None] > neg[None, :]).mean()
    ties = (pos[:, None] == neg[None, :]).mean()
    return wins + 0.5 * ties


# ---------------------------------------------------------------- Fig 1
# Alice vs Bob：分子效應
def fig1():
    fig, axes = plt.subplots(1, 2, figsize=(8.4, 3.6))
    cases = [
        ("Alice：只買了 C", [("H  (s=2.0)", False), ("C  (s=1.5)", True), ("X  (s=0.0)", False)],
         "C 的 precision@2 = 1/2 = 0.5\n（上方的 H 是假正例 → 扣分）"),
        ("Bob：H、C 都買了", [("H  (s=2.0)", True), ("C  (s=1.5)", True), ("X  (s=0.0)", False)],
         "C 的 precision@2 = 2/2 = 1.0\n（上方的 H 是真正例 → 不扣分）"),
    ]
    for ax, (title, rows, verdict) in zip(axes, cases):
        ax.set_xlim(0, 10)
        ax.set_ylim(-1.6, 3.6)
        ax.axis("off")
        ax.set_title(title, fontsize=12, pad=8)
        for i, (label, is_pos) in enumerate(rows):
            y = 2.4 - i * 1.1
            color = POS_COLOR if is_pos else NEG_COLOR
            face = "#e8f5e9" if is_pos else "#f5f5f5"
            box = FancyBboxPatch((1.6, y - 0.38), 6.8, 0.78,
                                 boxstyle="round,pad=0.02,rounding_size=0.12",
                                 fc=face, ec=color, lw=1.8)
            ax.add_patch(box)
            ax.text(0.8, y, f"第{i+1}名", ha="center", va="center", fontsize=10, color="#555555")
            ax.text(2.1, y, label, ha="left", va="center", fontsize=11, color="#212121")
            ax.text(8.0, y, "正例" if is_pos else "負例", ha="center", va="center",
                    fontsize=10, color=color, fontweight="bold")
        ax.text(5.0, -0.95, verdict, ha="center", va="center", fontsize=10.5,
                color=ACC_COLOR if "0.5" in verdict else POS_COLOR)
    fig.suptitle("同樣被 H 壓在上方，Alice 受害、Bob 無損：precision 的分子把上方正例也算進去", fontsize=11.5, y=1.02)
    fig.tight_layout()
    fig.savefig(f"{OUT}/fig1-alice-bob-precision.png", bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------- Fig 2
# 2x2 象限地圖
def fig2():
    fig, ax = plt.subplots(figsize=(7.6, 5.6))
    x_split, y_lo, y_hi = 0.62, -0.35, 0.35  # 示意門檻
    ax.axvline(x_split, color="#bbbbbb", lw=1.2, ls="--")
    ax.axhspan(y_lo, y_hi, color="#e3f2fd", alpha=0.5, zorder=0)
    ax.text(0.965, -0.27, "水準大致正確（帶狀區）", ha="right", va="center", fontsize=9, color=BLUE)

    items = [
        # (AUC, 校準差距, 名稱, 顏色, 標註)
        (0.78, 0.05, "item A（健康）", POS_COLOR, "判別力好、水準對"),
        (0.54, -0.08, "冷門 C", BLUE, "餓死型受害者：水準對、\n個人化沒學到（AP 差）"),
        (0.50, 1.6, "item D（常數高分）", ACC_COLOR, "加害者：自己 AP 好，\n把別人全壓一名"),
        (0.80, 1.1, "item F（水準偏高）", "#e65100", "也是加害者：判別力好\n但整體被抬高"),
        (0.55, -1.1, "item E（雙重受害）", "#6a1b9a", "水準偏低＋判別力差：\n正例被整體壓後、又分不出給誰"),
    ]
    for x, y, name, c, note in items:
        ax.scatter([x], [y], s=140, color=c, zorder=3, edgecolor="white", lw=1.5)
        ax.annotate(f"{name}\n{note}", (x, y), textcoords="offset points",
                    xytext=(12, 6), fontsize=9, color=c)
    ax.set_xlim(0.42, 1.0)
    ax.set_ylim(-1.7, 2.1)
    ax.axhline(0, color="#dddddd", lw=0.8)
    ax.set_xlabel("條件判別力：item 內 ROC-AUC（0.5 = 完全分不出該推給誰）", fontsize=10.5)
    ax.set_ylabel("水準：per-item 校準差距（log-odds，實測 − 應有）", fontsize=10.5)
    ax.set_title("兩病灶軸的 2×2 象限：每個 item 各報兩個數、一起看（示意資料）", fontsize=11.5)
    ax.text(0.50, -1.55, "← 判別力差", fontsize=9, color="#888888")
    ax.text(0.88, -1.55, "判別力好 →", fontsize=9, color="#888888")
    fig.tight_layout()
    fig.savefig(f"{OUT}/fig2-quadrant-map.png", bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------- Fig 3
# 現象1：水準偏移（各 item 分數箱型圖 + C 正例線）
def fig3():
    fig, ax = plt.subplots(figsize=(7.2, 4.4))
    data = {
        "熱門 H": rng.normal(2.0, 0.45, 4000),
        "冷門 C": rng.normal(0.7, 0.45, 4000),
        "陪襯 X": rng.normal(0.0, 0.45, 4000),
    }
    bp = ax.boxplot(data.values(), labels=data.keys(), widths=0.5, patch_artist=True,
                    showfliers=False)
    for patch, color in zip(bp["boxes"], [ACC_COLOR, BLUE, NEG_COLOR]):
        patch.set_facecolor(color)
        patch.set_alpha(0.35)
        patch.set_edgecolor(color)
    for med in bp["medians"]:
        med.set_color("#333333")
    ax.axhline(1.5, color=POS_COLOR, ls="--", lw=1.6)
    ax.text(3.42, 1.5, "C 的正例\n分數 ≈ 1.5", va="center", fontsize=9.5, color=POS_COLOR)
    ax.annotate("H 的整體分佈壓在 C 正例之上\n→ 在多數 query 佔第 1 名（現象1）",
                xy=(1.0, 2.05), xytext=(1.55, 2.75), fontsize=10, color=ACC_COLOR,
                arrowprops=dict(arrowstyle="->", color=ACC_COLOR))
    ax.set_ylabel("raw score（log-odds 尺度）", fontsize=10.5)
    ax.set_title("現象1（水準軸）：某 item 的分數整體偏高，壓低其他 item 正例的名次（示意資料）",
                 fontsize=11)
    ax.set_xlim(0.5, 4.4)
    fig.tight_layout()
    fig.savefig(f"{OUT}/fig3-level-shift.png", bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------- Fig 4
# 現象2：item 內正負例分佈——可分 vs 不可分
def fig4():
    fig, axes = plt.subplots(1, 2, figsize=(8.6, 3.8), sharey=True)
    specs = [
        ("條件判別力好（健康）", rng.normal(1.2, 0.55, 3000), rng.normal(0.2, 0.55, 3000)),
        ("條件判別力差（現象2）", rng.normal(0.78, 0.45, 3000), rng.normal(0.68, 0.45, 3000)),
    ]
    for ax, (title, pos, neg) in zip(axes, specs):
        a = auc(pos[:800], neg[:800])
        for arr, color, label in [(neg, NEG_COLOR, "負例（沒買）"), (pos, POS_COLOR, "正例（有買）")]:
            hist, edges = np.histogram(arr, bins=60, range=(-1.6, 3.2), density=True)
            centers = (edges[:-1] + edges[1:]) / 2
            ax.fill_between(centers, hist, alpha=0.45, color=color, label=label)
            ax.plot(centers, hist, color=color, lw=1.4)
        ax.set_title(f"{title}\nitem 內 ROC-AUC ≈ {a:.2f}", fontsize=11)
        ax.set_xlabel("該 item 的 raw score", fontsize=10)
        ax.legend(fontsize=9, loc="upper right")
    axes[0].set_ylabel("密度", fontsize=10)
    fig.suptitle("現象2（條件判別力軸）：同一個 item 的正例 vs 負例分數分佈——重疊＝模型分不出該推給誰（示意資料）",
                 fontsize=11, y=1.04)
    fig.tight_layout()
    fig.savefig(f"{OUT}/fig4-within-item-overlap.png", bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------- Fig 5
# 判讀陷阱二：邊際直方圖抓錯犯人
def fig5():
    n = 3000
    H_young, H_old = rng.normal(3.0, 0.3, n), rng.normal(1.0, 0.3, n)
    Y_young, Y_old = rng.normal(1.2, 0.3, n), rng.normal(1.6, 0.15, n)
    fig, axes = plt.subplots(1, 2, figsize=(9.0, 4.0))

    # 左：邊際（全體混在一起）
    ax = axes[0]
    for arr, color, label in [(np.r_[H_young, H_old], ACC_COLOR, "item H（邊際平均 2.0）"),
                              (np.r_[Y_young, Y_old], BLUE, "item Y（邊際平均 1.4）")]:
        hist, edges = np.histogram(arr, bins=70, range=(0, 4.2), density=True)
        centers = (edges[:-1] + edges[1:]) / 2
        ax.fill_between(centers, hist, alpha=0.4, color=color, label=label)
    ax.axvline(1.5, color=POS_COLOR, ls="--", lw=1.6)
    ax.text(1.56, 0.78, "C 正例\n≈1.5", fontsize=9, color=POS_COLOR)
    ax.annotate("H 在線右側的質量最多\n→ 邊際看 H 是頭號嫌疑犯（錯）",
                xy=(3.0, 0.55), xytext=(2.15, 1.05), fontsize=9.5, color=ACC_COLOR,
                arrowprops=dict(arrowstyle="->", color=ACC_COLOR))
    ax.set_title("邊際分佈（所有 query 混在一起）\n→ H 看起來比較危險", fontsize=11)
    ax.set_xlabel("raw score")
    ax.set_ylabel("密度")
    ax.legend(fontsize=9, loc="upper left")

    # 右：按客群拆開
    ax = axes[1]
    positions = [1, 1.7, 3, 3.7]
    arrays = [H_young, Y_young, H_old, Y_old]
    colors = [ACC_COLOR, BLUE, ACC_COLOR, BLUE]
    bp = ax.boxplot(arrays, positions=positions, widths=0.5, patch_artist=True, showfliers=False)
    for patch, c in zip(bp["boxes"], colors):
        patch.set_facecolor(c)
        patch.set_alpha(0.35)
        patch.set_edgecolor(c)
    ax.axhline(1.5, xmin=0.52, color=POS_COLOR, ls="--", lw=1.6)
    ax.text(4.35, 1.5, "C 正例\n≈1.5", va="center", fontsize=9, color=POS_COLOR)
    ax.set_xticks([1.35, 3.35])
    ax.set_xticklabels(["年輕客群\n（C 沒有買家）", "退休客群\n（C 的買家全在這）"], fontsize=10)
    ax.set_title("按客群拆開\n→ 退休群裡壓過 C 正例的是 Y，不是 H", fontsize=11)
    ax.text(3.0, 0.55, "H", color=ACC_COLOR, fontsize=11, fontweight="bold", ha="center")
    ax.text(3.7, 2.0, "Y", color=BLUE, fontsize=11, fontweight="bold", ha="center")
    fig.suptitle("判讀陷阱：邊際直方圖會抓錯犯人——傷害發生在「受害 item 正例所在的 query」裡（示意資料）",
                 fontsize=11, y=1.03)
    fig.tight_layout()
    fig.savefig(f"{OUT}/fig5-marginal-trap.png", bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------- Fig 6
# offset sweep 分流：水準缺口 vs 條件判別力缺口
def fig6():
    fig, ax = plt.subplots(figsize=(8.2, 3.4))
    ax.set_xlim(0.30, 0.80)
    ax.set_ylim(-0.6, 1.6)
    ax.axis("off")
    y = 0.5
    segs = [
        (0.45, 0.55, BLUE, "offset sweep 收復的部分\n＝水準缺口（不必重訓）"),
        (0.55, 0.70, "#e65100", "offset 收不回的部分\n＝條件判別力缺口（必須動訓練）"),
    ]
    ax.plot([0.45, 0.45], [y - 0.18, y + 0.55], color="#333333", lw=1)
    ax.text(0.45, y + 0.66, "mAP(0) = 0.45\n目前指標", ha="center", fontsize=10)
    for x0, x1, c, label in segs:
        ax.fill_between([x0, x1], y - 0.16, y + 0.16, color=c, alpha=0.45)
        ax.text((x0 + x1) / 2, y - 0.48, label, ha="center", fontsize=9.5, color=c)
    ax.plot([0.55, 0.55], [y - 0.18, y + 0.40], color=BLUE, lw=1)
    ax.text(0.55, y + 0.50, "mAP(δ*) = 0.55", ha="center", fontsize=10, color=BLUE)
    ax.plot([0.70, 0.70], [y - 0.18, y + 0.55], color="#333333", lw=1, ls="--")
    ax.text(0.70, y + 0.66, "0.70 ≈ 可及上限\n（資料層天花板前）", ha="center", fontsize=10)
    ax.annotate("", xy=(0.80, y), xytext=(0.30, y),
                arrowprops=dict(arrowstyle="->", color="#888888", lw=1.2))
    ax.text(0.795, y - 0.30, "macro per-item mAP", ha="right", fontsize=9, color="#888888")
    ax.set_title("診斷項目 6（offset sweep）是框架的分流閥：把指標缺口拆成「水準」與「條件判別力」兩塊（數字為示意）",
                 fontsize=11)
    fig.tight_layout()
    fig.savefig(f"{OUT}/fig6-offset-sweep-split.png", bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------- Fig 7
# 判讀流程總覽
def fig7():
    fig, ax = plt.subplots(figsize=(9.2, 7.6))
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 12.6)
    ax.axis("off")

    def box(x, y, w, h, text, fc, ec, fontsize=9.8, tc="#212121"):
        p = FancyBboxPatch((x - w / 2, y - h / 2), w, h,
                           boxstyle="round,pad=0.02,rounding_size=0.10",
                           fc=fc, ec=ec, lw=1.6)
        ax.add_patch(p)
        ax.text(x, y, text, ha="center", va="center", fontsize=fontsize, color=tc)

    def arrow(x0, y0, x1, y1, color="#555555", ls="-", label=None, lx=0, ly=0):
        ax.annotate("", xy=(x1, y1), xytext=(x0, y0),
                    arrowprops=dict(arrowstyle="->", color=color, lw=1.4, linestyle=ls))
        if label:
            ax.text((x0 + x1) / 2 + lx, (y0 + y1) / 2 + ly, label,
                    ha="center", fontsize=9, color=color)

    GRAY, LB, OR = "#eceff1", "#e3f2fd", "#fff3e0"

    box(5, 11.9, 6.6, 1.0, "第 0 步　建評估基座\n有標籤代表樣本＋macro mAP＋信賴區間（診斷項目 4）", GRAY, "#607d8b")
    arrow(5, 11.4, 5, 10.75)
    box(5, 10.1, 6.6, 1.1, "第 1 步　配置對帳（診斷項目 1、2）\n採樣 $r_j$／權重 $w_j$ 的理論偏移 vs 實測校準差距", GRAY, "#607d8b")
    box(8.75, 10.1, 2.3, 1.35, "對得上 →\nlogQ／offset 修正\n（槓桿 1，閉式）\n修完回第 0 步重量", LB, BLUE, fontsize=9)
    arrow(6.3, 10.1, 7.6, 10.1)
    arrow(5, 9.55, 5, 8.9, label="殘餘差距帶著走", lx=1.55, ly=0.02)

    box(5, 8.3, 5.4, 1.0, "第 2 步　offset sweep 分流（診斷項目 6）\n指標缺口有多少能被 per-item 常數收復？", "#ede7f6", "#5e35b1")
    arrow(3.6, 7.8, 2.6, 7.0, color=BLUE, label="收復得多", lx=-0.95, ly=0.12)
    arrow(6.4, 7.8, 7.4, 7.0, color="#e65100", label="收不回", lx=0.85, ly=0.12)

    box(2.45, 6.4, 4.1, 1.05, "水準問題（不必重訓）\n→ 指標決策（Ch 6）", LB, BLUE)
    arrow(2.45, 5.85, 2.45, 5.2, color=BLUE)
    box(2.45, 4.55, 4.1, 1.15, "處方：常態化 offset\n或 item-aware weight（槓桿 2）", LB, BLUE)

    box(7.55, 6.4, 4.3, 1.05, "條件判別力問題（必須動訓練）\n第 3 步　定位（診斷項目 3、8、9）", OR, "#e65100")
    arrow(6.6, 5.85, 5.95, 5.2, color="#e65100")
    arrow(8.5, 5.85, 9.0, 5.2, color="#e65100")
    box(5.9, 4.5, 3.2, 1.25, "餓死型\n（沒分到切點預算）\n→ 加權／欠採／HPO 先驗\n（槓桿 3）", OR, "#e65100", fontsize=9.2)
    box(9.05, 4.5, 1.85, 1.25, "特徵缺失型\n→ 補特徵\n（槓桿 5）", OR, "#e65100", fontsize=9.2)

    box(5, 2.3, 8.6, 1.3,
        "每輪動完訓練槓桿 → 回第 0 步重量。\n注意：加權／欠採會同時搬動水準（手冊3 Ch8），所以第 1、2 步要重走",
        "#fbe9e7", ACC_COLOR, fontsize=10)
    arrow(5.9, 3.85, 5.4, 3.0, color=ACC_COLOR, ls="--")
    arrow(2.45, 3.95, 3.4, 3.0, color=ACC_COLOR, ls="--")

    ax.set_title("判讀流程總覽（Ch 4.1 的圖形版）：診斷項目 → 判讀 → 槓桿", fontsize=12, pad=10)
    fig.tight_layout()
    fig.savefig(f"{OUT}/fig7-triage-flow.png", bbox_inches="tight")
    plt.close(fig)


for f in (fig1, fig2, fig3, fig4, fig5, fig6, fig7):
    f()
    print("done:", f.__name__)
print("all figures written to", OUT)
