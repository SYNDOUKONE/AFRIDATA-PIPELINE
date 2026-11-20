"""Génère les captures du dashboard BI (3 pages) à partir des tables Gold du lake.

Usage :
    python dashboard/generate_dashboard.py [--lake data/lake_demo] [--run-1280px?]
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

plt.rcParams.update({
    "figure.facecolor": "#0f1526",
    "axes.facecolor": "#0f1526",
    "savefig.facecolor": "#0f1526",
    "text.color": "#e8ecf5",
    "axes.labelcolor": "#e8ecf5",
    "xtick.color": "#aab3c5",
    "ytick.color": "#aab3c5",
    "axes.edgecolor": "#2a3350",
    "grid.color": "#232b45",
    "font.size": 10,
})

ACCENT = "#34d399"
ORANGE = "#f59e0b"
BLUE = "#60a5fa"
RED = "#f87171"
PINK = "#f472b6"
PALETTE = ["#34d399", "#60a5fa", "#f59e0b", "#f472b6", "#a78bfa", "#f87171"]


def read(lake: Path, table: str) -> pd.DataFrame:
    return pd.read_parquet(lake / "gold" / table)


def kpi_card(ax, title, value, sub, color=ACCENT):
    ax.axis("off")
    rect = plt.Rectangle((0.02, 0.08), 0.96, 0.84, transform=ax.transAxes,
                         fc="#161d33", ec="#2a3350", lw=1, zorder=0)
    ax.add_patch(rect)
    ax.text(0.5, 0.78, title, transform=ax.transAxes, ha="center", fontsize=11, color="#aab3c5")
    ax.text(0.5, 0.46, value, transform=ax.transAxes, ha="center", fontsize=24, fontweight="bold", color=color)
    ax.text(0.5, 0.18, sub, transform=ax.transAxes, ha="center", fontsize=10, color="#7c8aa8")


def money(v):
    if v >= 1e9:
        return f"{v/1e9:.2f} Md"
    if v >= 1e6:
        return f"{v/1e6:.1f} M"
    if v >= 1e3:
        return f"{v/1e3:.0f} k"
    return f"{v:.0f}"


def page1(lake: Path, out: Path) -> None:
    kpi = read(lake, "kpi_overview").iloc[0]
    daily = read(lake, "agg_daily_global").sort_values("tx_date")
    ops = read(lake, "agg_daily_operator").groupby("operator", as_index=False).agg(
        tx_count=("tx_count", "sum"), total_amount=("total_amount", "sum"),
        failed=("failed_count", "sum"), fees=("total_fees", "sum"))

    fig = plt.figure(figsize=(14, 7.5), dpi=150)
    gs = fig.add_gridspec(3, 4, hspace=0.55, wspace=0.3,
                          left=0.05, right=0.97, top=0.86, bottom=0.07)
    fig.suptitle("AFRIDATA PIPELINE — Vue d'ensemble des transactions mobile money", fontsize=15,
                 fontweight="bold", y=0.95)

    kpi_card(fig.add_subplot(gs[0, 0]), "Volume total", f"{int(kpi['total_transactions']):,}", "transactions nettoyées")
    kpi_card(fig.add_subplot(gs[0, 1]), "Montant transféré", f"{money(kpi['total_amount'])} XOF", "somme des montants", BLUE)
    kpi_card(fig.add_subplot(gs[0, 2]), "Frais collectés", f"{money(kpi['total_fees'])} XOF", "revenus opérateurs", ORANGE)
    kpi_card(fig.add_subplot(gs[0, 3]), "Taux d'échec", f"{float(kpi['fail_rate'])*100:.2f} %",
             f"{int(kpi['total_failed']):,} échecs", RED)

    ax = fig.add_subplot(gs[1, :2])
    ax.plot(daily["tx_date"].astype(str).astype("datetime64[ns]"), daily["tx_count"], color=ACCENT, lw=1.4)
    ax.set_title("Transactions par jour", loc="left", fontweight="bold")
    ax.grid(alpha=0.35)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v/1e3:.0f}k"))

    ax = fig.add_subplot(gs[1, 2:])
    cum = daily["total_amount"].cumsum()
    ax.fill_between(range(len(cum)), cum, color=BLUE, alpha=0.35)
    ax.plot(range(len(cum)), cum, color=BLUE, lw=1.4)
    ax.set_title("Cumul des montants transférés (XOF)", loc="left", fontweight="bold")
    ax.grid(alpha=0.35)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: money(v)))
    ax.set_xticks([0, len(cum) - 1])
    ax.set_xticklabels([str(daily["tx_date"].iloc[0]), str(daily["tx_date"].iloc[-1])])

    ax = fig.add_subplot(gs[2, 0])
    wedges, _, autotexts = ax.pie(ops["tx_count"], colors=PALETTE, autopct="%1.1f%%",
                                  startangle=100, pctdistance=0.72,
                                  wedgeprops={"width": 0.42, "edgecolor": "#0f1526"})
    for t in autotexts:
        t.set_fontsize(8)
    ax.legend(wedges, ops["operator"], loc="lower center", bbox_to_anchor=(0.5, -0.38),
              ncol=1, fontsize=8, frameon=False)
    ax.set_title("Part par opérateur", loc="left", fontweight="bold")

    ax = fig.add_subplot(gs[2, 1:])
    daily_ops = read(lake, "agg_daily_operator").pivot_table(index="tx_date", columns="operator",
                                                             values="tx_count", aggfunc="sum").fillna(0).sort_index()
    bottom = None
    for i, col in enumerate(daily_ops.columns):
        ax.bar(daily_ops.index.astype(str).astype("datetime64[ns]"), daily_ops[col], bottom=bottom,
               color=PALETTE[i % len(PALETTE)], width=0.9, label=col)
        bottom = daily_ops[col] if bottom is None else bottom + daily_ops[col]
    ax.set_title("Volume journalier empilé par opérateur", loc="left", fontweight="bold")
    ax.grid(alpha=0.35, axis="y")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v/1e3:.0f}k"))
    ax.legend(fontsize=8, frameon=False, ncol=4, loc="upper left")

    out.mkdir(parents=True, exist_ok=True)
    fig.savefig(out / "page1_overview.png", bbox_inches="tight")
    plt.close(fig)


def page2(lake: Path, out: Path) -> None:
    anoms = pd.read_parquet(lake / "gold" / "anomalies")
    suspects = read(lake, "agg_suspects")

    fig = plt.figure(figsize=(14, 7.5), dpi=150)
    gs = fig.add_gridspec(2, 6, hspace=0.5, wspace=0.45,
                          left=0.05, right=0.97, top=0.84, bottom=0.09)
    fig.suptitle("AFRIDATA PIPELINE — Détection d'anomalies (règles statistiques + Isolation Forest)",
                 fontsize=15, fontweight="bold", y=0.95)

    rules = anoms["anomaly_rules"].str.split("|").explode()
    rules = rules[rules != ""].value_counts()
    ia = int((anoms["if_label"] == "ANOMALY_IA").sum())
    if "extreme_amount" not in rules.index:
        rules["extreme_amount"] = int(anoms["is_high_amount"].sum())

    kpi_card(fig.add_subplot(gs[0, 0:2]), "Transactions suspectes", f"{len(anoms):,}",
             f"isolation forest : {ia} alertes IA", PINK)

    ax = fig.add_subplot(gs[0, 2:4])
    bars = ax.barh(list(rules.index) + ["isolation_forest"], list(rules.values) + [ia],
                   color=[PALETTE[i % len(PALETTE)] for i in range(len(rules) + 1)])
    ax.set_title("Alertes par règle", loc="left", fontweight="bold")
    ax.invert_yaxis()
    ax.grid(alpha=0.35, axis="x")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f}"))

    ax = fig.add_subplot(gs[0, 4:])
    by_day = suspects.groupby("tx_date")["suspect_count"].sum().sort_index()
    ax.fill_between(by_day.index.astype(str).astype("datetime64[ns]"), by_day, color=RED, alpha=0.3)
    ax.plot(by_day.index.astype(str).astype("datetime64[ns]"), by_day, color=RED, lw=1.2)
    ax.set_title("Suspects par jour", loc="left", fontweight="bold")
    ax.grid(alpha=0.35)
    ax.tick_params(axis="x", labelsize=7)

    ax = fig.add_subplot(gs[1, :2])
    ax.hist(anoms["amount"], bins=40, color=PINK, alpha=0.8)
    ax.set_title("Distribution des montants suspects (XOF)", loc="left", fontweight="bold")
    ax.grid(alpha=0.35, axis="y")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: money(v)))

    ax = fig.add_subplot(gs[1, 2:4])
    ops_susp = anoms["operator"].value_counts()
    ax.bar(ops_susp.index, ops_susp.values, color=PALETTE[1], alpha=0.85)
    ax.set_title("Suspects par opérateur", loc="left", fontweight="bold")
    ax.grid(alpha=0.35, axis="y")
    ax.tick_params(axis="x", labelsize=7, rotation=18)
    for label in ax.get_xticklabels():
        label.set_ha("right")

    ax = fig.add_subplot(gs[1, 4:])
    ax.scatter(range(len(anoms)), anoms["if_score"], s=6, c=anoms["amount"], cmap="viridis")
    ax.set_title("Score Isolation Forest (couleur = montant)", loc="left", fontweight="bold")
    ax.set_xticks([])
    ax.grid(alpha=0.35)

    out.mkdir(parents=True, exist_ok=True)
    fig.savefig(out / "page2_anomalies.png", bbox_inches="tight")
    plt.close(fig)


def page3(lake: Path, out: Path) -> None:
    hourly = read(lake, "agg_hourly_pattern")
    regions = read(lake, "agg_region").groupby("sender_region", as_index=False)[["tx_count", "total_amount"]].sum()
    types = read(lake, "agg_tx_type_channel").groupby("tx_type", as_index=False)[["tx_count", "total_amount"]].sum()
    failures = read(lake, "agg_daily_operator")
    failures = failures.groupby("tx_date", as_index=False).agg(tx=("tx_count", "sum"), failed=("failed_count", "sum"))
    failures["fail_rate"] = failures["failed"] / failures["tx"]

    fig = plt.figure(figsize=(14, 7.5), dpi=150)
    gs = fig.add_gridspec(2, 6, hspace=0.55, wspace=0.5,
                          left=0.05, right=0.97, top=0.84, bottom=0.1)
    fig.suptitle("AFRIDATA PIPELINE — Comportement des utilisateurs", fontsize=15, fontweight="bold", y=0.95)

    ax = fig.add_subplot(gs[0, :3])
    pivot = hourly.pivot_table(index="tx_hour", columns="channel", values="tx_count", aggfunc="sum").fillna(0)
    im = ax.imshow(pivot.T, aspect="auto", cmap="viridis")
    ax.set_title("Activité : heure x canal", loc="left", fontweight="bold")
    ax.set_xticks(range(0, len(pivot.index), 3))
    ax.set_xticklabels([f"{pivot.index[h]}h" for h in range(0, len(pivot.index), 3)], fontsize=7)
    ax.set_yticks(range(len(pivot.columns)))
    ax.set_yticklabels(pivot.columns, fontsize=8)
    fig.colorbar(im, ax=ax, fraction=0.03, pad=0.02)

    ax = fig.add_subplot(gs[0, 3:])
    reg = regions.sort_values("tx_count", ascending=True)
    ax.barh(reg["sender_region"], reg["tx_count"], color=BLUE, alpha=0.85)
    ax.set_title("Transactions par région (Côte d'Ivoire)", loc="left", fontweight="bold")
    ax.grid(alpha=0.35, axis="x")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v/1e3:.0f}k"))

    ax = fig.add_subplot(gs[1, :2])
    tp = types.sort_values("total_amount", ascending=True)
    ax.barh(tp["tx_type"], tp["total_amount"], color=PALETTE[4], alpha=0.9)
    ax.set_title("Montant par type (XOF)", loc="left", fontweight="bold")
    ax.grid(alpha=0.35, axis="x")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: money(v)))

    ax = fig.add_subplot(gs[1, 2:4])
    ax.plot(failures["tx_date"].astype(str).astype("datetime64[ns]"), failures["fail_rate"] * 100,
            color=ORANGE, lw=1.3)
    ax.axhline(5, color=RED, ls="--", lw=0.9)
    ax.text(0.99, 0.95, "seuil alerte 5%", transform=ax.transAxes, ha="right", fontsize=8, color=RED)
    ax.set_title("Taux d'échec quotidien (%)", loc="left", fontweight="bold")
    ax.grid(alpha=0.35)
    ax.tick_params(axis="x", labelsize=7)

    ax = fig.add_subplot(gs[1, 4:])
    ab = read(lake, "agg_tx_type_channel").groupby("amount_bucket", as_index=False)["tx_count"].sum()
    ax.bar(ab["amount_bucket"], ab["tx_count"], color=ACCENT, alpha=0.9)
    ax.set_title("Répartition par tranche de montant", loc="left", fontweight="bold")
    ax.grid(alpha=0.35, axis="y")
    ax.tick_params(axis="x", labelsize=6.5, rotation=30)
    for label in ax.get_xticklabels():
        label.set_ha("right")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v/1e3:.0f}k"))

    out.mkdir(parents=True, exist_ok=True)
    fig.savefig(out / "page3_behavior.png", bbox_inches="tight")
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--lake", default="data/lake_demo")
    parser.add_argument("--out", default="dashboard/img")
    args = parser.parse_args()

    lake = Path(args.lake)
    out = Path(args.out)
    page1(lake, out)
    print(f"OK page1_overview.png")
    page2(lake, out)
    print(f"OK page2_anomalies.png")
    page3(lake, out)
    print(f"OK page3_behavior.png")


if __name__ == "__main__":
    main()
