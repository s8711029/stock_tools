"""
tw_stock_today_filter.py
依據回塑性分析（2026-05-12）發現的規則，掃描今日全市場約 1967 檔上市+上櫃，
列出「今日 K 棒符合 4 條買進規則任一」的股票，並標註起漲前型態。
取前 10 名（依強度排序）寄送 Email。
"""

import json
import os
import sys
import io
import smtplib
import time
import warnings
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from datetime import datetime, timedelta
from collections import Counter

import numpy as np
import pandas as pd
import yfinance as yf

try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
except Exception:
    pass

warnings.filterwarnings("ignore")

EMAIL_CFG  = r"C:\Users\s8711\OneDrive\桌面\stock_email_config.json"
REPORT_DIR = r"C:\Users\s8711\OneDrive\桌面\stock_reports"
RUN_DATE   = datetime.now().strftime("%Y-%m-%d")
TOP_N      = 10
BATCH_SIZE = 50
RECIPIENTS = ["wic0935@gmail.com", "chenyichieh70@gmail.com"]

# 共用 screener 的清單抓取
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from tw_stock_screener_v2 import fetch_twse_list, fetch_tpex_list


# ── 指標計算 ──────────────────────────────────────────────────────────────────
def add_indicators(df):
    df = df.copy()
    df["ma5"]  = df["close"].rolling(5).mean()
    df["ma10"] = df["close"].rolling(10).mean()
    df["ma20"] = df["close"].rolling(20).mean()
    df["ma60"] = df["close"].rolling(60).mean()
    df["vol_ma20"] = df["volume"].rolling(20).mean()
    low9  = df["low"].rolling(9).min()
    high9 = df["high"].rolling(9).max()
    rng = (high9 - low9).replace(0, np.nan)
    rsv = ((df["close"] - low9) / rng * 100).fillna(50)
    k_vals = [50.0]; d_vals = [50.0]
    for r in rsv.values[1:]:
        k = (2/3) * k_vals[-1] + (1/3) * float(r)
        d = (2/3) * d_vals[-1] + (1/3) * k
        k_vals.append(k); d_vals.append(d)
    df["k9"] = k_vals
    df["d9"] = d_vals
    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss.replace(0, np.nan)
    df["rsi14"] = 100 - 100 / (1 + rs)
    return df


# ── 4 條買進規則 (套用「今日」) ────────────────────────────────────────────────
def check_rules_today(df):
    n = len(df)
    if n < 25:
        return []
    i = n - 1
    row  = df.iloc[i]
    prev = df.iloc[i - 1] if i > 0 else None
    triggered = []

    if pd.isna(row["ma5"]) or pd.isna(row["ma20"]) or pd.isna(row["vol_ma20"]):
        return []

    # Rule 1
    if i >= 3:
        past3 = df.iloc[i - 3:i]
        if (past3["volume"] < past3["vol_ma20"]).all():
            if (row["volume"] > row["vol_ma20"] * 1.8
                    and row["close"] > row["open"]
                    and row["close"] > row["ma5"]):
                triggered.append("①量縮整理後爆量站5MA")

    # Rule 2
    if (prev is not None and pd.notna(prev["k9"]) and pd.notna(row["k9"])
            and pd.notna(row["ma10"])):
        if (prev["k9"] < 20 and prev["k9"] < prev["d9"]
                and row["k9"] > row["d9"]
                and row["close"] > row["ma10"]):
            triggered.append("②KD低檔黃金交叉站10MA")

    # Rule 3
    if i >= 5:
        past5 = df.iloc[i - 5:i]
        broke = (past5["low"] < past5["ma20"]).any()
        if (broke and row["low"] >= row["ma20"] * 0.98
                and row["close"] > row["open"]
                and row["volume"] < row["vol_ma20"]):
            triggered.append("③縮量回測20MA不破紅K")

    # Rule 4
    if i >= 20:
        recent_high = float(df.iloc[i - 20:i]["high"].max())
        if (row["close"] > recent_high
                and row["volume"] > row["vol_ma20"] * 1.5
                and row["close"] > row["open"]):
            triggered.append("④突破20日高點帶量")

    return triggered


# ── 起漲前型態（含今日，往前 20 日結構） ──────────────────────────────────────
def classify_pattern_today(df):
    n = len(df)
    if n < 30:
        return "資料不足"
    i = n - 1
    pre = df.iloc[max(0, i - 20):i]
    pre60 = df.iloc[max(0, i - 60):i]
    if pre.empty:
        return "其他"
    high_pre = float(pre["high"].max())
    low_pre  = float(pre["low"].min())
    close_today = float(df.iloc[i]["close"])
    if low_pre <= 0:
        return "其他"
    range_pct = (high_pre - low_pre) / low_pre * 100

    if len(pre60) >= 30:
        top60 = float(pre60["high"].max())
        if top60 > 0 and (top60 - close_today) / top60 > 0.20:
            return "跌深反彈"

    if range_pct < 15 and close_today >= high_pre * 0.99:
        return "箱型突破"

    if range_pct > 15:
        low_idx_pos = int(pre["low"].values.argmin())
        if low_idx_pos < len(pre) - 1:
            rebound = (close_today - low_pre) / low_pre * 100
            if rebound > 5:
                return "W底/V反"

    ma20_now = df.iloc[i]["ma20"]
    ma20_5d  = df.iloc[max(0, i - 5)]["ma20"]
    if pd.notna(ma20_now) and pd.notna(ma20_5d) and ma20_now > ma20_5d:
        if pre.iloc[-1]["low"] <= ma20_now * 1.03 and close_today > ma20_now:
            return "多頭續攻回測"

    return "其他"


# ── 今日快照 ────────────────────────────────────────────────────────────────
def today_snapshot(df):
    i = len(df) - 1
    r = df.iloc[i]
    vol_ratio = float(r["volume"] / r["vol_ma20"]) if pd.notna(r["vol_ma20"]) and r["vol_ma20"] > 0 else None
    bias_ma20 = float((r["close"] - r["ma20"]) / r["ma20"] * 100) if pd.notna(r["ma20"]) and r["ma20"] > 0 else None
    return {
        "date": df.index[i].strftime("%Y-%m-%d"),
        "close": round(float(r["close"]), 2),
        "rsi": round(float(r["rsi14"]), 1) if pd.notna(r["rsi14"]) else None,
        "k": round(float(r["k9"]), 1) if pd.notna(r["k9"]) else None,
        "d": round(float(r["d9"]), 1) if pd.notna(r["d9"]) else None,
        "vol_ratio": round(vol_ratio, 2) if vol_ratio else None,
        "bias_ma20": round(bias_ma20, 2) if bias_ma20 is not None else None,
    }


def yticker(code, market):
    return f"{code}.TWO" if market == "上櫃" else f"{code}.TW"


def batch_download(codes_markets):
    tickers = [yticker(c, m) for c, m in codes_markets]
    end = datetime.now() + timedelta(days=1)
    start = end - timedelta(days=150)
    try:
        df = yf.download(tickers, start=start.strftime("%Y-%m-%d"),
                         end=end.strftime("%Y-%m-%d"),
                         group_by="ticker", progress=False,
                         auto_adjust=False, threads=True)
        return df
    except Exception as e:
        print(f"  [WARN] batch fail: {e}")
        return None


def extract_ohlcv(big_df, ticker):
    try:
        if isinstance(big_df.columns, pd.MultiIndex):
            sub = big_df[ticker].copy()
        else:
            sub = big_df.copy()
        sub = sub[["Open", "High", "Low", "Close", "Volume"]].copy()
        sub.columns = ["open", "high", "low", "close", "volume"]
        sub = sub.dropna()
        if len(sub) < 25:
            return None
        return sub
    except Exception:
        return None


# ── HTML 報告 ──────────────────────────────────────────────────────────────
PATTERN_BADGE = {
    "W底/V反":      ("🏆 黃金型態",   "#f57c00"),
    "跌深反彈":     ("⭐ 跌深反彈",   "#1565c0"),
    "多頭續攻回測": ("⭐ 多頭續攻",   "#1565c0"),
    "箱型突破":     ("◆ 箱型突破",    "#6a1b9a"),
    "其他":         ("一般",          "#757575"),
    "資料不足":     ("?",             "#bdbdbd"),
}


def build_row(idx, r, in_open_set):
    pat_text, pat_color = PATTERN_BADGE.get(r["pattern"], ("一般", "#757575"))
    rules_html = "<br>".join(f"<span style='color:#2e7d32'>{x}</span>" for x in r["triggered"])
    s = r["snap"]
    tv = f"https://tw.tradingview.com/chart/?symbol=TWSE%3A{r['code']}" if r["market"] == "上市" \
         else f"https://tw.tradingview.com/chart/?symbol=TPEX%3A{r['code']}"
    cm = f"https://www.cmoney.tw/finance/{r['code']}/f00000"
    held = "<span style='background:#1565c0;color:white;border-radius:3px;padding:1px 5px;font-size:.75em;margin-left:4px'>持股中</span>" if r["code"] in in_open_set else ""
    return f"""
    <tr>
      <td><b>{idx}</b></td>
      <td><b>{r['code']}</b><br><a href='{tv}' target='_blank' style='font-size:.8em;color:#1565c0'>TV</a> | <a href='{cm}' target='_blank' style='font-size:.8em;color:#1565c0'>K線</a></td>
      <td>{r['name']}{held}</td>
      <td style='font-size:.9em'>{r['sector']}</td>
      <td><b>${s['close']:.2f}</b></td>
      <td style='font-size:.85em'>{rules_html}</td>
      <td><span style='background:{pat_color};color:white;border-radius:4px;padding:2px 8px;font-size:.8em'>{pat_text}</span></td>
      <td>{s['vol_ratio']:.2f}x</td>
      <td>{s['rsi']}</td>
      <td>{s['k']}</td>
      <td>{s['bias_ma20']:+.1f}%</td>
      <td><b style='color:#c62828'>${s['close']:.2f}</b></td>
    </tr>"""


def build_html(top, gold, all_hits, in_open_set):
    pat_cnt = Counter(r["pattern"] for r in all_hits)
    rule_cnt = Counter()
    for r in all_hits:
        for t in r["triggered"]:
            rule_cnt[t] += 1

    def _tbl(rows):
        return f"""<table>
<thead><tr><th>#</th><th>代號</th><th>名稱</th><th>類股</th><th>收盤</th>
<th>觸發規則</th><th>起漲前型態</th><th>量比</th><th>RSI</th><th>K</th>
<th>距20MA</th><th>建議買價</th></tr></thead>
<tbody>{rows}</tbody></table>"""

    top_rows = "".join(build_row(i, r, in_open_set) for i, r in enumerate(top, 1))
    gold_rows = "".join(build_row(i, r, in_open_set) for i, r in enumerate(gold, 1))

    pat_rows = "".join(
        f"<tr><td>{p}</td><td>{c}</td><td>{c/len(all_hits)*100:.0f}%</td></tr>"
        for p, c in pat_cnt.most_common())
    rule_rows = "".join(
        f"<tr><td>{r}</td><td>{c}</td></tr>"
        for r, c in rule_cnt.most_common())

    html = f"""<!DOCTYPE html>
<html lang="zh-Hant">
<head>
<meta charset="utf-8">
<title>今日符合回塑性規則之候選股 {RUN_DATE}</title>
<style>
  body {{ font-family: "Microsoft JhengHei", Arial, sans-serif; font-size:14px; background:#f5f5f5; margin:0; padding:20px; color:#333; }}
  .wrap {{ max-width:1400px; margin:0 auto; background:white; padding:28px; border-radius:8px; box-shadow:0 2px 8px rgba(0,0,0,.05); }}
  h1 {{ color:#1a237e; font-size:22px; border-bottom:3px solid #1a237e; padding-bottom:10px; }}
  h2 {{ color:#283593; font-size:17px; margin-top:30px; border-left:5px solid #42a5f5; padding-left:12px; }}
  h2.gold {{ border-left-color:#f57c00; color:#e65100; }}
  table {{ border-collapse:collapse; width:100%; margin:12px 0; font-size:13px; }}
  th {{ background:#1a237e; color:white; padding:8px 10px; text-align:left; }}
  td {{ padding:8px 10px; border-bottom:1px solid #e0e0e0; vertical-align:middle; }}
  tr:hover {{ background:#f0f4ff; }}
  .summary {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(220px,1fr)); gap:14px; margin:16px 0; }}
  .card {{ background:#e8eaf6; border-radius:6px; padding:14px 18px; }}
  .card .big {{ font-size:24px; font-weight:bold; color:#1a237e; display:block; margin-top:4px; }}
  .tip {{ background:#fffde7; border-left:4px solid #f9a825; padding:12px 16px; margin:14px 0; border-radius:4px; line-height:1.7; }}
  .rules {{ background:#e8f5e9; border-left:4px solid #43a047; padding:12px 16px; margin:14px 0; border-radius:4px; line-height:1.8; font-size:13px; }}
  footer {{ font-size:12px; color:#888; margin-top:30px; text-align:center; padding-top:20px; border-top:1px solid #eee; }}
</style>
</head>
<body><div class="wrap">

<h1>台股今日候選股 — 套用回塑性分析規則</h1>
<p style="color:#666">分析日期：{RUN_DATE} ｜ 全市場掃描 1967 檔 ｜ 命中：{len(all_hits)} 檔 ｜ 黃金型態+規則：{len(gold)} 檔</p>

<div class="summary">
  <div class="card">命中總數 <span class="big">{len(all_hits)}</span></div>
  <div class="card">🏆 黃金候選 (W底/V反+規則) <span class="big" style="color:#e65100">{len(gold)}</span></div>
  <div class="card">前 10 強 <span class="big" style="color:#2e7d32">{len(top)}</span></div>
</div>

<div class="rules">
<b>📐 套用規則（4 條任一觸發）</b>：<br>
① 量縮整理 ≥3 日後爆量站 5MA ｜ ② KD&lt;20 黃金交叉站 10MA ｜ ③ 縮量回測 20MA 不破紅K ｜ ④ 突破近 20 日高點帶量<br>
<b>📐 起漲前型態加分</b>：W底/V反（回塑性 85%）、跌深反彈、多頭續攻回測、箱型突破<br>
<b>📐 排序</b>：① 觸發規則數多者優先 → ② 黃金型態優先 → ③ 量比大者優先
</div>

<h2 class="gold">🏆 最強候選 — 黃金型態（W底/V反）＋ 規則觸發</h2>
{_tbl(gold_rows) if gold else '<p style="color:#999">今日無同時符合 W底/V反 型態 + 4 條規則任一者</p>'}

<h2>🥇 前 10 強候選（依強度排序）</h2>
{_tbl(top_rows)}

<h2>📊 命中型態分布</h2>
<table><tr><th>起漲前型態</th><th>檔數</th><th>佔比</th></tr>{pat_rows}</table>

<h2>📊 觸發規則統計</h2>
<table><tr><th>規則</th><th>檔數</th></tr>{rule_rows}</table>

<div class="tip">
⚠️ 本報告僅為依過往獲利 ≥10% 個股反推的型態+規則套用，<b>非建議買賣</b>。
實際操作前請自行確認個股法人、籌碼、產業面是否配合。
</div>

<footer>
由 tw_stock_today_filter.py 自動生成 ｜ 收件人：wic0935 + chenyichieh70 ｜ 規則來源：retro_analysis_20260512<br>
</footer>

</div></body></html>"""
    return html


def send_email(html, html_path, n_hit, n_gold):
    with open(EMAIL_CFG, encoding="utf-8") as f:
        cfg = json.load(f)
    msg = MIMEMultipart("mixed")
    msg["Subject"] = f"[今日候選] 套用回塑性規則 {RUN_DATE}（命中{n_hit}檔 / 黃金{n_gold}檔）"
    msg["From"]    = cfg["sender_email"]
    msg["To"]      = ", ".join(RECIPIENTS)
    msg.attach(MIMEText(html, "html", "utf-8"))
    with open(html_path, "rb") as f:
        att = MIMEApplication(f.read(), _subtype="octet-stream")
        fname = os.path.basename(html_path)
        att.add_header("Content-Disposition", "attachment",
                       filename=("utf-8", "", fname))
    msg.attach(att)
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
        s.login(cfg["sender_email"], cfg["sender_app_password"])
        s.sendmail(cfg["sender_email"], RECIPIENTS, msg.as_bytes())
    print(f"[OK] Email sent → {RECIPIENTS}")


# ── 主程式 ──────────────────────────────────────────────────────────────────
def main():
    print(f"[{RUN_DATE}] 全市場掃描 + 規則套用")

    df_twse = fetch_twse_list()
    df_twse["market"] = "上市"
    df_tpex = fetch_tpex_list()
    df_tpex["market"] = "上櫃"
    pool = pd.concat([df_twse, df_tpex], ignore_index=True)
    print(f"  總清單 {len(pool)} 檔（上市{len(df_twse)}、上櫃{len(df_tpex)}）")

    # 讀 sim_trades.json 標註持股中
    try:
        with open(r"C:\Users\s8711\OneDrive\桌面\stock_reports\sim_trades.json", encoding="utf-8") as f:
            sim = json.load(f)
        in_open_set = {t["code"] for t in sim.get("open", [])}
    except Exception:
        in_open_set = set()

    hits = []
    items = list(pool.itertuples(index=False))
    total = len(items)
    n_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE

    for bi in range(n_batches):
        chunk = items[bi * BATCH_SIZE:(bi + 1) * BATCH_SIZE]
        codes_markets = [(c.code, c.market) for c in chunk]
        print(f"  batch {bi+1}/{n_batches}  抓 {len(chunk)} 檔...")
        big = batch_download(codes_markets)
        if big is None:
            time.sleep(2)
            continue
        for c in chunk:
            tk = yticker(c.code, c.market)
            sub = extract_ohlcv(big, tk)
            if sub is None:
                continue
            try:
                df = add_indicators(sub)
                triggered = check_rules_today(df)
                if not triggered:
                    continue
                pat = classify_pattern_today(df)
                snap = today_snapshot(df)
                if snap["vol_ratio"] is None or snap["bias_ma20"] is None or snap["rsi"] is None:
                    continue
                hits.append({
                    "code": c.code, "name": c.name, "sector": c.sector,
                    "market": c.market,
                    "triggered": triggered,
                    "pattern": pat,
                    "snap": snap,
                })
            except Exception as e:
                continue
        time.sleep(0.6)

    print(f"  命中 {len(hits)} 檔")
    if not hits:
        print("[ERR] 今日無命中股")
        return

    # 排序：規則數 → 是否黃金型態 → 量比
    def rank_key(r):
        is_gold = 1 if r["pattern"] == "W底/V反" else 0
        return (len(r["triggered"]), is_gold, r["snap"]["vol_ratio"] or 0)

    hits_sorted = sorted(hits, key=rank_key, reverse=True)
    top = hits_sorted[:TOP_N]
    gold = [r for r in hits_sorted if r["pattern"] == "W底/V反"][:TOP_N]

    html = build_html(top, gold, hits, in_open_set)
    os.makedirs(REPORT_DIR, exist_ok=True)
    fname = f"today_filter_{RUN_DATE.replace('-','')}.html"
    path  = os.path.join(REPORT_DIR, fname)
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[OK] Report saved: {path}")

    send_email(html, path, len(hits), len(gold))
    print("[DONE]")


if __name__ == "__main__":
    main()
