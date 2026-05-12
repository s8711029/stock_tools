"""
tw_stock_retro_analysis.py
回塑性分析：對 sim_trades.json 中持股中(open) + 已出場(closed) 獲利 >=10% 的個股
- 用日線 K+量+指標 標出「更好的買點」（4 條規則）
- 標出「更好的賣點」（3 條規則）
- 分類起漲前型態，找共通性
- 產 HTML 報告並 Email 給指定收件人
"""

import json
import os
import smtplib
import sys
import io
import time
import math

try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
except Exception:
    pass
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from datetime import datetime, timedelta
from collections import Counter, defaultdict

import numpy as np
import pandas as pd
import yfinance as yf

SIM_JSON   = r"C:\Users\s8711\OneDrive\桌面\stock_reports\sim_trades.json"
EMAIL_CFG  = r"C:\Users\s8711\OneDrive\桌面\stock_email_config.json"
REPORT_DIR = r"C:\Users\s8711\OneDrive\桌面\stock_reports"
WIN_THRESHOLD = 10.0
RUN_DATE = datetime.now().strftime("%Y-%m-%d")

RECIPIENTS = ["wic0935@gmail.com", "chenyichieh70@gmail.com"]


def yticker(code, market):
    suffix = ".TWO" if "櫃" in (market or "") else ".TW"
    return f"{code}{suffix}"


def fetch_daily(code, market, start_date, end_date):
    tk = yticker(code, market)
    try:
        df = yf.download(tk, start=start_date, end=end_date,
                         progress=False, auto_adjust=False, threads=False)
        if df is None or df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] for c in df.columns]
        df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
        df.columns = ["open", "high", "low", "close", "volume"]
        df = df.dropna()
        if len(df) < 25:
            return None
        df["ma5"]  = df["close"].rolling(5).mean()
        df["ma10"] = df["close"].rolling(10).mean()
        df["ma20"] = df["close"].rolling(20).mean()
        df["ma60"] = df["close"].rolling(60).mean()
        df["vol_ma20"] = df["volume"].rolling(20).mean()
        # KD(9)
        low9  = df["low"].rolling(9).min()
        high9 = df["high"].rolling(9).max()
        rng = (high9 - low9).replace(0, np.nan)
        rsv = ((df["close"] - low9) / rng * 100).fillna(50)
        k_vals = [50.0]
        d_vals = [50.0]
        for r in rsv.values[1:]:
            k = (2/3) * k_vals[-1] + (1/3) * float(r)
            d = (2/3) * d_vals[-1] + (1/3) * k
            k_vals.append(k); d_vals.append(d)
        df["k9"] = k_vals
        df["d9"] = d_vals
        # RSI(14)
        delta = df["close"].diff()
        gain = delta.where(delta > 0, 0).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss.replace(0, np.nan)
        df["rsi14"] = 100 - 100 / (1 + rs)
        # MACD (12,26,9)
        ema12 = df["close"].ewm(span=12, adjust=False).mean()
        ema26 = df["close"].ewm(span=26, adjust=False).mean()
        df["macd"]   = ema12 - ema26
        df["signal"] = df["macd"].ewm(span=9, adjust=False).mean()
        df["hist"]   = df["macd"] - df["signal"]
        return df
    except Exception as e:
        print(f"  [WARN] {code}: {e}")
        return None


def classify_pattern(df, entry_idx):
    if entry_idx < 20:
        return "資料不足"
    pre = df.iloc[max(0, entry_idx - 20):entry_idx]
    pre60 = df.iloc[max(0, entry_idx - 60):entry_idx]
    if pre.empty:
        return "其他"
    high_pre = float(pre["high"].max())
    low_pre  = float(pre["low"].min())
    entry_close = float(df.iloc[entry_idx]["close"])
    if low_pre <= 0:
        return "其他"
    range_pct = (high_pre - low_pre) / low_pre * 100

    if len(pre60) >= 30:
        top60 = float(pre60["high"].max())
        if top60 > 0 and (top60 - entry_close) / top60 > 0.20:
            return "跌深反彈"

    if range_pct < 15 and entry_close >= high_pre * 0.99:
        return "箱型突破"

    if range_pct > 15:
        low_idx_pos = pre["low"].values.argmin()
        if low_idx_pos < len(pre) - 1:
            rebound = (entry_close - low_pre) / low_pre * 100
            if rebound > 5:
                return "W底/V反"

    ma20_now = df.iloc[entry_idx]["ma20"]
    ma20_5d  = df.iloc[max(0, entry_idx - 5)]["ma20"]
    if pd.notna(ma20_now) and pd.notna(ma20_5d) and ma20_now > ma20_5d:
        if pre.iloc[-1]["low"] <= ma20_now * 1.03 and entry_close > ma20_now:
            return "多頭續攻回測"

    return "其他"


def find_better_buy(df, entry_idx, entry_price):
    start = max(20, entry_idx - 30)
    end = min(len(df), entry_idx + 5)
    candidates = []
    for i in range(start, end):
        row = df.iloc[i]
        if pd.isna(row["ma5"]) or pd.isna(row["ma20"]) or pd.isna(row["vol_ma20"]):
            continue
        prev = df.iloc[i - 1] if i > 0 else None

        # Rule 1: 量縮整理 ≥3 日後第一根 量增 ≥1.8x + 收紅站上 5MA
        if i >= 3:
            past3 = df.iloc[i - 3:i]
            if (past3["volume"] < past3["vol_ma20"]).all():
                if (row["volume"] > row["vol_ma20"] * 1.8
                        and row["close"] > row["open"]
                        and row["close"] > row["ma5"]):
                    candidates.append((i, "量縮整理後爆量站5MA", float(row["close"])))
                    continue

        # Rule 2: KD<20 黃金交叉 + 收盤站上 10MA
        if prev is not None and pd.notna(prev["k9"]) and pd.notna(row["k9"]):
            if (prev["k9"] < 20 and prev["k9"] < prev["d9"]
                    and row["k9"] > row["d9"]
                    and pd.notna(row["ma10"]) and row["close"] > row["ma10"]):
                candidates.append((i, "KD低檔黃金交叉站10MA", float(row["close"])))
                continue

        # Rule 3: 跌破 20MA 後第一次縮量回測不破紅K
        if i >= 5:
            past5 = df.iloc[i - 5:i]
            broke = (past5["low"] < past5["ma20"]).any()
            if (broke and row["low"] >= row["ma20"] * 0.98
                    and row["close"] > row["open"]
                    and row["volume"] < row["vol_ma20"]):
                candidates.append((i, "縮量回測20MA不破紅K", float(row["close"])))
                continue

        # Rule 4: 突破近 20 日盤整高點 + 帶量
        if i >= 20:
            recent_high = float(df.iloc[i - 20:i]["high"].max())
            if (row["close"] > recent_high
                    and row["volume"] > row["vol_ma20"] * 1.5
                    and row["close"] > row["open"]):
                candidates.append((i, "突破20日高點帶量", float(row["close"])))
                continue

    earlier = [c for c in candidates if c[0] < entry_idx and c[2] < entry_price]
    if not earlier:
        return None
    i, reason, price = min(earlier, key=lambda c: c[2])
    date = df.index[i].strftime("%Y-%m-%d")
    saved = (entry_price - price) / price * 100 if price else 0
    return {"date": date, "price": round(price, 2),
            "reason": reason, "saved_pct": round(saved, 2)}


def find_better_sell(df, entry_idx, exit_idx, exit_price):
    start = entry_idx + 1
    end = min(len(df), exit_idx + 1)
    if end <= start:
        return None
    candidates = []
    for i in range(start, end):
        row = df.iloc[i]
        if pd.isna(row["ma5"]) or pd.isna(row["rsi14"]) or pd.isna(row["vol_ma20"]):
            continue
        prev = df.iloc[i - 1] if i > 0 else None

        # Rule 1: 爆量長上影線 (上影>2x實體 + 量>2.5x)
        body = abs(row["close"] - row["open"])
        upper = row["high"] - max(row["close"], row["open"])
        if body > 0 and upper > body * 2 and row["volume"] > row["vol_ma20"] * 2.5:
            candidates.append((i, "爆量長上影線", float(row["high"])))
            continue

        # Rule 2: 連 2 日跌破 5MA + MACD 柱由正轉負
        if prev is not None and pd.notna(prev["ma5"]) and pd.notna(prev["hist"]):
            if (row["close"] < row["ma5"] and prev["close"] < prev["ma5"]
                    and row["hist"] < 0 and prev["hist"] >= 0):
                candidates.append((i, "連2日破5MA且MACD柱翻負", float(row["close"])))
                continue

        # Rule 3: RSI>80 後當日翻黑帶量
        if prev is not None and pd.notna(prev["rsi14"]):
            if (prev["rsi14"] > 80 and row["close"] < row["open"]
                    and row["volume"] > row["vol_ma20"] * 1.5):
                candidates.append((i, "RSI過熱翻黑帶量", float(row["close"])))
                continue

    better = [c for c in candidates if float(c[2]) > exit_price]
    if not better:
        return None
    i, reason, price = max(better, key=lambda c: float(c[2]))
    date = df.index[i].strftime("%Y-%m-%d")
    extra = (float(price) - exit_price) / exit_price * 100 if exit_price else 0
    return {"date": date, "price": round(float(price), 2),
            "reason": reason, "extra_pct": round(extra, 2)}


def collect_entry_features(df, entry_idx):
    """進場日當天技術指標快照"""
    if entry_idx < 0 or entry_idx >= len(df):
        return {}
    r = df.iloc[entry_idx]
    pre20 = df.iloc[max(0, entry_idx - 20):entry_idx]
    pre_high = float(pre20["high"].max()) if not pre20.empty else None
    pre_low  = float(pre20["low"].min())  if not pre20.empty else None
    vol_ratio = float(r["volume"] / r["vol_ma20"]) if pd.notna(r["vol_ma20"]) and r["vol_ma20"] > 0 else None
    bias_ma20 = float((r["close"] - r["ma20"]) / r["ma20"] * 100) if pd.notna(r["ma20"]) and r["ma20"] > 0 else None
    bias_ma60 = float((r["close"] - r["ma60"]) / r["ma60"] * 100) if pd.notna(r["ma60"]) and r["ma60"] > 0 else None
    return {
        "rsi": round(float(r["rsi14"]), 1) if pd.notna(r["rsi14"]) else None,
        "k": round(float(r["k9"]), 1) if pd.notna(r["k9"]) else None,
        "d": round(float(r["d9"]), 1) if pd.notna(r["d9"]) else None,
        "hist": round(float(r["hist"]), 3) if pd.notna(r["hist"]) else None,
        "vol_ratio": round(vol_ratio, 2) if vol_ratio else None,
        "bias_ma20": round(bias_ma20, 2) if bias_ma20 else None,
        "bias_ma60": round(bias_ma60, 2) if bias_ma60 else None,
        "pre_range_pct": round((pre_high - pre_low) / pre_low * 100, 1) if pre_high and pre_low else None,
        "ma_aligned": bool(pd.notna(r["ma5"]) and pd.notna(r["ma20"]) and pd.notna(r["ma60"])
                            and r["ma5"] > r["ma20"] > r["ma60"]),
    }


def analyze_one(trade, is_open):
    code   = trade["code"]
    market = trade.get("market", "")
    entry_date  = trade["entry_date"]
    entry_price = float(trade["entry_price"])

    if is_open:
        exit_date  = RUN_DATE
        exit_price = float(trade["curr_price"])
        ret_pct    = float(trade["curr_pct"])
        exit_slot  = "持股中"
    else:
        exit_date  = trade["exit_date"]
        exit_price = float(trade["exit_price"])
        ret_pct    = float(trade["return_pct"])
        exit_slot  = trade.get("exit_slot", "")

    start = (datetime.strptime(entry_date, "%Y-%m-%d") - timedelta(days=150)).strftime("%Y-%m-%d")
    end_dt = datetime.strptime(exit_date, "%Y-%m-%d") + timedelta(days=10)
    end = min(end_dt, datetime.now()).strftime("%Y-%m-%d")

    df = fetch_daily(code, market, start, end)
    if df is None or df.empty:
        return None

    entry_ts = pd.Timestamp(datetime.strptime(entry_date, "%Y-%m-%d"))
    entry_pos = int(df.index.searchsorted(entry_ts, side="right") - 1)
    if entry_pos < 20:
        return None

    if is_open:
        exit_pos = len(df) - 1
    else:
        exit_ts = pd.Timestamp(datetime.strptime(exit_date, "%Y-%m-%d"))
        exit_pos = int(df.index.searchsorted(exit_ts, side="right") - 1)
        if exit_pos <= entry_pos:
            exit_pos = min(entry_pos + 1, len(df) - 1)

    pattern = classify_pattern(df, entry_pos)
    better_buy = find_better_buy(df, entry_pos, entry_price)
    better_sell = find_better_sell(df, entry_pos, exit_pos, exit_price)
    feats = collect_entry_features(df, entry_pos)

    return {
        "code": code, "name": trade.get("name", ""),
        "sector": trade.get("sector", ""), "market": market,
        "entry_date": entry_date, "entry_slot": trade.get("entry_slot", ""),
        "entry_price": round(entry_price, 2),
        "exit_date": exit_date, "exit_slot": exit_slot,
        "exit_price": round(exit_price, 2),
        "return_pct": round(ret_pct, 2),
        "is_open": is_open,
        "pattern": pattern,
        "better_buy": better_buy,
        "better_sell": better_sell,
        "features": feats,
        "entry_signals": trade.get("entry_signals", ""),
        "entry_score": trade.get("entry_score", 0),
    }


def build_narrative(r):
    parts = []
    f = r["features"]
    parts.append(f"📊 進場日型態：<b>{r['pattern']}</b>")
    feat_bits = []
    if f.get("rsi") is not None:        feat_bits.append(f"RSI {f['rsi']}")
    if f.get("k") is not None:          feat_bits.append(f"K {f['k']}")
    if f.get("vol_ratio") is not None:  feat_bits.append(f"量比 {f['vol_ratio']}x")
    if f.get("bias_ma20") is not None:  feat_bits.append(f"距20MA {f['bias_ma20']:+.1f}%")
    if f.get("ma_aligned"):             feat_bits.append("均線多頭排列")
    if feat_bits:
        parts.append("　指標：" + "、".join(feat_bits))

    if r["better_buy"]:
        b = r["better_buy"]
        parts.append(f"🟢 更佳買點 {b['date']} ${b['price']:.2f}（{b['reason']}，較實際進場便宜 {b['saved_pct']:.2f}%）")
    else:
        parts.append("🟢 實際進場價已是區間最低，無更佳買點")

    if r["better_sell"]:
        s = r["better_sell"]
        tag = "建議賣點" if r["is_open"] else "更佳賣點"
        parts.append(f"🔴 {tag} {s['date']} ${s['price']:.2f}（{s['reason']}，較實際出場高 {s['extra_pct']:.2f}%）")
    else:
        if r["is_open"]:
            parts.append("🔴 目前尚無賣出訊號，續抱觀察")
        else:
            parts.append("🔴 實際賣點即區間最高，無更佳賣點")
    return "<br>".join(parts)


def build_html(results):
    n = len(results)
    n_open   = sum(1 for r in results if r["is_open"])
    n_closed = n - n_open
    pattern_cnt = Counter(r["pattern"] for r in results)
    sector_cnt  = Counter(r["sector"] for r in results)
    signal_kw   = ["KD黃金交叉", "MACD柱翻正", "MACD增強", "均線多頭", "站上MA20", "量增", "RSI=", "KD低檔"]
    sig_cnt     = Counter()
    for r in results:
        sig = r["entry_signals"] or ""
        for kw in signal_kw:
            if kw in sig:
                sig_cnt[kw.replace("=", "")] += 1

    # 共通特徵統計
    rsi_vals = [r["features"]["rsi"] for r in results if r["features"].get("rsi") is not None]
    vol_vals = [r["features"]["vol_ratio"] for r in results if r["features"].get("vol_ratio") is not None]
    bias_vals = [r["features"]["bias_ma20"] for r in results if r["features"].get("bias_ma20") is not None]
    ma_aligned_n = sum(1 for r in results if r["features"].get("ma_aligned"))

    def stat(lst):
        if not lst:
            return "-"
        return f"中位 {np.median(lst):.1f} / 區間 {min(lst):.1f}~{max(lst):.1f}"

    pattern_rows = "".join(
        f"<tr><td>{p}</td><td>{c}</td><td>{c/n*100:.0f}%</td></tr>"
        for p, c in pattern_cnt.most_common())
    sector_rows = "".join(
        f"<tr><td>{s or '未分類'}</td><td>{c}</td><td>{c/n*100:.0f}%</td></tr>"
        for s, c in sector_cnt.most_common())
    signal_rows = "".join(
        f"<tr><td>{s}</td><td>{c}</td><td>{c/n*100:.0f}%</td></tr>"
        for s, c in sig_cnt.most_common())

    # 主表
    rows_html = ""
    for i, r in enumerate(sorted(results, key=lambda x: -x["return_pct"]), 1):
        color = "#2e7d32" if not r["is_open"] else "#1565c0"
        tag = "持股" if r["is_open"] else "已出場"
        bb = r["better_buy"]
        bs = r["better_sell"]
        bb_str = (f"{bb['date']}<br>${bb['price']:.2f}<br><span style='color:#2e7d32'>省 {bb['saved_pct']:.2f}%</span>"
                  if bb else "<span style='color:#999'>已是最低</span>")
        bs_str = (f"{bs['date']}<br>${bs['price']:.2f}<br><span style='color:#c62828'>多賺 {bs['extra_pct']:.2f}%</span>"
                  if bs else "<span style='color:#999'>已是最高</span>")
        tv_url = f"https://tw.tradingview.com/chart/?symbol=TWSE%3A{r['code']}" if "櫃" not in r["market"] \
                 else f"https://tw.tradingview.com/chart/?symbol=TPEX%3A{r['code']}"
        cmoney_url = f"https://www.cmoney.tw/finance/{r['code']}/f00000"
        rows_html += f"""
        <tr>
          <td>{i}</td>
          <td>{r['entry_date']}<br><span style='color:#666;font-size:.9em'>{r['entry_slot']}</span></td>
          <td><b>{r['code']}</b><br><a href='{tv_url}' target='_blank' style='font-size:.8em;color:#1565c0'>TV</a> | <a href='{cmoney_url}' target='_blank' style='font-size:.8em;color:#1565c0'>K線</a></td>
          <td>{r['name']}<br><span style='background:{color};color:white;border-radius:3px;padding:1px 6px;font-size:.75em'>{tag}</span></td>
          <td style='font-size:.9em'>{r['sector']}</td>
          <td>{r['entry_date']}<br>{r['entry_slot']}</td>
          <td>${r['entry_price']:.2f}</td>
          <td>{r['exit_date']}<br>{r['exit_slot']}</td>
          <td>${r['exit_price']:.2f}<br><span style='color:{color};font-weight:bold'>{r['return_pct']:+.2f}%</span></td>
          <td style='font-size:.85em;line-height:1.6'>
            <div><b style='color:#1565c0'>實際買入 →</b></div>
            <div><b style='color:#2e7d32'>建議買 ▲</b> {bb_str}</div>
            <div style='margin-top:4px'><b style='color:#c62828'>建議賣 ▼</b> {bs_str}</div>
            <div style='margin-top:6px;padding-top:4px;border-top:1px dashed #ccc'>{build_narrative(r)}</div>
          </td>
        </tr>"""

    # 共通性結論
    top_pattern = pattern_cnt.most_common(1)[0] if pattern_cnt else ("-", 0)
    top_sector  = sector_cnt.most_common(1)[0] if sector_cnt else ("-", 0)
    top_signal  = sig_cnt.most_common(1)[0] if sig_cnt else ("-", 0)

    html = f"""<!DOCTYPE html>
<html lang="zh-Hant">
<head>
<meta charset="utf-8">
<title>台股獲利 ≥10% 回塑性分析 {RUN_DATE}</title>
<style>
  body {{ font-family: "Microsoft JhengHei", Arial, sans-serif; font-size:14px; background:#f5f5f5; margin:0; padding:20px; color:#333; }}
  .wrap {{ max-width:1400px; margin:0 auto; background:white; padding:28px; border-radius:8px; box-shadow:0 2px 8px rgba(0,0,0,.05); }}
  h1 {{ color:#1a237e; font-size:22px; border-bottom:3px solid #1a237e; padding-bottom:10px; }}
  h2 {{ color:#283593; font-size:17px; margin-top:30px; border-left:5px solid #42a5f5; padding-left:12px; }}
  table {{ border-collapse:collapse; width:100%; margin:12px 0; font-size:13px; }}
  th {{ background:#1a237e; color:white; padding:8px 10px; text-align:left; }}
  td {{ padding:8px 10px; border-bottom:1px solid #e0e0e0; vertical-align:top; }}
  tr:hover {{ background:#f0f4ff; }}
  .summary {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(220px,1fr)); gap:14px; margin:16px 0; }}
  .card {{ background:#e8eaf6; border-radius:6px; padding:14px 18px; }}
  .card b {{ color:#1a237e; }}
  .card .big {{ font-size:24px; font-weight:bold; color:#1a237e; display:block; margin-top:4px; }}
  .tip {{ background:#fffde7; border-left:4px solid #f9a825; padding:12px 16px; margin:14px 0; border-radius:4px; line-height:1.7; }}
  .conclusion {{ background:#e8f5e9; border-left:4px solid #43a047; padding:14px 18px; margin:14px 0; border-radius:4px; line-height:1.8; }}
  .badge {{ display:inline-block; background:#1565c0; color:white; border-radius:4px; padding:3px 10px; font-size:12px; margin:3px; }}
  .badge.green {{ background:#2e7d32; }}
  .badge.orange {{ background:#e65100; }}
  .badge.red {{ background:#c62828; }}
  footer {{ font-size:12px; color:#888; margin-top:30px; text-align:center; padding-top:20px; border-top:1px solid #eee; }}
</style>
</head>
<body><div class="wrap">

<h1>台股獲利 ≥10% 個股 — 回塑性分析報告</h1>
<p style="color:#666">分析日期：{RUN_DATE} ｜ 樣本：{n} 檔（已出場 {n_closed} ＋ 持股中 {n_open}）｜ 資料：sim_trades.json</p>

<div class="summary">
  <div class="card">樣本總數 <span class="big">{n}</span></div>
  <div class="card">已出場 ≥10% <span class="big" style="color:#2e7d32">{n_closed}</span></div>
  <div class="card">持股浮盈 ≥10% <span class="big" style="color:#1565c0">{n_open}</span></div>
  <div class="card">最強型態 <span class="big" style="color:#e65100">{top_pattern[0]}</span><span style="color:#666">{top_pattern[1]} 檔 ／ {top_pattern[1]/n*100:.0f}%</span></div>
</div>

<h2>一、起漲前型態共通性</h2>
<table>
  <tr><th>進場前型態</th><th>檔數</th><th>佔比</th></tr>
  {pattern_rows}
</table>
<div class="tip">
<b>📌 結論</b>：本批獲利股最多的進場前型態為 <b>{top_pattern[0]}</b>（{top_pattern[1]} 檔），代表此型態在實際操作中最容易抓到 ≥10% 漲幅。
</div>

<h2>二、技術指標分布（進場日當天）</h2>
<table>
  <tr><th>指標</th><th>統計</th></tr>
  <tr><td>RSI(14)</td><td>{stat(rsi_vals)}</td></tr>
  <tr><td>量比（當日量/20日均量）</td><td>{stat(vol_vals)}</td></tr>
  <tr><td>距 MA20 乖離 (%)</td><td>{stat(bias_vals)}</td></tr>
  <tr><td>均線多頭排列（5&gt;20&gt;60）</td><td>{ma_aligned_n} 檔（{ma_aligned_n/n*100:.0f}%）</td></tr>
</table>

<h2>三、選股訊號出現頻率</h2>
<table>
  <tr><th>訊號</th><th>檔數</th><th>佔比</th></tr>
  {signal_rows}
</table>

<h2>四、優勢類股</h2>
<table>
  <tr><th>類股</th><th>檔數</th><th>佔比</th></tr>
  {sector_rows}
</table>
<div class="tip">
<b>📌 結論</b>：最強類股為 <b>{top_sector[0]}</b>（{top_sector[1]} 檔）。後續選股可優先掃描此類股，搭配「{top_pattern[0]}」型態。
</div>

<h2>五、高勝率組合（建議套用）</h2>
<div class="conclusion">
  <b>🎯 進場最佳條件組合</b>（依本批 {n} 檔反推）：<br>
  <span class="badge orange">起漲前型態：{top_pattern[0]}</span>
  <span class="badge">最強類股：{top_sector[0]}</span>
  <span class="badge green">最高頻訊號：{top_signal[0]}</span><br>
  <span class="badge green">RSI 中位 {np.median(rsi_vals):.0f}</span>
  <span class="badge green">量比中位 {np.median(vol_vals):.1f}x</span>
  <span class="badge green">距 20MA {np.median(bias_vals):+.1f}%</span><br>
  <br>
  <b>⚙️ 買進規則</b>（取最早出現訊號）：<br>
  ① 量縮整理 ≥3 日後第一根量增 ≥1.8x 站上 5MA &nbsp; ② KD&lt;20 黃金交叉站 10MA<br>
  ③ 縮量回測 20MA 不破紅K &nbsp; ④ 突破 20 日盤整高點帶量<br>
  <br>
  <b>⚙️ 賣出規則</b>（取最早出現訊號）：<br>
  ① 爆量長上影線（量&gt;2.5x、上影&gt;2×實體）&nbsp; ② 連 2 日跌破 5MA + MACD 柱由正轉負<br>
  ③ RSI&gt;80 後當日翻黑且量增
</div>

<h2>六、個股明細（依報酬率排序）</h2>
<table>
<thead>
<tr>
  <th>編號</th><th>選股時間</th><th>代號</th><th>名稱</th><th>類股</th>
  <th>買入時間</th><th>買入價</th><th>賣出時間</th><th>賣出價/報酬</th>
  <th style="min-width:340px">分析結果說明</th>
</tr>
</thead>
<tbody>
{rows_html}
</tbody>
</table>

<footer>
由 tw_stock_retro_analysis.py 自動生成 ｜ 收件人：wic0935 + chenyichieh70 ｜ 資料：sim_trades.json<br>
※ 本分析僅供參考，買賣決策請自行評估風險。
</footer>

</div></body></html>"""
    return html


def send_email(html, html_path):
    with open(EMAIL_CFG, encoding="utf-8") as f:
        cfg = json.load(f)

    msg = MIMEMultipart("mixed")
    msg["Subject"] = f"[台股回塑性分析] 獲利≥10% 個股 {RUN_DATE}"
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


def main():
    print(f"[{RUN_DATE}] 開始回塑性分析…")
    with open(SIM_JSON, encoding="utf-8") as f:
        sim = json.load(f)

    closed = [t for t in sim.get("closed", []) if t.get("return_pct", 0) >= WIN_THRESHOLD]
    opens  = [t for t in sim.get("open", [])   if t.get("curr_pct",   0) >= WIN_THRESHOLD]
    print(f"  已出場 ≥{WIN_THRESHOLD}%：{len(closed)} 檔")
    print(f"  持股中 ≥{WIN_THRESHOLD}%：{len(opens)} 檔")

    results = []
    targets = [(t, False) for t in closed] + [(t, True) for t in opens]
    for idx, (trade, is_open) in enumerate(targets, 1):
        tag = "[持]" if is_open else "[平]"
        print(f"  [{idx}/{len(targets)}] {tag} {trade['code']} {trade.get('name','')}", end=" ")
        try:
            r = analyze_one(trade, is_open)
        except Exception as e:
            print(f"-> ERR {e}")
            continue
        if r is None:
            print("-> SKIP (無法取得日線)")
            continue
        print(f"-> {r['pattern']} | buy:{'Y' if r['better_buy'] else '-'} sell:{'Y' if r['better_sell'] else '-'}")
        results.append(r)
        time.sleep(0.4)

    if not results:
        print("[ERR] 無有效資料")
        return

    html = build_html(results)
    os.makedirs(REPORT_DIR, exist_ok=True)
    fname = f"retro_analysis_{RUN_DATE.replace('-','')}.html"
    path  = os.path.join(REPORT_DIR, fname)
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[OK] Report saved: {path}")

    send_email(html, path)
    print("[DONE]")


if __name__ == "__main__":
    main()
