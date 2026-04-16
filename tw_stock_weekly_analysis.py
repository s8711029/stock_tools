#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
台股週K壓力/支撐分析 + 進出場建議  v2
執行時機：每日 18:00（盤後）

資料來源：
  1. 當日各時段選股結果（hourly/ HTML，每時段前10名）
  2. 模擬持倉中的股票（sim_trades.json open 清單）

分析邏輯：
  支撐：週MA10、週MA20、週MA52、近13週最低點、週樞紐 S1/S2
  壓力：近13週高點、近52週高點、週樞紐 R1/R2/P
  進場：最近支撐上方
  出場：最近壓力下方 1%
  停損：關鍵支撐下方 3%
"""

import os, sys, re, json, datetime, math, warnings, webbrowser
import smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text      import MIMEText
from email.mime.base      import MIMEBase
from email               import encoders
from email.header        import Header
import concurrent.futures
import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────
# 路徑設定（與主工具一致）
# ─────────────────────────────────────────────
TOOLS_DIR   = os.path.dirname(os.path.abspath(__file__))
DESKTOP     = os.path.dirname(TOOLS_DIR)
REPORT_DIR  = os.path.join(DESKTOP, "stock_reports")
HOURLY_DIR  = os.path.join(REPORT_DIR, "hourly")
SIM_JSON    = os.path.join(REPORT_DIR, "sim_trades.json")
PREV_JSON   = os.path.join(REPORT_DIR, "prev_scores.json")
OUT_HTML    = os.path.join(REPORT_DIR, "weekly_analysis.html")
OUT_MD_DIR  = REPORT_DIR   # 每日 MD 存到 stock_reports/週K隔日推薦_YYYY-MM-DD.md

TOP_N       = 10
MAX_WORKERS = 8

# 時段標籤（依 HTML 檔名判斷）
SLOT_HOURS = {"09": "09:00", "10": "10:00", "11": "11:00", "12": "12:00", "13": "13:00"}


# ─────────────────────────────────────────────
# 從 prev_scores.json 載入法人/評分資料（補充用）
# ─────────────────────────────────────────────
def load_prev_scores():
    """回傳 {code: {combined, d_signals, h_signals, foreign_net, trust_net,
                     consec_foreign, consec_trust}}"""
    if not os.path.exists(PREV_JSON):
        return {}
    try:
        with open(PREV_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            data = list(data.values())
        return {
            r["code"]: {
                "combined":       r.get("combined", 0),
                "d_signals":      r.get("d_signals", ""),
                "h_signals":      r.get("h_signals", ""),
                "foreign_net":    r.get("foreign_net", 0),
                "trust_net":      r.get("trust_net", 0),
                "consec_foreign": r.get("consec_foreign", 0),
                "consec_trust":   r.get("consec_trust", 0),
                "market":         r.get("market", "上市"),
            }
            for r in data if "code" in r
        }
    except Exception as e:
        print(f"  [警告] 讀取 prev_scores.json 失敗: {e}")
        return {}


# ─────────────────────────────────────────────
# 解析單一 hourly HTML → 前 TOP_N 股票清單
# ─────────────────────────────────────────────
def _parse_hourly_html(filepath):
    """
    回傳 list of dict: {code, name, sector, price}
    HTML table 格式（每 row 的 td）：
      [0] 排名  [1] 日期時間  [2] 代號+TVK線  [3] 名稱
      [4] 類股  [5] 現價  ...
    """
    try:
        with open(filepath, encoding="utf-8", errors="replace") as f:
            content = f.read()
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", content, re.DOTALL)
        stocks = []
        for row in rows:
            tds = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
            if len(tds) < 6:
                continue
            # 取代號：td[2] 格式為 "2330\n              TVK線"
            raw_code = re.sub(r"<[^>]+>", "", tds[2]).strip()
            code = raw_code.split()[0] if raw_code.split() else ""
            if not re.match(r"^\d{4}$", code):
                continue
            name   = re.sub(r"<[^>]+>", "", tds[3]).strip()
            sector = re.sub(r"<[^>]+>", "", tds[4]).strip()
            price_s = re.sub(r"<[^>]+>", "", tds[5]).strip().replace(",", "")
            try:
                price = float(price_s)
            except ValueError:
                continue
            stocks.append({"code": code, "name": name, "sector": sector, "price": price})
            if len(stocks) >= TOP_N:
                break
        return stocks
    except Exception as e:
        print(f"  [警告] 解析失敗 {os.path.basename(filepath)}: {e}")
        return []


# ─────────────────────────────────────────────
# 讀取當日各時段選股（指定日期，預設今日）
# ─────────────────────────────────────────────
def load_hourly_stocks(date_str=None):
    """
    掃描 hourly/ 目錄中當日的 HTML，
    回傳：
      slot_map  = { "09:00": [stock, ...], "10:00": [...], ... }
      stock_map = { code: {code, name, sector, price, sources:[...]} }
    """
    if date_str is None:
        date_str = datetime.date.today().strftime("%Y-%m-%d")

    slot_map  = {}
    stock_map = {}   # code → merged stock info

    if not os.path.isdir(HOURLY_DIR):
        print(f"  [警告] 找不到 hourly 目錄：{HOURLY_DIR}")
        return slot_map, stock_map

    # 找當日所有 HTML，取各時段最新的一個（最大 HHMM）
    pattern = re.compile(rf"^{re.escape(date_str)}_(\d{{4}})\.html$")
    files_by_slot = {}  # slot_label → (hhmm, filepath)
    for fname in os.listdir(HOURLY_DIR):
        m = pattern.match(fname)
        if not m:
            continue
        hhmm = m.group(1)
        slot_label = SLOT_HOURS.get(hhmm[:2])
        if not slot_label:
            continue
        fpath = os.path.join(HOURLY_DIR, fname)
        # 同一時段有多個檔案（如 0905, 0912），取最後一個（最大 hhmm）
        if slot_label not in files_by_slot or hhmm > files_by_slot[slot_label][0]:
            files_by_slot[slot_label] = (hhmm, fpath)

    if not files_by_slot:
        print(f"  [警告] {date_str} 無任何時段 HTML 資料")
        return slot_map, stock_map

    for slot_label in sorted(files_by_slot):
        hhmm, fpath = files_by_slot[slot_label]
        stocks = _parse_hourly_html(fpath)
        slot_map[slot_label] = stocks
        print(f"  [{slot_label}] {os.path.basename(fpath)} → {len(stocks)} 檔")
        for s in stocks:
            code = s["code"]
            if code not in stock_map:
                stock_map[code] = dict(s, sources=[], combined=0,
                                       d_signals="", h_signals="",
                                       foreign_net=0, trust_net=0,
                                       consec_foreign=0, consec_trust=0,
                                       market="上市")
            stock_map[code]["sources"].append(slot_label)
            # 用最新時段的價格（較後面的覆蓋）
            stock_map[code]["price"] = s["price"]

    return slot_map, stock_map


# ─────────────────────────────────────────────
# 讀取模擬持倉
# ─────────────────────────────────────────────
def load_open_positions():
    """
    回傳 list of dict，每筆含：
      code, name, sector, price(=curr_price), entry_price,
      entry_slot, curr_pct, days_held, entry_signals
    """
    if not os.path.exists(SIM_JSON):
        return []
    try:
        with open(SIM_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
        positions = data.get("open", [])
        result = []
        for p in positions:
            result.append({
                "code":          p.get("code", ""),
                "name":          p.get("name", ""),
                "market":        p.get("market", "上市"),
                "sector":        p.get("sector", ""),
                "price":         p.get("curr_price", p.get("entry_price", 0)),
                "entry_price":   p.get("entry_price", 0),
                "entry_slot":    p.get("entry_slot", ""),
                "entry_date":    p.get("entry_date", ""),
                "curr_pct":      p.get("curr_pct", 0),
                "days_held":     p.get("days_held", 0),
                "entry_signals": p.get("entry_signals", ""),
                "sources":       ["持倉中"],
                "combined":      p.get("entry_score", 0),
                "d_signals":     p.get("entry_signals", ""),
                "h_signals":     "",
                "foreign_net":   0,
                "trust_net":     0,
                "consec_foreign":0,
                "consec_trust":  0,
            })
        return result
    except Exception as e:
        print(f"  [警告] 讀取持倉失敗: {e}")
        return []


# ─────────────────────────────────────────────
# 週K技術分析（核心計算，不變）
# ─────────────────────────────────────────────
def _round2(v):
    try:
        f = float(v)
        return None if math.isnan(f) else round(f, 2)
    except Exception:
        return None

def analyze_weekly(stock):
    code  = stock["code"]
    name  = stock["name"]
    price = stock["price"]
    ticker = code + ".TW"

    try:
        t  = yf.Ticker(ticker)
        wk = t.history(period="2y", interval="1wk", auto_adjust=True)
        if wk is None or len(wk) < 20:
            return None
        wk = wk.dropna(subset=["Close"])
        if len(wk) < 20:
            return None

        c = wk["Close"]
        h = wk["High"]
        l = wk["Low"]

        ma10 = _round2(c.rolling(10).mean().iloc[-1])
        ma20 = _round2(c.rolling(20).mean().iloc[-1])
        ma52 = _round2(c.rolling(min(52, len(c))).mean().iloc[-1])

        hi13 = _round2(h.iloc[-13:].max())
        lo13 = _round2(l.iloc[-13:].min())
        hi52 = _round2(h.iloc[-52:].max()) if len(h) >= 52 else _round2(h.max())
        lo52 = _round2(l.iloc[-52:].min()) if len(l) >= 52 else _round2(l.min())

        ph    = _round2(h.iloc[-2])
        pl    = _round2(l.iloc[-2])
        pc    = _round2(c.iloc[-2])
        pivot = _round2((ph + pl + pc) / 3)
        r1    = _round2(2 * pivot - pl)
        r2    = _round2(pivot + (ph - pl))
        s1    = _round2(2 * pivot - ph)
        s2    = _round2(pivot - (ph - pl))

        supports = sorted(
            [v for v in [ma10, ma20, ma52, lo13, s1, s2]
             if v is not None and v < price * 1.01],
            reverse=True
        )
        resistances = sorted(
            [v for v in [hi13, hi52, r1, r2, pivot]
             if v is not None and v > price * 0.99],
        )

        key_support     = supports[0]    if supports         else lo13
        key_support2    = supports[1]    if len(supports) > 1 else None
        key_resistance  = resistances[0] if resistances       else hi13
        key_resistance2 = resistances[1] if len(resistances) > 1 else None

        if key_support and key_resistance:
            total_range = key_resistance - key_support
            pos_pct = (price - key_support) / total_range * 100 if total_range > 0 else 50
        else:
            pos_pct = 50

        if key_support and price <= key_support * 1.03:
            entry        = _round2(key_support * 1.005)
            strategy     = "支撐買進"
            strategy_note = f"現價接近支撐 {key_support}，可於支撐區分批承接"
        elif key_support and pos_pct < 35:
            entry        = _round2(price * 1.005)
            strategy     = "低位進場"
            strategy_note = f"位於區間下段（{pos_pct:.0f}%），支撐 {key_support} / 壓力 {key_resistance}"
        elif ma20 and price >= ma20 * 0.98 and price <= ma20 * 1.05:
            entry        = _round2(price * 1.008)
            strategy     = "均線支撐"
            strategy_note = f"站穩週MA20（{ma20}），拉回確認後進場"
        else:
            entry        = _round2(price * 1.01)
            strategy     = "現價觀察"
            strategy_note = f"區間中段（{pos_pct:.0f}%），可等拉回至 {_round2(price * 0.97)}"

        target  = _round2(key_resistance  * 0.99) if key_resistance  else _round2(price * 1.08)
        target2 = _round2(key_resistance2 * 0.99) if key_resistance2 else None
        stop    = _round2(key_support * 0.97)      if key_support     else _round2(price * 0.93)

        rr = None
        if entry and target and stop and entry > stop:
            rr = _round2((target - entry) / (entry - stop))

        trend = "不明"
        if ma10 and ma20:
            if price > ma10 > ma20:
                trend = "週線多頭"
            elif price > ma20 > ma10:
                trend = "MA10下彎，觀望"
            elif ma10 > ma20 and price < ma10:
                trend = "拉回修正"
            elif price < ma10 < ma20:
                trend = "週線空頭"
            else:
                trend = "盤整"

        result = dict(stock)   # 保留 sources、entry_price 等原始欄位
        result.update({
            "ma10": ma10, "ma20": ma20, "ma52": ma52,
            "hi13": hi13, "lo13": lo13, "hi52": hi52, "lo52": lo52,
            "pivot": pivot, "r1": r1, "r2": r2, "s1": s1, "s2": s2,
            "trend": trend, "pos_pct": round(pos_pct, 1),
            "strategy": strategy, "strategy_note": strategy_note,
            "entry": entry, "target": target, "target2": target2,
            "stop": stop, "rr": rr,
            "key_support": key_support, "key_support2": key_support2,
            "key_resistance": key_resistance, "key_resistance2": key_resistance2,
        })
        return result
    except Exception as e:
        print(f"  [警告] {code} {name} 分析失敗: {e}")
        return None


# ─────────────────────────────────────────────
# HTML 輔助函式
# ─────────────────────────────────────────────
def _inst_tag(net, consec, label):
    if net > 0:
        s = f'<span style="color:#c0392b">▲{label}買{net:,}張'
        if consec >= 3:
            s += f'（連{consec}日）'
        return s + "</span>"
    elif net < 0:
        s = f'<span style="color:#27ae60">▼{label}賣{abs(net):,}張'
        if consec >= 3:
            s += f'（連{consec}日）'
        return s + "</span>"
    return f'<span style="color:#bbb">{label}中立</span>'

def _trend_badge(trend):
    colors = {
        "週線多頭":      "#27ae60",
        "拉回修正":      "#e67e22",
        "週線空頭":      "#c0392b",
        "盤整":          "#7f8c8d",
        "MA10下彎，觀望": "#e74c3c",
        "不明":          "#95a5a6",
    }
    c = colors.get(trend, "#7f8c8d")
    return f'<span style="background:{c};color:#fff;padding:2px 7px;border-radius:4px;font-size:.82em">{trend}</span>'

def _rr_badge(rr):
    if rr is None:
        return '<span style="color:#aaa">—</span>'
    c = "#27ae60" if rr >= 2 else ("#e67e22" if rr >= 1.2 else "#c0392b")
    return f'<span style="color:{c};font-weight:bold">{rr:.1f}</span>'

def _source_badges(sources):
    badges = []
    for s in sources:
        if s == "持倉中":
            badges.append('<span style="background:#6c3483;color:#fff;padding:2px 7px;border-radius:4px;font-size:.8em;font-weight:bold">持倉中</span>')
        else:
            badges.append(f'<span style="background:#1a5276;color:#fff;padding:2px 6px;border-radius:4px;font-size:.78em">{s}</span>')
    return " ".join(badges)

def _build_row(i, r, td, is_position=False):
    f_tag  = _inst_tag(r.get("foreign_net", 0), r.get("consec_foreign", 0), "外資")
    t_tag  = _inst_tag(r.get("trust_net",  0),  r.get("consec_trust",  0), "投信")

    entry_str  = f'<b style="color:#1565c0">{r["entry"]}</b>'  if r.get("entry")  else "—"
    target_str = f'<b style="color:#c0392b">{r["target"]}</b>' if r.get("target") else "—"
    t2_str     = f' / <span style="color:#e74c3c;font-size:.9em">{r["target2"]}</span>' if r.get("target2") else ""
    stop_str   = f'<span style="color:#27ae60">{r["stop"]}</span>' if r.get("stop") else "—"

    sup_str = str(r.get("key_support", "—"))
    if r.get("key_support2"):
        sup_str += f' <span style="color:#aaa;font-size:.82em">/ {r["key_support2"]}</span>'
    res_str = str(r.get("key_resistance", "—"))
    if r.get("key_resistance2"):
        res_str += f' <span style="color:#aaa;font-size:.82em">/ {r["key_resistance2"]}</span>'

    # 持倉額外資訊
    pos_info = ""
    if is_position or "持倉中" in r.get("sources", []):
        ep   = r.get("entry_price", "")
        pct  = r.get("curr_pct", 0)
        days = r.get("days_held", 0)
        pct_color = "#c0392b" if pct >= 0 else "#27ae60"
        pos_info = (f'<div style="font-size:.8em;margin-top:3px;padding:3px 6px;'
                    f'background:#f5eef8;border-radius:3px">'
                    f'進場價：<b>{ep}</b>｜'
                    f'損益：<span style="color:{pct_color};font-weight:bold">{pct:+.2f}%</span>｜'
                    f'持有 {days} 日</div>')

    row_bg = "#f5eef8" if "持倉中" in r.get("sources", []) else ("#fffef5" if i % 2 == 0 else "#fff")
    _mkt_w = r.get("market", "上市")
    _mkt_wbg = "#1a5276" if _mkt_w == "上市" else "#7d6608"
    _mkt_wb  = f'<span style="background:{_mkt_wbg};color:#fff;padding:1px 5px;border-radius:3px;font-size:.75em">{_mkt_w}</span>'

    return f"""
<tr style="background:{row_bg}">
  <td style="{td};text-align:center;font-weight:bold">{i}</td>
  <td style="{td}">
    <b style="font-size:1.05em">{r['code']}</b> {r['name']} {_mkt_wb}<br>
    <span style="color:#777;font-size:.8em">{r.get('sector','')}</span><br>
    {_source_badges(r.get('sources', []))}
    {pos_info}
  </td>
  <td style="{td};text-align:center">
    <b style="font-size:1.1em">{r['price']}</b><br>
    {_trend_badge(r.get('trend','不明'))}
  </td>
  <td style="{td};text-align:center;color:#555;font-size:.82em">
    {'MA10: '+str(r['ma10']) if r.get('ma10') else '—'}<br>
    {'MA20: '+str(r['ma20']) if r.get('ma20') else '—'}
  </td>
  <td style="{td};text-align:center;color:#27ae60;font-size:.85em">{sup_str}</td>
  <td style="{td};text-align:center;color:#c0392b;font-size:.85em">{res_str}</td>
  <td style="{td}">
    <div style="font-weight:bold;color:#1a5276">{r.get('strategy','')}</div>
    <div style="color:#555;font-size:.82em;margin-top:2px">{r.get('strategy_note','')}</div>
    <div style="margin-top:4px;font-size:.82em">{f_tag} &nbsp; {t_tag}</div>
  </td>
  <td style="{td};text-align:center">{entry_str}</td>
  <td style="{td};text-align:center">{target_str}{t2_str}</td>
  <td style="{td};text-align:center">{stop_str}</td>
  <td style="{td};text-align:center">{_rr_badge(r.get('rr'))}</td>
  <td style="{td};font-size:.8em;color:#666">
    {r.get('d_signals','')}<br>
    <span style="color:#888">{r.get('h_signals','')}</span>
  </td>
</tr>"""


# ─────────────────────────────────────────────
# 產生完整 HTML
# ─────────────────────────────────────────────
def generate_html(results, slot_map, date_str, run_time):
    td  = "padding:8px 10px;border:1px solid #e0e0e0;vertical-align:middle;font-size:.87em"
    th  = "padding:9px 11px;background:#003366;color:#fff;border:1px solid #1a3a6e;white-space:nowrap;font-size:.88em"

    thead = f"""<thead><tr>
  <th style="{th}">#</th>
  <th style="{th}">股票 / 來源</th>
  <th style="{th}">現價<br>趨勢</th>
  <th style="{th}">週均線</th>
  <th style="{th};color:#a9dfbf">支撐</th>
  <th style="{th};color:#f1948a">壓力</th>
  <th style="{th}">策略 / 法人動向</th>
  <th style="{th};color:#aed6f1">進場價</th>
  <th style="{th};color:#f1948a">出場目標</th>
  <th style="{th};color:#a9dfbf">停損</th>
  <th style="{th}">R:R</th>
  <th style="{th}">技術訊號</th>
</tr></thead>"""

    # 統計
    total     = len(results)
    bull      = sum(1 for r in results if "多頭" in r.get("trend", ""))
    good_rr   = sum(1 for r in results if r.get("rr") and r["rr"] >= 1.5)
    positions = [r for r in results if "持倉中" in r.get("sources", [])]
    new_stocks = [r for r in results if "持倉中" not in r.get("sources", [])]

    # ── 時段摘要表 ───────────────────────────
    slot_summary_rows = ""
    for slot in sorted(slot_map.keys()):
        codes_in_slot = [s["code"] for s in slot_map[slot]]
        # 標記哪些有出現在最終分析結果
        tags = []
        for code in codes_in_slot:
            matched = next((r for r in results if r["code"] == code), None)
            if matched:
                trend_b = _trend_badge(matched.get("trend", "不明"))
                rr_v    = matched.get("rr")
                rr_s    = f'R:R <b>{rr_v:.1f}</b>' if rr_v else ""
                in_pos  = "持倉中" in matched.get("sources", [])
                pos_b   = '<span style="background:#6c3483;color:#fff;padding:1px 5px;border-radius:3px;font-size:.75em">持倉</span>' if in_pos else ""
                m_w = matched.get("market", "上市")
                m_wbg = "#1a5276" if m_w == "上市" else "#7d6608"
                mkt_b = f'<span style="background:{m_wbg};color:#fff;padding:0 4px;border-radius:3px;font-size:.72em">{m_w}</span>'
                tags.append(f'<span style="white-space:nowrap">{code} {matched["name"]} {mkt_b} {trend_b} {rr_s} {pos_b}</span>')
            else:
                tags.append(f'<span style="color:#aaa">{code}</span>')
        slot_summary_rows += f"""<tr>
  <td style="padding:7px 12px;border:1px solid #ddd;font-weight:bold;color:#003366">{slot}</td>
  <td style="padding:7px 12px;border:1px solid #ddd;font-size:.85em">{' &nbsp;|&nbsp; '.join(tags)}</td>
</tr>"""

    # ── 持倉區塊 ─────────────────────────────
    pos_rows = ""
    for i, r in enumerate(sorted(positions, key=lambda x: x.get("curr_pct", 0), reverse=True), 1):
        pos_rows += _build_row(i, r, td, is_position=True)

    # ── 當日新選股區塊（依出現時段數排序，再依R:R） ──
    new_rows = ""
    new_sorted = sorted(new_stocks,
                        key=lambda x: (len(x.get("sources", [])), x.get("rr") or 0),
                        reverse=True)
    for i, r in enumerate(new_sorted, 1):
        new_rows += _build_row(i, r, td)

    return f"""<!DOCTYPE html>
<html lang="zh-Hant">
<head>
<meta charset="UTF-8">
<title>週K壓力支撐分析 {date_str}</title>
<style>
  body{{font-family:'Microsoft JhengHei',Arial,sans-serif;margin:20px;background:#f4f6f9;color:#333}}
  h1{{color:#003366;margin-bottom:4px}}
  h2{{color:#1a5276;margin-top:28px;border-bottom:2px solid #d6eaf8;padding-bottom:5px;font-size:1.1em}}
  .summary{{background:#fff;border:1px solid #aed6f1;border-radius:6px;padding:12px 18px;margin:10px 0;font-size:.92em;display:flex;gap:18px;flex-wrap:wrap}}
  .stat{{text-align:center;min-width:80px}}
  .stat .val{{font-size:1.6em;font-weight:bold;color:#003366}}
  .stat .lbl{{font-size:.8em;color:#777}}
  .note{{background:#fffde7;border:1px solid #f9a825;padding:10px 14px;border-radius:4px;font-size:.83em;margin:10px 0}}
  table{{border-collapse:collapse;width:100%;background:#fff;box-shadow:0 1px 4px rgba(0,0,0,.1);border-radius:6px;overflow:hidden;margin-bottom:16px}}
  tr:hover{{background:#eaf2ff!important}}
  footer{{color:#aaa;font-size:.78em;margin-top:20px}}
  .section-head{{background:#eaf4fb;padding:8px 14px;border-left:4px solid #1a5276;margin:18px 0 8px;font-weight:bold;font-size:.95em;border-radius:0 4px 4px 0}}
</style>
</head>
<body>
<h1>台股週K壓力/支撐分析 + 進出場建議</h1>
<div style="color:#888;font-size:.9em;margin-bottom:10px">分析日期：{date_str}｜產生時間：{run_time}</div>

<div class="summary">
  <div class="stat"><div class="val">{total}</div><div class="lbl">分析檔數</div></div>
  <div class="stat"><div class="val">{len(positions)}</div><div class="lbl">持倉中</div></div>
  <div class="stat"><div class="val">{len(new_stocks)}</div><div class="lbl">當日新選股</div></div>
  <div class="stat"><div class="val">{bull}</div><div class="lbl">週線多頭</div></div>
  <div class="stat"><div class="val">{good_rr}</div><div class="lbl">R:R ≥ 1.5</div></div>
  <div class="stat"><div class="val">{len(slot_map)}</div><div class="lbl">有效時段</div></div>
</div>

<div class="note">
  <b>停損</b> = 關鍵支撐下方 3%｜<b>出場目標</b> = 最近壓力下方 1%｜
  <b>R:R</b> = (目標−進場)÷(進場−停損)｜
  <span style="background:#6c3483;color:#fff;padding:1px 6px;border-radius:3px;font-size:.85em">紫色</span> = 持倉中
</div>

<h2>各時段選股摘要</h2>
<table>
<thead><tr>
  <th style="padding:9px 12px;background:#003366;color:#fff;border:1px solid #1a3a6e;width:80px">時段</th>
  <th style="padding:9px 12px;background:#003366;color:#fff;border:1px solid #1a3a6e">前10名（點選表格可比對下方詳細分析）</th>
</tr></thead>
<tbody>{slot_summary_rows}</tbody>
</table>

{'<div class="section-head">持倉中股票週K分析（' + str(len(positions)) + ' 檔）</div><div style="overflow-x:auto"><table>' + thead + '<tbody>' + pos_rows + '</tbody></table></div>' if positions else ''}

<div class="section-head">當日各時段選股週K分析（{len(new_stocks)} 檔，依出現時段數＋R:R排序）</div>
<div style="overflow-x:auto">
<table>
{thead}
<tbody>{new_rows}</tbody>
</table>
</div>

<h2>週K支撐/壓力計算說明</h2>
<table style="width:auto;min-width:480px">
<thead><tr>
  <th style="padding:7px 12px;background:#003366;color:#fff;border:1px solid #1a3a6e">指標</th>
  <th style="padding:7px 12px;background:#003366;color:#fff;border:1px solid #1a3a6e">計算方式</th>
  <th style="padding:7px 12px;background:#003366;color:#fff;border:1px solid #1a3a6e">意義</th>
</tr></thead>
<tbody>
<tr><td style="padding:7px 12px;border:1px solid #ddd">週MA10 / MA20 / MA52</td><td style="padding:7px 12px;border:1px solid #ddd">週收盤價移動平均</td><td style="padding:7px 12px;border:1px solid #ddd">短/中/長期趨勢支撐</td></tr>
<tr style="background:#f8f9fb"><td style="padding:7px 12px;border:1px solid #ddd">近13週高低</td><td style="padding:7px 12px;border:1px solid #ddd">近13週（約一季）最高/最低</td><td style="padding:7px 12px;border:1px solid #ddd">季內壓力/支撐帶</td></tr>
<tr><td style="padding:7px 12px;border:1px solid #ddd">近52週高低</td><td style="padding:7px 12px;border:1px solid #ddd">年度最高/最低</td><td style="padding:7px 12px;border:1px solid #ddd">年度主要壓力/支撐</td></tr>
<tr style="background:#f8f9fb"><td style="padding:7px 12px;border:1px solid #ddd">樞紐 P / R1 / R2</td><td style="padding:7px 12px;border:1px solid #ddd">P=(H+L+C)/3，R1=2P-L，R2=P+(H-L)</td><td style="padding:7px 12px;border:1px solid #ddd">當週預期壓力</td></tr>
<tr><td style="padding:7px 12px;border:1px solid #ddd">樞紐 S1 / S2</td><td style="padding:7px 12px;border:1px solid #ddd">S1=2P-H，S2=P-(H-L)</td><td style="padding:7px 12px;border:1px solid #ddd">當週預期支撐</td></tr>
</tbody>
</table>

<footer>
  資料來源：Yahoo Finance 週線 + hourly HTML + sim_trades.json｜工具：tw_stock_weekly_analysis.py v2｜{run_time}<br>
  <b>免責聲明：</b>本分析為技術指標計算結果，不構成投資建議。投資有風險，請審慎評估。
</footer>
</body></html>"""


# ─────────────────────────────────────────────
# Email 相關（與主工具相同規則）
# ─────────────────────────────────────────────
EMAIL_CFG = os.path.join(DESKTOP, "stock_email_config.json")

def generate_md_report(all_results, slot_map, date_str, run_time):
    """
    產生每日隔日推薦 MD 報告。
    篩選條件：R:R ≥ 1.2、非週線空頭、策略非「現價觀察」
    評分：R:R×10 + 時段數×5 + 外資買×8 + 投信買×8 + 持倉×3 + 多頭+5
    """

    GOOD_STRATEGIES = {"支撐買進", "低位進場", "均線支撐"}
    candidates = []
    for r in all_results:
        rr = r.get("rr")
        if rr is None or rr < 1.2:
            continue
        if r.get("trend") == "週線空頭":
            continue
        if r.get("strategy") not in GOOD_STRATEGIES:
            continue
        sources = r.get("sources", [])
        slot_count = sum(1 for s in sources if s != "持倉中")
        is_pos     = 1 if "持倉中" in sources else 0
        f_buy      = 1 if r.get("foreign_net", 0) > 0 else 0
        t_buy      = 1 if r.get("trust_net",   0) > 0 else 0
        trend_bonus = 5 if r.get("trend") == "週線多頭" else 0
        score = rr * 10 + slot_count * 5 + f_buy * 8 + t_buy * 8 + is_pos * 3 + trend_bonus
        candidates.append((score, r))

    candidates.sort(key=lambda x: x[0], reverse=True)

    def inst_md(r):
        parts = []
        fn = r.get("foreign_net", 0)
        tn = r.get("trust_net",   0)
        if fn > 0:
            parts.append(f"▲外資買{fn:,}張")
        elif fn < 0:
            parts.append(f"▼外資賣{abs(fn):,}張")
        if tn > 0:
            parts.append(f"▲投信買{tn:,}張")
        elif tn < 0:
            parts.append(f"▼投信賣{abs(tn):,}張")
        return "、".join(parts) if parts else "法人中立"

    def src_md(r):
        srcs = r.get("sources", [])
        return " + ".join(srcs) if srcs else "—"

    def pct_str(v, ref):
        if v is None or ref is None or ref == 0:
            return ""
        return f"（{(v - ref) / ref * 100:+.1f}%）"

    lines = []
    lines.append(f"# 台股週K隔日推薦 — {date_str}")
    lines.append(f"\n> 產生時間：{run_time}　　共篩出 **{len(candidates)}** 檔\n")
    lines.append("> 篩選條件：R:R ≥ 1.2、非週線空頭、有明確買點策略（支撐買進 / 低位進場 / 均線支撐）\n")
    lines.append("---\n")

    # ── 總覽表 ──
    lines.append("## 一覽表\n")
    lines.append("| # | 市場 | 代號 | 名稱 | 現價 | 趨勢 | 策略 | 進場價 | 出場目標 | 停損 | R:R | 來源 |")
    lines.append("|---|------|------|------|------|------|------|--------|----------|------|-----|------|")
    for rank, (score, r) in enumerate(candidates, 1):
        code     = r.get("code",     "—")
        name     = r.get("name",     "—")
        mkt_md   = r.get("market",   "上市")
        price    = r.get("price",    0)
        trend    = r.get("trend",    "—")
        strategy = r.get("strategy", "—")
        entry    = r.get("entry",    "—")
        target   = r.get("target",   "—")
        stop     = r.get("stop",     "—")
        rr       = r.get("rr",       "—")
        srcs     = src_md(r)
        lines.append(f"| {rank} | {mkt_md} | {code} | {name} | {price} | {trend} | {strategy} | "
                     f"{entry} | {target} | {stop} | {rr} | {srcs} |")
    lines.append("")

    lines.append("---\n")

    # ── 各標的詳細說明（分上市 / 上櫃區塊） ──
    twse_cands = [(rk, sc, r) for rk, (sc, r) in enumerate(candidates, 1) if r.get("market", "上市") == "上市"]
    tpex_cands = [(rk, sc, r) for rk, (sc, r) in enumerate(candidates, 1) if r.get("market", "上市") == "上櫃"]

    def _write_cands_section(section_title, cand_list):
        if not cand_list:
            return
        lines.append(f"## {section_title}（{len(cand_list)} 檔）\n")
        for rank, score, r in cand_list:
            code     = r.get("code",     "—")
            name     = r.get("name",     "—")
            price    = r.get("price",    0)
            trend    = r.get("trend",    "—")
            strategy = r.get("strategy", "—")
            entry    = r.get("entry")
            target   = r.get("target")
            target2  = r.get("target2")
            stop_v   = r.get("stop")
            rr       = r.get("rr",       "—")
            srcs     = src_md(r)
            inst     = inst_md(r)
            note     = r.get("strategy_note", "")
            is_pos   = "持倉中" in r.get("sources", [])

            star = "★ " if rank <= 2 else ""
            lines.append(f"### {star}第{rank}推薦｜{code} {name}\n")

            tgt_str = str(target)
            if entry and target:
                tgt_str += pct_str(target, entry)
            tgt2_str = f" / {target2}{pct_str(target2, entry)}" if target2 else ""
            stop_str = str(stop_v)
            if entry and stop_v:
                stop_str += pct_str(stop_v, entry)

            lines.append("| 項目 | 數值 |")
            lines.append("|------|------|")
            lines.append(f"| 現價 | {price} |")
            lines.append(f"| 趨勢 | {trend} |")
            lines.append(f"| 策略 | {strategy} |")
            lines.append(f"| 進場價 | **{entry}** |")
            lines.append(f"| 出場目標 | {tgt_str}{tgt2_str} |")
            lines.append(f"| 停損 | {stop_str} |")
            lines.append(f"| **R:R** | **{rr}** |")
            lines.append(f"| 來源 | {srcs} |")
            lines.append(f"| 法人 | {inst} |")
            if is_pos:
                ep   = r.get("entry_price")
                pct  = r.get("curr_pct")
                days = r.get("days_held")
                lines.append(f"| 持倉進場價 | {ep}（損益 {pct:+.1f}%，持有 {days} 日） |")
            lines.append("")
            if note:
                lines.append(f"**分析：** {note}\n")

            # 操作建議
            tip_parts = []
            if strategy == "支撐買進":
                tip_parts.append(f"可於 {entry} 附近分批承接，確認支撐不破後加碼")
            elif strategy == "低位進場":
                tip_parts.append(f"位於區間下段，可於 {entry} 小量試單")
            elif strategy == "均線支撐":
                tip_parts.append(f"站穩週MA20，等開盤確認支撐有效後進場")
            if stop_v:
                tip_parts.append(f"跌破 {stop_v} 停損出場")
            if tip_parts:
                lines.append(f"**操作：** {'；'.join(tip_parts)}\n")

            lines.append("---\n")

    _write_cands_section("上市推薦股", twse_cands)
    _write_cands_section("上櫃推薦股", tpex_cands)

    # ── 操作優先順序（上市 + 上櫃合併，帶市場標示） ──
    if candidates:
        lines.append("## 操作優先順序\n")
        lines.append("```")
        for rank, (score, r) in enumerate(candidates, 1):
            code  = r.get("code",   "—")
            name  = r.get("name",   "—")
            mkt_p = r.get("market", "上市")
            rr    = r.get("rr",     "—")
            entry = r.get("entry",  "—")
            stop_ = r.get("stop",   "—")
            srcs  = src_md(r)
            lines.append(f"{rank}. [{mkt_p}] {code} {name}   進場 {entry} / 停損 {stop_} / R:R {rr}   [{srcs}]")
        lines.append("```\n")

    lines.append("---\n")
    lines.append("> 以上為週K技術指標計算結果，不構成投資建議。"
                 "支撐/壓力為統計計算值，非精確預測，實際操作請結合自身判斷與風險承受度。")
    lines.append(f"\n*本報告由 tw_stock_weekly_analysis.py 自動產生於 {run_time}*")

    return "\n".join(lines)


def load_email_cfg():
    if not os.path.exists(EMAIL_CFG):
        return None
    try:
        with open(EMAIL_CFG, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _parse_recipients(val):
    if isinstance(val, list):
        return [v.strip() for v in val if v.strip()]
    return [v.strip() for v in re.split(r"[,;]", str(val)) if v.strip()]

def build_email_body(all_results, slot_map, date_str, run_time):
    """產生 Gmail 相容的 email 內文（無 JS、純 HTML table）"""
    td  = "padding:7px 9px;border:1px solid #ddd;vertical-align:middle;font-size:.84em"
    thh = "padding:7px 9px;background:#003366;color:#fff;border:1px solid #1a3a6e;white-space:nowrap;font-size:.82em"

    positions_r  = [r for r in all_results if "持倉中" in r.get("sources", [])]
    new_stocks_r = [r for r in all_results if "持倉中" not in r.get("sources", [])]

    total    = len(all_results)
    bull     = sum(1 for r in all_results if "多頭" in r.get("trend", ""))
    good_rr  = sum(1 for r in all_results if r.get("rr") and r["rr"] >= 1.5)

    def trend_span(t):
        c = {"週線多頭":"#27ae60","拉回修正":"#e67e22","週線空頭":"#c0392b",
             "盤整":"#7f8c8d","MA10下彎，觀望":"#e74c3c"}.get(t,"#95a5a6")
        return f'<span style="background:{c};color:#fff;padding:1px 6px;border-radius:3px;font-size:.8em">{t}</span>'

    def rr_span(rr):
        if rr is None: return "—"
        c = "#27ae60" if rr >= 2 else ("#e67e22" if rr >= 1.2 else "#c0392b")
        return f'<span style="color:{c};font-weight:bold">{rr:.1f}</span>'

    def inst_span(net, consec, label):
        if net > 0:
            s = f'<span style="color:#c0392b">▲{label}買{net:,}張'
            if consec >= 3: s += f'連{consec}日'
            return s + '</span>'
        elif net < 0:
            s = f'<span style="color:#27ae60">▼{label}賣{abs(net):,}張'
            if consec >= 3: s += f'連{consec}日'
            return s + '</span>'
        return f'<span style="color:#bbb">{label}中立</span>'

    def src_span(sources):
        parts = []
        for s in sources:
            c = "#6c3483" if s == "持倉中" else "#1a5276"
            parts.append(f'<span style="background:{c};color:#fff;padding:1px 5px;border-radius:3px;font-size:.76em">{s}</span>')
        return " ".join(parts)

    def build_rows(rows_list):
        html = ""
        for i, r in enumerate(rows_list, 1):
            bg = "#f5eef8" if "持倉中" in r.get("sources",[]) else ("#fffef5" if i%2==0 else "#fff")
            pos_info = ""
            if "持倉中" in r.get("sources", []):
                ep   = r.get("entry_price", "")
                pct  = r.get("curr_pct", 0)
                days = r.get("days_held", 0)
                pc   = "#c0392b" if pct >= 0 else "#27ae60"
                pos_info = (f'<br><span style="font-size:.78em;background:#f5eef8;padding:2px 5px;border-radius:3px">'
                            f'進場:{ep} / <span style="color:{pc}">{pct:+.2f}%</span> / 持{days}日</span>')
            target_s = str(r.get("target","—"))
            if r.get("target2"):
                target_s += f'<br><span style="font-size:.8em;color:#e74c3c">{r["target2"]}</span>'
            _emkt = r.get("market", "上市")
            _emkt_bg = "#1a5276" if _emkt == "上市" else "#7d6608"
            _emkt_b  = f'<span style="background:{_emkt_bg};color:#fff;padding:0 4px;border-radius:3px;font-size:.72em">{_emkt}</span>'
            html += f"""<tr style="background:{bg}">
  <td style="{td};text-align:center;font-weight:bold">{i}</td>
  <td style="{td}">{r['code']} {r['name']} {_emkt_b}<br>
    <span style="color:#888;font-size:.78em">{r.get('sector','')}</span><br>
    {src_span(r.get('sources',[]))}{pos_info}</td>
  <td style="{td};text-align:center">{r['price']}<br>{trend_span(r.get('trend','不明'))}</td>
  <td style="{td};text-align:center;font-size:.8em;color:#555">
    {'MA10:'+str(r.get('ma10','')) if r.get('ma10') else '—'}<br>
    {'MA20:'+str(r.get('ma20','')) if r.get('ma20') else '—'}</td>
  <td style="{td};text-align:center;color:#27ae60">{r.get('key_support','—')}</td>
  <td style="{td};text-align:center;color:#c0392b">{r.get('key_resistance','—')}</td>
  <td style="{td}"><b style="color:#1a5276">{r.get('strategy','')}</b><br>
    <span style="font-size:.78em;color:#555">{r.get('strategy_note','')}</span><br>
    <span style="font-size:.8em">{inst_span(r.get('foreign_net',0),r.get('consec_foreign',0),'外資')} {inst_span(r.get('trust_net',0),r.get('consec_trust',0),'投信')}</span></td>
  <td style="{td};text-align:center;color:#1565c0;font-weight:bold">{r.get('entry','—')}</td>
  <td style="{td};text-align:center;color:#c0392b;font-weight:bold">{target_s}</td>
  <td style="{td};text-align:center;color:#27ae60">{r.get('stop','—')}</td>
  <td style="{td};text-align:center">{rr_span(r.get('rr'))}</td>
</tr>"""
        return html

    thead = f"""<tr>
  <th style="{thh}">#</th><th style="{thh}">股票/來源</th>
  <th style="{thh}">現價/趨勢</th><th style="{thh}">週均線</th>
  <th style="{thh};color:#a9dfbf">支撐</th><th style="{thh};color:#f1948a">壓力</th>
  <th style="{thh}">策略/法人</th>
  <th style="{thh};color:#aed6f1">進場價</th>
  <th style="{thh};color:#f1948a">出場目標</th>
  <th style="{thh};color:#a9dfbf">停損</th>
  <th style="{thh}">R:R</th>
</tr>"""

    # 時段摘要
    slot_rows = ""
    for slot in sorted(slot_map.keys()):
        codes = [s["code"] for s in slot_map[slot]]
        matched = []
        for code in codes:
            r = next((x for x in all_results if x["code"] == code), None)
            if r:
                in_pos = "持倉中" in r.get("sources", [])
                pc = "#6c3483" if in_pos else "#1a5276"
                em = r.get("market", "上市")
                em_bg = "#1a5276" if em == "上市" else "#7d6608"
                em_b = f'<span style="background:{em_bg};color:#fff;padding:0 3px;border-radius:2px;font-size:.72em">{em}</span>'
                matched.append(f'<span style="background:{pc};color:#fff;padding:1px 5px;border-radius:3px;font-size:.78em">{code} {r["name"]}</span>{em_b}')
            else:
                matched.append(f'<span style="color:#aaa;font-size:.78em">{code}</span>')
        slot_rows += f'<tr><td style="padding:5px 10px;border:1px solid #ddd;font-weight:bold;color:#003366;white-space:nowrap">{slot}</td><td style="padding:5px 10px;border:1px solid #ddd">{" ".join(matched)}</td></tr>'

    pos_section = ""
    if positions_r:
        pos_section = f"""
<p style="font-size:.95em;font-weight:bold;color:#6c3483;border-left:4px solid #6c3483;padding-left:8px;margin:16px 0 6px">
  ▶ 持倉中週K分析（{len(positions_r)} 檔）</p>
<table style="border-collapse:collapse;width:100%;background:#fff;margin-bottom:12px">
<thead>{thead}</thead>
<tbody>{build_rows(positions_r)}</tbody>
</table>"""

    return f"""<html><head><meta charset="UTF-8"></head>
<body style="font-family:'Microsoft JhengHei',Arial,sans-serif;margin:0;padding:16px;background:#f4f6f9">
<div style="max-width:1100px;margin:0 auto">

<h2 style="color:#003366;margin-bottom:4px">台股週K壓力/支撐分析  {date_str}  {run_time}</h2>
<div style="display:flex;gap:16px;flex-wrap:wrap;background:#fff;border:1px solid #aed6f1;border-radius:6px;padding:12px 18px;margin:10px 0;font-size:.9em">
  <span>分析：<b>{total}</b> 檔</span>
  <span>持倉中：<b>{len(positions_r)}</b> 檔</span>
  <span>新選股：<b>{len(new_stocks_r)}</b> 檔</span>
  <span>週線多頭：<b>{bull}</b> 檔</span>
  <span>R:R≥1.5：<b style="color:#27ae60">{good_rr}</b> 檔</span>
  <span>有效時段：<b>{len(slot_map)}</b> 個</span>
</div>
<div style="background:#fffde7;border:1px solid #f9a825;padding:9px 14px;border-radius:4px;font-size:.82em;margin:8px 0">
  <b>停損</b>=支撐×0.97｜<b>出場目標</b>=壓力×0.99｜<b>R:R</b>=(目標-進場)÷(進場-停損)
</div>

<p style="font-size:.95em;font-weight:bold;color:#003366;border-left:4px solid #003366;padding-left:8px;margin:14px 0 6px">
  ▶ 各時段選股摘要</p>
<table style="border-collapse:collapse;background:#fff;margin-bottom:12px">
<thead><tr>
  <th style="{thh};width:70px">時段</th>
  <th style="{thh}">前10名</th>
</tr></thead>
<tbody>{slot_rows}</tbody>
</table>

{pos_section}

<p style="font-size:.95em;font-weight:bold;color:#1a5276;border-left:4px solid #1a5276;padding-left:8px;margin:16px 0 6px">
  ▶ 當日各時段新選股週K分析（{len(new_stocks_r)} 檔）</p>
<table style="border-collapse:collapse;width:100%;background:#fff;margin-bottom:12px">
<thead>{thead}</thead>
<tbody>{build_rows(new_stocks_r)}</tbody>
</table>

<p style="color:#aaa;font-size:.76em;margin-top:16px">
  資料來源：Yahoo Finance 週線｜工具：tw_stock_weekly_analysis.py v2｜{run_time}<br>
  本分析為技術指標計算結果，不構成投資建議。投資有風險，請審慎評估。
</p>
</div></body></html>"""


def send_email(cfg, all_results, slot_map, html_path, date_str, run_time):
    try:
        sender    = cfg["sender_email"]
        password  = cfg["sender_app_password"]
        receivers = _parse_recipients(cfg.get("recipient_email", ""))
        if not receivers:
            print("  [警告] 無收件人設定")
            return

        # 主旨：標示 R:R 最高的一檔
        best = max((r for r in all_results if r.get("rr") and r["rr"] > 0),
                   key=lambda x: x["rr"], default=None)
        best_str = f" 最佳R:R:{best['code']}{best['name']}({best['rr']:.1f})" if best else ""
        subject = f"[週K分析] {date_str} {run_time} 共{len(all_results)}檔{best_str}"

        body_html = build_email_body(all_results, slot_map, date_str, run_time)

        msg = MIMEMultipart("mixed")
        msg["Subject"] = Header(subject, "utf-8")
        msg["From"]    = sender
        msg["To"]      = ", ".join(receivers)

        # 內文
        msg.attach(MIMEText(body_html, "html", "utf-8"))

        # 附件：完整 HTML 報告
        if html_path and os.path.exists(html_path):
            with open(html_path, "rb") as f:
                attach = MIMEBase("application", "octet-stream")
                attach.set_payload(f.read())
            encoders.encode_base64(attach)
            attach.add_header("Content-Disposition", "attachment",
                               filename=os.path.basename(html_path))
            msg.attach(attach)

        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as s:
            s.login(sender, password)
            s.send_message(msg)

        print(f"  [Email] 已寄出 → {', '.join(receivers)}")
        print(f"          主旨：{subject}")
        print(f"          附件：{os.path.basename(html_path) if html_path else '無'}")
    except Exception as e:
        print(f"  [警告] Email 寄送失敗: {e}")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
def main():
    # 支援指定日期：python tw_stock_weekly_analysis.py 2026-04-10
    if len(sys.argv) > 1 and re.match(r"\d{4}-\d{2}-\d{2}", sys.argv[1]):
        date_str = sys.argv[1]
    else:
        date_str = datetime.date.today().strftime("%Y-%m-%d")

    run_time = datetime.datetime.now().strftime("%Y/%m/%d %H:%M")
    print(f"=== 台股週K壓力/支撐分析 v2  {run_time} ===")
    print(f"    分析日期：{date_str}\n")

    os.makedirs(REPORT_DIR, exist_ok=True)

    # 1. 讀取各時段選股
    print("[1/4] 讀取各時段 hourly 選股結果...")
    slot_map, stock_map = load_hourly_stocks(date_str)
    if not stock_map:
        print("  [警告] 無各時段選股資料，僅分析持倉")

    # 補充 prev_scores.json 的法人/評分資料
    prev_scores = load_prev_scores()
    enriched = 0
    for code, s in stock_map.items():
        if code in prev_scores:
            s.update(prev_scores[code])
            enriched += 1
    print(f"  [法人補充] 從 prev_scores.json 補充 {enriched}/{len(stock_map)} 檔法人資料")

    # 2. 讀取持倉
    print("\n[2/4] 讀取模擬持倉...")
    open_positions = load_open_positions()
    print(f"  持倉中：{len(open_positions)} 檔")

    # 3. 合併（持倉優先，避免重複）
    all_stocks = {}
    for pos in open_positions:
        code = pos["code"]
        if code not in all_stocks:
            all_stocks[code] = pos
        else:
            # 已在 stock_map，補上持倉資訊
            all_stocks[code]["sources"] = list(set(all_stocks[code].get("sources", []) + ["持倉中"]))
            all_stocks[code].update({k: pos[k] for k in ("entry_price", "entry_slot", "entry_date", "curr_pct", "days_held")})

    for code, s in stock_map.items():
        if code not in all_stocks:
            all_stocks[code] = s
        else:
            # 已是持倉，補充時段來源
            for slot in s.get("sources", []):
                if slot not in all_stocks[code].get("sources", []):
                    all_stocks[code].setdefault("sources", []).append(slot)

    total_unique = len(all_stocks)
    print(f"\n[3/4] 共 {total_unique} 檔唯一標的，下載週K線（{MAX_WORKERS} 執行緒）...")

    # 4. 平行分析
    results = []
    done = 0
    stocks_list = list(all_stocks.values())
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = {pool.submit(analyze_weekly, s): s for s in stocks_list}
        for fut in concurrent.futures.as_completed(futs):
            done += 1
            sys.stdout.write(f"\r  進度: {done}/{total_unique}  ")
            sys.stdout.flush()
            res = fut.result()
            if res:
                results.append(res)

    print(f"\n  成功分析 {len(results)} 檔\n")

    if not results:
        print("[錯誤] 無法取得任何週K資料")
        return

    # 5. 排序
    positions_r  = [r for r in results if "持倉中" in r.get("sources", [])]
    new_stocks_r = [r for r in results if "持倉中" not in r.get("sources", [])]
    new_sorted   = sorted(new_stocks_r,
                          key=lambda x: (len(x.get("sources", [])), x.get("rr") or 0),
                          reverse=True)
    all_results  = positions_r + new_sorted

    # 6. 產生 HTML（先存檔，再開啟，確保編碼錯誤不影響輸出）
    print(f"\n[4/4] 產生 HTML → {OUT_HTML}")
    html = generate_html(all_results, slot_map, date_str, run_time)
    with open(OUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    webbrowser.open(f"file:///{OUT_HTML.replace(os.sep, '/')}")
    print("[完成] 報告已開啟")

    # 7. 產生每日 MD 推薦報告
    out_md = os.path.join(OUT_MD_DIR, f"週K隔日推薦_{date_str}.md")
    print(f"\n[MD] 產生隔日推薦 → {out_md}")
    md_content = generate_md_report(all_results, slot_map, date_str, run_time)
    with open(out_md, "w", encoding="utf-8") as f:
        f.write(md_content)
    cand_count = sum(
        1 for r in all_results
        if (r.get("rr") or 0) >= 1.2
        and r.get("trend") != "週線空頭"
        and r.get("strategy") in {"支撐買進", "低位進場", "均線支撐"}
    )
    print(f"[完成] 共篩出 {cand_count} 檔隔日推薦候選")

    # 8. 寄送 Email
    print("\n[Email] 載入設定...")
    cfg = load_email_cfg()
    if cfg:
        send_email(cfg, all_results, slot_map, OUT_HTML, date_str, run_time)
    else:
        print(f"  [略過] 找不到 {EMAIL_CFG}，不寄送 Email")

    # 8. 終端機摘要（忽略編碼錯誤，不影響 HTML）
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    print("\n" + "=" * 72)
    print(f"{'來源':12} {'代號':>5} {'名稱':8} {'現價':>8} {'趨勢':12} {'進場':>8} {'目標':>8} {'停損':>8} {'R:R':>5}")
    print("-" * 72)

    def _print_row(r):
        src = "+".join(r.get("sources", []))[:11]
        name_s = (r['name'][:7] if r['name'] else "")
        trend_s = (r.get('trend','')[:11] if r.get('trend') else "")
        print(f"{src:12} {r['code']:>5} {name_s:8} {r['price']:>8.2f} "
              f"{trend_s:12} "
              f"{str(r.get('entry','—')):>8} {str(r.get('target','—')):>8} "
              f"{str(r.get('stop','—')):>8} "
              f"{str(round(r['rr'],1)) if r.get('rr') else '—':>5}")

    if positions_r:
        print("── 持倉中 ──")
        for r in sorted(positions_r, key=lambda x: x.get("curr_pct", 0), reverse=True):
            _print_row(r)

    print("── 當日選股 ──")
    for r in new_sorted:
        _print_row(r)
    print("=" * 72)


if __name__ == "__main__":
    main()
