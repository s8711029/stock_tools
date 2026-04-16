#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
台股處置股當沖篩選工具
策略規則（來自操作指南）：
  ✅ 小資金（每次 15% 資金，最多 3 檔）
  ✅ 停損 -5% 必砍
  ✅ 不留隔夜（收盤前出場）
  ❌ 不 All in / 不攤平 / 不追新聞
"""

import sys, os, warnings, datetime, json, webbrowser, math
import smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text      import MIMEText
from email.header         import Header
import requests, io
import pandas as pd
import yfinance as yf
import ta
import concurrent.futures

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────
# 路徑設定
# ─────────────────────────────────────────────
TOOLS_DIR    = os.path.dirname(os.path.abspath(__file__))
DESKTOP      = os.path.dirname(TOOLS_DIR)
REPORT_BASE  = os.path.join(DESKTOP, "stock_reports")
REPORT_DIR   = os.path.join(REPORT_BASE, "disposition")
DAILY_DIR    = os.path.join(REPORT_DIR, "daily")
HOURLY_DIR   = os.path.join(REPORT_DIR, "hourly")
LATEST_HTML  = os.path.join(REPORT_DIR, "latest.html")
SIM_JSON     = os.path.join(REPORT_DIR, "sim_trades.json")
EMAIL_CFG    = os.path.join(DESKTOP, "stock_email_config.json")
COMPARE_HTML = os.path.join(REPORT_BASE, "compare.html")

# v2 sim json（比較用）
V2_SIM_JSON  = os.path.join(REPORT_BASE, "sim_trades.json")

# ─────────────────────────────────────────────
# 策略參數
# ─────────────────────────────────────────────
CAPITAL       = 100.0   # 模擬資本（萬元，僅供顯示比例用）
POS_PCT       = 0.15    # 每次倉位 15%
STOP_LOSS_PCT = -5.0    # 停損 -5%
MAX_POSITIONS = 3       # 最多同時 3 檔
MAX_NEW_PER_RUN = 2     # 每次最多新進 2 檔
MAX_WORKERS   = 8
MIN_SCORE     = 50      # 進場門檻


# ─────────────────────────────────────────────
# 台灣股市國定假日（休市日）
# 每年初請依行政院 / TWSE 公告更新此清單
# 週末不需列入（is_holiday 會自動排除）
# ─────────────────────────────────────────────
TW_HOLIDAYS = {
    # 2025
    "2025-01-01",                                           # 元旦
    "2025-01-27","2025-01-28","2025-01-29",
    "2025-01-30","2025-01-31","2025-02-03",                 # 春節連假
    "2025-02-28",                                           # 和平紀念日
    "2025-04-03","2025-04-04",                              # 兒童節+清明
    "2025-05-01",                                           # 勞動節
    "2025-05-30",                                           # 端午補假
    "2025-10-06",                                           # 中秋
    "2025-10-09","2025-10-10",                              # 國慶
    # 2026
    "2026-01-01",                                           # 元旦
    "2026-01-26","2026-01-27","2026-01-28",
    "2026-01-29","2026-01-30","2026-02-02",                 # 春節連假
    "2026-03-02",                                           # 和平紀念日補假(2/28週六)
    "2026-04-03","2026-04-06",                              # 兒童節補假+清明補假
    "2026-05-01",                                           # 勞動節
    "2026-06-19",                                           # 端午
    "2026-09-28",                                           # 中秋
    "2026-10-09","2026-10-10",                              # 國慶補假+國慶
}

def is_holiday(date=None):
    """判斷是否為台灣股市休市日（週末或國定假日）"""
    if date is None:
        date = datetime.date.today()
    if isinstance(date, datetime.datetime):
        date = date.date()
    if date.weekday() >= 5:
        return True
    return date.strftime("%Y-%m-%d") in TW_HOLIDAYS


# ─────────────────────────────────────────────
# 資料夾初始化
# ─────────────────────────────────────────────
def init_dirs():
    for d in [REPORT_DIR, DAILY_DIR, HOURLY_DIR]:
        os.makedirs(d, exist_ok=True)


# ─────────────────────────────────────────────
# 市場狀態
# ─────────────────────────────────────────────
def is_market_open():
    now = datetime.datetime.now()
    if is_holiday(now):
        return False
    t = now.time()
    return datetime.time(9, 0) <= t <= datetime.time(13, 30)


# ─────────────────────────────────────────────
# 抓取處置股清單（TWSE）
# ─────────────────────────────────────────────
def fetch_disposition_list():
    """從 TWSE 抓取目前列為處置股的清單"""
    print("[1/5] 抓取 TWSE 處置股清單...")
    codes = []

    # TWSE 處置股公告 JSON API
    # 欄位順序: [序號, 日期, 股票代號, 股票名稱, 累計次數, 處置類別, 處置期間, 處置措施, 處置原因, 備註]
    try:
        url = "https://www.twse.com.tw/rwd/zh/announcement/punish"
        r = requests.get(url, timeout=15, verify=False,
                         params={"response": "json"},
                         headers={"User-Agent": "Mozilla/5.0"})
        data = r.json()
        if data.get("stat") == "OK" and data.get("data"):
            seen = set()
            for row in data["data"]:
                code   = str(row[2]).strip() if len(row) > 2 else ""
                name   = str(row[3]).strip() if len(row) > 3 else ""
                reason = str(row[5]).strip() if len(row) > 5 else ""
                period = str(row[6]).strip() if len(row) > 6 else ""
                if len(code) == 4 and code.isdigit() and code not in seen:
                    seen.add(code)
                    codes.append({"code": code, "name": name,
                                  "reason": f"{reason} {period}".strip()})
            print(f"    TWSE API：共 {len(codes)} 檔處置股")
            return codes
    except Exception as e:
        print(f"    [API] 失敗: {e}")

    # 備用：OTC 處置股（TPEx）
    try:
        url = "https://www.tpex.org.tw/web/bulletin/announcement/punish/punish_result.php"
        r = requests.get(url, timeout=15, verify=False,
                         headers={"User-Agent": "Mozilla/5.0"})
        r.encoding = "utf-8"
        tables = pd.read_html(io.StringIO(r.text))
        seen = set()
        for df in tables:
            for _, row in df.iterrows():
                for val in row:
                    s = str(val).strip()
                    if len(s) == 4 and s.isdigit() and s not in seen:
                        seen.add(s)
                        codes.append({"code": s, "name": "", "reason": "上櫃處置"})
        if codes:
            print(f"    TPEx 備用：{len(codes)} 檔")
            return codes
    except Exception as e:
        print(f"    [TPEx] 失敗: {e}")

    print("    [警告] 無法取得處置股清單，使用固定測試清單")
    return [
        {"code": "2603", "name": "長榮", "reason": "測試用"},
        {"code": "2615", "name": "萬海", "reason": "測試用"},
        {"code": "5347", "name": "世界", "reason": "測試用"},
    ]


# ─────────────────────────────────────────────
# 技術指標（60分線為主，日線輔助）
# ─────────────────────────────────────────────
def _calc_indicators(df):
    if df is None or len(df) < 20:
        return None
    close  = df["Close"].squeeze()
    high   = df["High"].squeeze()
    low    = df["Low"].squeeze()
    volume = df["Volume"].squeeze()

    ma5  = ta.trend.sma_indicator(close, window=5)
    ma10 = ta.trend.sma_indicator(close, window=10)
    ma20 = ta.trend.sma_indicator(close, window=min(20, len(close)-1))

    rsi  = ta.momentum.rsi(close, window=14)

    macd_obj  = ta.trend.MACD(close)
    macd_hist = macd_obj.macd_diff()

    stoch = ta.momentum.StochasticOscillator(high, low, close, window=9, smooth_window=3)
    k_s   = stoch.stoch()
    d_s   = stoch.stoch_signal()

    bb    = ta.volatility.BollingerBands(close, window=20, window_dev=2)
    bbu   = bb.bollinger_hband()
    bbl   = bb.bollinger_lband()

    vol_ma20 = volume.rolling(20).mean()

    return dict(
        c=close.iloc[-1],   c1=close.iloc[-2],
        h=high.iloc[-1],    l=low.iloc[-1],
        m5=ma5.iloc[-1],    m10=ma10.iloc[-1],  m20=ma20.iloc[-1],
        r=rsi.iloc[-1],
        k=k_s.iloc[-1],     d=d_s.iloc[-1],
        k1=k_s.iloc[-2],    d1=d_s.iloc[-2],
        mch=macd_hist.iloc[-1],   mch1=macd_hist.iloc[-2],
        vol=volume.iloc[-1],      vma=vol_ma20.iloc[-1],
        bbu=bbu.iloc[-1],         bbl=bbl.iloc[-1],
        close_series=close,       high_series=high, low_series=low,
    )


def _score_disposition(ind):
    """
    處置股當沖評分（0-100）
    重點：量增、短期動能、不追高
    """
    score, signals = 0, []
    c, c1, k, d, k1, d1 = ind["c"], ind["c1"], ind["k"], ind["d"], ind["k1"], ind["d1"]
    r, mch, mch1         = ind["r"], ind["mch"], ind["mch1"]
    m5, m10, m20         = ind["m5"], ind["m10"], ind["m20"]
    vol, vma, bbu, bbl   = ind["vol"], ind["vma"], ind["bbu"], ind["bbl"]

    # ── 量能確認（處置股最重要）────────────────
    if vma > 0:
        vr = vol / vma
        if vr >= 3.0:
            score += 25; signals.append(f"爆量{vr:.1f}x")
        elif vr >= 2.0:
            score += 15; signals.append(f"量增{vr:.1f}x")
        elif vr >= 1.5:
            score += 8;  signals.append(f"量增{vr:.1f}x")
        elif vr < 0.5:
            score -= 10  # 縮量不進

    # ── KD 黃金交叉（低檔更有效）────────────────
    if (k1 < d1) and (k > d):
        bonus = 25 if (k < 30 and d < 30) else 15
        score += bonus; signals.append("KD黃金交叉" + ("低檔" if k < 30 else ""))
    elif k < 20 and d < 20:
        score += 10; signals.append("KD極低")

    # ── MACD 翻正 ────────────────────────────────
    if mch > 0 and mch1 <= 0:
        score += 20; signals.append("MACD翻正")
    elif mch > mch1 and mch > 0:
        score += 8;  signals.append("MACD增強")

    # ── RSI 合理區間（不追高）────────────────────
    if 25 < r < 55:
        score += 10; signals.append(f"RSI={r:.0f}")
    elif r >= 70:
        score -= 20; signals.append("RSI過熱⚠")  # 處置股過熱不進
    elif r < 20:
        score += 5;  signals.append(f"RSI超賣{r:.0f}")

    # ── 均線多頭排列 ─────────────────────────────
    if m5 > m10 > m20:
        score += 10; signals.append("均線多頭")
    elif m5 > m10:
        score += 5

    # ── 布林帶位置 ───────────────────────────────
    bb_range = bbu - bbl
    if bb_range > 0:
        pos = (c - bbl) / bb_range  # 0=下軌, 1=上軌
        if pos < 0.25:
            score += 12; signals.append("布林低檔")
        elif pos > 0.85:
            score -= 15; signals.append("近上軌⚠")  # 處置股不追高

    # ── 昨日低點守住 ─────────────────────────────
    if c > c1:
        score += 5; signals.append("今強於昨")

    return max(0, min(100, score)), signals


# ─────────────────────────────────────────────
# 分析單檔處置股
# ─────────────────────────────────────────────
def analyze_one(item):
    code   = item["code"]
    name   = item["name"]
    reason = item.get("reason", "")
    try:
        ticker = yf.Ticker(code + ".TW")

        # 日線（判斷趨勢）
        df_d = ticker.history(period="3mo", interval="1d")
        ind_d = _calc_indicators(df_d)
        if ind_d is None:
            return None

        d_score, d_signals = _score_disposition(ind_d)

        # 60 分線（進場時機）
        df_h = ticker.history(period="30d", interval="60m")
        ind_h = _calc_indicators(df_h)
        h_score, h_signals = 0, "無資料"
        if ind_h:
            for key in ("r", "k", "d", "k1", "d1", "mch", "mch1"):
                val = ind_h.get(key)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    ind_h = None
                    break
        if ind_h:
            h_score, h_sigs = _score_disposition(ind_h)
            h_signals = ", ".join(h_sigs) if h_sigs else "訊號未觸發"

        # 綜合分（盤中偏重小時線）
        if is_market_open() and h_score > 0:
            combined = round(h_score * 0.6 + d_score * 0.4)
        elif h_score > 0:
            combined = round(d_score * 0.7 + h_score * 0.3)
        else:
            combined = d_score

        close = ind_d["close_series"]
        wchg  = (ind_d["c"] - close.iloc[-6])  / close.iloc[-6]  * 100 if len(close) >= 6  else 0
        mchg  = (ind_d["c"] - close.iloc[-22]) / close.iloc[-22] * 100 if len(close) >= 22 else 0

        # 停損評估：若日線低點 <= 現價 * 0.95 出現過，風險較高
        low_series = ind_d.get("low_series", pd.Series())
        stop_risk  = "高" if len(low_series) >= 3 and low_series.iloc[-3:].min() < ind_d["c"] * 0.95 else "低"

        return dict(
            code=code, name=name, reason=reason,
            price=round(float(ind_d["c"]), 2),
            week_chg=round(float(wchg), 2),
            month_chg=round(float(mchg), 2),
            rsi=round(float(ind_d["r"]), 1),
            k=round(float(ind_d["k"]), 1),
            d=round(float(ind_d["d"]), 1),
            vol_ratio=round(float(ind_d["vol"] / ind_d["vma"]), 2) if ind_d["vma"] > 0 else 0,
            d_score=d_score, d_signals=", ".join(d_signals) if d_signals else "-",
            h_score=h_score, h_signals=h_signals,
            combined=combined,
            stop_risk=stop_risk,
        )
    except Exception as e:
        return None


# ─────────────────────────────────────────────
# 當沖模擬下單系統
# ─────────────────────────────────────────────
def load_sim():
    try:
        if os.path.exists(SIM_JSON):
            with open(SIM_JSON, "r", encoding="utf-8") as f:
                return json.load(f)
    except:
        pass
    return {"open": [], "closed": []}

def save_sim(sim):
    with open(SIM_JSON, "w", encoding="utf-8") as f:
        json.dump(sim, f, ensure_ascii=False, indent=2)

def sim_update(results):
    """
    當沖模擬：
    - 進場：今日開盤（或排程時段）
    - 出場：當日收盤 or 停損 -5%
    - 停損判斷：若日低 <= 進場價 * 0.95 → 以進場價 * 0.95 出場
    """
    now   = datetime.datetime.now()
    today = now.strftime("%Y-%m-%d")
    hour  = now.hour
    if hour < 9:  hour = 9
    if hour > 13: hour = 13
    slot  = f"{hour:02d}:00"

    price_map = {r["code"]: r for r in results}
    sim = load_sim()

    # ── 1. 結算當日持倉 ───────────────────────────
    still_open = []
    for pos in sim["open"]:
        if pos["entry_date"] != today:
            # 隔夜倉（理論上不應出現）→ 強制以記錄價出場
            curr_info  = price_map.get(pos["code"])
            curr_price = curr_info["price"] if curr_info else pos["entry_price"]
            stop_price = round(pos["entry_price"] * (1 + STOP_LOSS_PCT / 100), 2)
            # 判斷是否曾觸停損
            exit_price = stop_price if curr_price < stop_price else curr_price
            ret_pct    = round((exit_price - pos["entry_price"]) / pos["entry_price"] * 100, 2)
            note = "強制出場(隔夜)" if exit_price != stop_price else "停損-5%"
            sim["closed"].append({
                **{k: v for k, v in pos.items() if k not in ("curr_price","curr_pct")},
                "exit_date": today, "exit_price": exit_price,
                "return_pct": ret_pct, "exit_note": note,
            })
        else:
            # 同日持倉：更新浮動損益
            curr_info  = price_map.get(pos["code"])
            curr_price = curr_info["price"] if curr_info else pos["entry_price"]
            stop_price = round(pos["entry_price"] * (1 + STOP_LOSS_PCT / 100), 2)
            ret_pct    = round((curr_price - pos["entry_price"]) / pos["entry_price"] * 100, 2)

            # 若已觸及停損（日低曾跌破）
            if curr_price <= stop_price:
                sim["closed"].append({
                    **{k: v for k, v in pos.items() if k not in ("curr_price","curr_pct")},
                    "exit_date": today, "exit_price": stop_price,
                    "return_pct": round(STOP_LOSS_PCT, 2), "exit_note": "停損-5%",
                })
            # 13:00 後視為日末，結算
            elif hour >= 13:
                sim["closed"].append({
                    **{k: v for k, v in pos.items() if k not in ("curr_price","curr_pct")},
                    "exit_date": today, "exit_price": curr_price,
                    "return_pct": ret_pct, "exit_note": "收盤出場",
                })
            else:
                pos["curr_price"] = curr_price
                pos["curr_pct"]   = ret_pct
                still_open.append(pos)

    sim["open"] = still_open

    # ── 2. 新進場（今日該時段）────────────────────
    open_count = len(sim["open"])
    entered_today = {p["code"] for p in sim["open"] if p["entry_date"] == today}
    entered_today |= {p["code"] for p in sim["closed"] if p["entry_date"] == today}

    candidates = sorted(
        [r for r in results
         if r["combined"] >= MIN_SCORE
         and r["code"] not in entered_today
         and r.get("stop_risk", "高") != "高"],   # 停損風險高的跳過
        key=lambda x: x["combined"], reverse=True
    )

    new_count = 0
    for r in candidates:
        if open_count >= MAX_POSITIONS or new_count >= MAX_NEW_PER_RUN:
            break
        sim["open"].append({
            "code":          r["code"],
            "name":          r["name"],
            "entry_date":    today,
            "entry_slot":    slot,
            "entry_price":   r["price"],
            "curr_price":    r["price"],
            "curr_pct":      0.0,
            "entry_score":   r["combined"],
            "entry_signals": r["d_signals"],
            "stop_price":    round(r["price"] * (1 + STOP_LOSS_PCT / 100), 2),
            "pos_pct":       POS_PCT,
        })
        open_count += 1
        new_count  += 1

    sim["closed"] = sim["closed"][-200:]
    save_sim(sim)
    print(f"    [處置股模擬] [{slot}] 新增 {new_count} 檔  持倉 {len(sim['open'])} 檔  累計出場 {len(sim['closed'])} 筆")
    return sim


# ─────────────────────────────────────────────
# 績效統計
# ─────────────────────────────────────────────
def _calc_stats(closed):
    if not closed:
        return dict(total=0, wins=0, wr=0, avg_ret=0, avg_win=0, avg_loss=0, total_ret=0)
    wins  = [c for c in closed if c["return_pct"] > 0]
    loses = [c for c in closed if c["return_pct"] <= 0]
    total = len(closed)
    wr    = round(len(wins) / total * 100)
    avg_ret  = round(sum(c["return_pct"] for c in closed) / total, 2)
    avg_win  = round(sum(c["return_pct"] for c in wins)  / max(1, len(wins)),  2)
    avg_loss = round(sum(c["return_pct"] for c in loses) / max(1, len(loses)), 2)
    total_ret = round(sum(c["return_pct"] * POS_PCT for c in closed), 2)  # 以15%倉位換算總報酬%
    return dict(total=total, wins=len(wins), wr=wr,
                avg_ret=avg_ret, avg_win=avg_win, avg_loss=avg_loss, total_ret=total_ret)


# ─────────────────────────────────────────────
# 產生 HTML 報告
# ─────────────────────────────────────────────
def generate_html(results, today_str, run_time_str, market_open, sim):
    closed = sim.get("closed", [])
    opened = sim.get("open", [])
    stats  = _calc_stats(closed)

    td  = "padding:5px 8px;border:1px solid #ddd;text-align:center;font-size:.85em"
    tdl = "padding:5px 8px;border:1px solid #ddd;text-align:left;font-size:.85em"
    th  = "padding:7px 8px;border:1px solid #b35900;background:#b35900;color:#fff;white-space:nowrap;font-size:.83em"

    def badge(s):
        if s is None:
            return '<span style="background:#ddd;color:#999;padding:1px 6px;border-radius:3px">—</span>'
        bg = "#c0392b" if s >= 70 else ("#e67e22" if s >= 50 else "#aaa")
        return f'<span style="background:{bg};color:#fff;padding:1px 7px;border-radius:3px;font-weight:bold">{s}</span>'

    def chg_td(v):
        col = "#c0392b" if v > 0 else ("#27ae60" if v < 0 else "#333")
        return f'<td style="color:{col};{td[td.index(":")+1:]}">{v:+.2f}%</td>'

    def ret_col(v):
        return "#c0392b" if v > 0 else ("#27ae60" if v < 0 else "#555")

    # 候選股表格
    sorted_res = sorted(results, key=lambda x: x["combined"], reverse=True)
    rows = ""
    for i, r in enumerate(sorted_res, 1):
        code = r["code"]
        risk_bg = ' style="background:#fff0f0"' if r.get("stop_risk") == "高" else ""
        tv  = f"https://www.tradingview.com/chart/?symbol=TWSE%3A{code}"
        gi  = f"https://goodinfo.tw/tw/ShowK_Chart.asp?STOCK_ID={code}"
        ls  = "font-size:.76em;padding:1px 5px;border-radius:2px;text-decoration:none;margin:1px;display:inline-block;color:#fff"
        links = (f'<a href="{tv}" target="_blank" style="{ls};background:#1565c0">TV</a>'
                 f'<a href="{gi}" target="_blank" style="{ls};background:#2e7d32">K線</a>')
        rows += f"""<tr{risk_bg}>
          <td style="{td}">{i}</td>
          <td style="{td}"><a href="{gi}" target="_blank"
               style="font-weight:bold;color:#7b1900;text-decoration:none">{code}</a><br>{links}</td>
          <td style="{tdl}">{r['name']}</td>
          <td style="{td};font-size:.78em;color:#888">{r.get('reason','')}</td>
          <td style="{td}"><b>{r['price']}</b></td>
          <td style="color:{'#c0392b' if r['week_chg']>0 else '#27ae60'};{td[td.index(':')+1:]}">{r['week_chg']:+.2f}%</td>
          <td style="{td}">{r['rsi']}</td>
          <td style="{td}">{r['k']}/{r['d']}</td>
          <td style="{td}">{r['vol_ratio']:.1f}x</td>
          <td style="{td};color:{'#c0392b' if r.get('stop_risk')=='高' else '#27ae60'}">{r.get('stop_risk','—')}</td>
          <td style="{td}">{badge(r['d_score'])}<br><span style="font-size:.73em;color:#666">{r['d_signals']}</span></td>
          <td style="{td}">{badge(r['h_score'])}<br><span style="font-size:.73em;color:#666">{r['h_signals']}</span></td>
          <td style="{td}">{badge(r['combined'])}</td>
        </tr>"""

    # 持倉中
    open_rows = ""
    for p in sorted(opened, key=lambda x: x.get("curr_pct", 0), reverse=True):
        cc = ret_col(p.get("curr_pct", 0))
        _c = p['code']
        _tv = f"https://www.tradingview.com/chart/?symbol=TWSE%3A{_c}"
        _gi = f"https://goodinfo.tw/tw/ShowK_Chart.asp?STOCK_ID={_c}"
        _ls = "font-size:.74em;padding:1px 4px;border-radius:2px;text-decoration:none;margin:1px;display:inline-block;color:#fff"
        _links = (f'<a href="{_tv}" target="_blank" style="{_ls};background:#1565c0">TV</a>'
                  f'<a href="{_gi}" target="_blank" style="{_ls};background:#2e7d32">K線</a>')
        open_rows += f"""<tr>
          <td style="{td}"><a href="{_gi}" target="_blank" style="font-weight:bold;color:#7b1900;text-decoration:none">{_c}</a><br>{_links}</td>
          <td style="{tdl}">{p['name']}</td>
          <td style="{td}">{p['entry_slot']}</td>
          <td style="{td}">{p['entry_price']}</td>
          <td style="{td}">{p.get('curr_price', p['entry_price'])}</td>
          <td style="{td};color:#e74c3c;font-weight:bold">停損：{p['stop_price']}</td>
          <td style="{td};color:{cc};font-weight:bold">{p.get('curr_pct', 0):+.2f}%</td>
          <td style="{td}">{p['entry_score']}</td>
        </tr>"""

    # 已出場
    closed_rows = ""
    for c in reversed(closed[-30:]):
        cc  = ret_col(c["return_pct"])
        note_col = "#c0392b" if "停損" in c.get("exit_note","") else "#27ae60"
        closed_rows += f"""<tr>
          <td style="{td}">{c.get('entry_slot','—')}</td>
          <td style="{td}">{c['code']}</td>
          <td style="{tdl}">{c['name']}</td>
          <td style="{td}">{c['entry_date']}</td>
          <td style="{td}">{c['entry_price']}</td>
          <td style="{td}">{c['exit_price']}</td>
          <td style="{td};color:{cc};font-weight:bold">{c['return_pct']:+.2f}%</td>
          <td style="{td};color:{note_col};font-size:.8em">{c.get('exit_note','')}</td>
        </tr>"""

    mstr = ('<span style="color:#28a745;font-weight:bold">● 盤中</span>'
            if market_open else '<span style="color:#aaa">● 收盤</span>')

    wr_col = "#c0392b" if stats["wr"] >= 50 else "#888"

    return f"""<!DOCTYPE html>
<html lang="zh-Hant">
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="3600">
<title>處置股當沖 {today_str}</title>
<style>
  body{{font-family:'Microsoft JhengHei',Arial,sans-serif;margin:16px;background:#fdf4ee}}
  h1{{color:#7b1900;margin-bottom:4px}}
  .bar{{display:flex;gap:18px;align-items:center;flex-wrap:wrap;margin-bottom:10px;font-size:.9em}}
  .rule-box{{background:#fff8f0;border:2px solid #e67e22;border-radius:6px;padding:10px 16px;margin:10px 0;font-size:.85em}}
  .stat-box{{display:flex;gap:12px;flex-wrap:wrap;margin:10px 0}}
  .stat{{background:#fff;border:1px solid #e0c8b0;border-radius:6px;padding:8px 14px;text-align:center;min-width:90px}}
  .stat .val{{font-size:1.4em;font-weight:bold;color:#7b1900}}
  .stat .lab{{font-size:.75em;color:#888;margin-top:2px}}
  table{{border-collapse:collapse;width:100%;background:#fff;box-shadow:0 1px 4px rgba(0,0,0,.1);border-radius:6px;overflow:hidden;margin-bottom:14px}}
  th,td{{border:1px solid #e8d5c4;padding:6px 8px;text-align:center;font-size:.87em}}
  th{{background:#b35900;color:#fff;position:sticky;top:0;z-index:1;white-space:nowrap}}
  tr:nth-child(even){{background:#fdf0e8}}
  tr:hover{{background:#ffe8d0}}
  details{{margin:12px 0;background:#fff;border:1px solid #e0c8b0;border-radius:6px;padding:10px 14px}}
  summary{{font-weight:bold;color:#7b1900;cursor:pointer;font-size:.97em}}
  footer{{color:#aaa;font-size:.78em;margin-top:12px}}
  .compare-btn{{display:inline-block;padding:7px 18px;background:#1a5276;color:#fff;border-radius:5px;text-decoration:none;font-size:.87em;margin-top:6px}}
</style>
</head>
<body>
<h1>處置股當沖篩選 🎯</h1>
<div class="bar">
  <span>日期：{today_str}</span>
  <span>市場：{mstr}</span>
  <span>更新：{run_time_str}</span>
  <span>處置股數：<b>{len(results)}</b> 檔</span>
</div>

<div class="rule-box">
  <b>策略規則：</b>
  ✅ 每次 <b style="color:#c0392b">15%</b> 資金
  ✅ 停損 <b style="color:#c0392b">-5%</b> 必砍
  ✅ <b style="color:#c0392b">不留隔夜</b>（收盤前出場）
  ❌ 不 All in ／ 不攤平 ／ 不追新聞<br>
  <span style="color:#888;font-size:.88em">🔴底色 = 近期有觸低停損風險，謹慎；進場門檻：綜合分 ≥ {MIN_SCORE}</span>
</div>

<div class="stat-box">
  <div class="stat"><div class="val">{stats['total']}</div><div class="lab">總筆數</div></div>
  <div class="stat"><div class="val" style="color:{'#c0392b' if stats['wr']>=50 else '#888'}">{stats['wr']}%</div><div class="lab">勝率</div></div>
  <div class="stat"><div class="val" style="color:{'#c0392b' if stats['avg_ret']>0 else '#27ae60'}">{stats['avg_ret']:+.2f}%</div><div class="lab">平均報酬</div></div>
  <div class="stat"><div class="val" style="color:#c0392b">{stats['avg_win']:+.2f}%</div><div class="lab">平均獲利</div></div>
  <div class="stat"><div class="val" style="color:#27ae60">{stats['avg_loss']:+.2f}%</div><div class="lab">平均虧損</div></div>
  <div class="stat"><div class="val" style="color:{'#c0392b' if stats['total_ret']>0 else '#27ae60'}">{stats['total_ret']:+.2f}%</div><div class="lab">累計總報酬<br><span style="font-size:.8em;color:#aaa">(15%倉位換算)</span></div></div>
</div>

<a class="compare-btn" href="file:///{COMPARE_HTML.replace(os.sep,'/')}">📊 與 v2 工具比較績效</a>

<details open>
<summary>📋 持倉中（{len(opened)} 檔）</summary>
<table style="margin-top:8px">
<thead><tr>
  <th style="{th}">代號</th><th style="{th}">名稱</th><th style="{th}">進場時段</th>
  <th style="{th}">進場價</th><th style="{th}">現價</th><th style="{th}">停損線</th>
  <th style="{th}">浮動損益</th><th style="{th}">進場分</th>
</tr></thead>
<tbody>{open_rows if open_rows else f'<tr><td colspan="8" style="{td};color:#aaa">目前無持倉</td></tr>'}</tbody>
</table>
</details>

<details open>
<summary>📈 候選處置股（綜合評分排序）</summary>
<table style="margin-top:8px">
<thead><tr>
  <th style="{th}">#</th><th style="{th}">代號</th><th style="{th}">名稱</th>
  <th style="{th}">處置原因</th><th style="{th}">股價</th><th style="{th}">週漲幅</th>
  <th style="{th}">RSI</th><th style="{th}">KD</th><th style="{th}">量比</th>
  <th style="{th}">停損風險</th><th style="{th}">日線</th><th style="{th}">60分線</th>
  <th style="{th}">綜合分</th>
</tr></thead>
<tbody>{rows if rows else f'<tr><td colspan="13" style="{td};color:#aaa">目前無處置股資料</td></tr>'}</tbody>
</table>
</details>

<details>
<summary>📉 已出場紀錄（最近30筆）</summary>
<table style="margin-top:8px">
<thead><tr>
  <th style="{th}">進場時段</th><th style="{th}">代號</th><th style="{th}">名稱</th>
  <th style="{th}">進場日</th><th style="{th}">進場價</th><th style="{th}">出場價</th>
  <th style="{th}">報酬率</th><th style="{th}">出場原因</th>
</tr></thead>
<tbody>{closed_rows if closed_rows else f'<tr><td colspan="8" style="{td};color:#aaa">尚無出場紀錄</td></tr>'}</tbody>
</table>
</details>

<footer>
  資料來源：TWSE 處置股公告 + Yahoo Finance｜停損 {STOP_LOSS_PCT}%｜倉位 {int(POS_PCT*100)}%／檔｜最多 {MAX_POSITIONS} 檔同時持倉｜產生：{run_time_str}
</footer>
</body></html>"""


# ─────────────────────────────────────────────
# 比較報告（v2 vs 處置股）
# ─────────────────────────────────────────────
def generate_compare_html():
    """讀取兩套工具的 sim_trades.json，產生比較報告"""
    try:
        v2_sim = {"open": [], "closed": []}
        if os.path.exists(V2_SIM_JSON):
            with open(V2_SIM_JSON, "r", encoding="utf-8") as f:
                v2_sim = json.load(f)

        disp_sim = load_sim()

        def stats(closed, pos_pct=1.0):
            if not closed:
                return dict(total=0, wins=0, wr=0, avg_ret=0, avg_win=0, avg_loss=0, total_ret=0)
            wins  = [c for c in closed if c["return_pct"] > 0]
            loses = [c for c in closed if c["return_pct"] <= 0]
            t = len(closed)
            avg_ret  = round(sum(c["return_pct"] for c in closed) / t, 2)
            avg_win  = round(sum(c["return_pct"] for c in wins)  / max(1, len(wins)), 2)
            avg_loss = round(sum(c["return_pct"] for c in loses) / max(1, len(loses)), 2)
            total_ret = round(sum(c["return_pct"] * pos_pct for c in closed), 2)
            return dict(total=t, wins=len(wins), wr=round(len(wins)/t*100),
                        avg_ret=avg_ret, avg_win=avg_win, avg_loss=avg_loss, total_ret=total_ret)

        sv2   = stats(v2_sim.get("closed", []),   pos_pct=0.1)   # v2 假設 10% 倉位
        sdisp = stats(disp_sim.get("closed", []), pos_pct=POS_PCT)

        today_str = datetime.datetime.now().strftime("%Y/%m/%d %H:%M")

        def stat_block(name, s, color, url, hold_note):
            wr_c = "#c0392b" if s["wr"] >= 50 else "#888"
            ret_c = "#c0392b" if s["total_ret"] > 0 else "#27ae60"
            return f"""
<div style="flex:1;min-width:280px;background:#fff;border:2px solid {color};border-radius:8px;padding:16px">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
    <b style="color:{color};font-size:1.05em">{name}</b>
    <a href="{url}" style="font-size:.8em;color:#555;text-decoration:none">📄 查看報告</a>
  </div>
  <div style="font-size:.82em;color:#888;margin-bottom:10px">{hold_note}</div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px">
    <div style="text-align:center;padding:6px;background:#f8f8f8;border-radius:4px">
      <div style="font-size:1.3em;font-weight:bold;color:#333">{s['total']}</div>
      <div style="font-size:.72em;color:#888">總筆數</div>
    </div>
    <div style="text-align:center;padding:6px;background:#f8f8f8;border-radius:4px">
      <div style="font-size:1.3em;font-weight:bold;color:{wr_c}">{s['wr']}%</div>
      <div style="font-size:.72em;color:#888">勝率</div>
    </div>
    <div style="text-align:center;padding:6px;background:#f8f8f8;border-radius:4px">
      <div style="font-size:1.3em;font-weight:bold;color:{'#c0392b' if s['avg_ret']>0 else '#27ae60'}">{s['avg_ret']:+.2f}%</div>
      <div style="font-size:.72em;color:#888">平均報酬/筆</div>
    </div>
    <div style="text-align:center;padding:6px;background:#f8f8f8;border-radius:4px">
      <div style="font-size:1.3em;font-weight:bold;color:{ret_c}">{s['total_ret']:+.2f}%</div>
      <div style="font-size:.72em;color:#888">累計總報酬</div>
    </div>
    <div style="text-align:center;padding:6px;background:#f8f8f8;border-radius:4px">
      <div style="font-size:1.1em;font-weight:bold;color:#c0392b">{s['avg_win']:+.2f}%</div>
      <div style="font-size:.72em;color:#888">平均獲利</div>
    </div>
    <div style="text-align:center;padding:6px;background:#f8f8f8;border-radius:4px">
      <div style="font-size:1.1em;font-weight:bold;color:#27ae60">{s['avg_loss']:+.2f}%</div>
      <div style="font-size:.72em;color:#888">平均虧損</div>
    </div>
  </div>
</div>"""

        v2_url   = "file:///" + os.path.join(REPORT_BASE, "latest.html").replace(os.sep, "/")
        disp_url = "file:///" + LATEST_HTML.replace(os.sep, "/")

        winner = ""
        if sv2["total"] > 0 and sdisp["total"] > 0:
            if sdisp["total_ret"] > sv2["total_ret"]:
                winner = '<div style="text-align:center;padding:10px;background:#fff8e1;border:1px solid #f9ca24;border-radius:6px;margin-bottom:16px;font-size:.92em">🏆 目前累計報酬：<b style="color:#c0392b">處置股當沖</b> 領先</div>'
            elif sv2["total_ret"] > sdisp["total_ret"]:
                winner = '<div style="text-align:center;padding:10px;background:#fff8e1;border:1px solid #f9ca24;border-radius:6px;margin-bottom:16px;font-size:.92em">🏆 目前累計報酬：<b style="color:#003366">選股 v2</b> 領先</div>'
            else:
                winner = '<div style="text-align:center;padding:10px;background:#eee;border-radius:6px;margin-bottom:16px;font-size:.92em">🤝 兩套工具目前持平</div>'

        html = f"""<!DOCTYPE html>
<html lang="zh-Hant">
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="3600">
<title>選股績效比較</title>
<style>
  body{{font-family:'Microsoft JhengHei',Arial,sans-serif;margin:20px;background:#f4f6f9}}
  h1{{color:#1a1a2e;margin-bottom:4px}}
</style>
</head>
<body>
<h1>📊 兩套選股工具績效比較</h1>
<p style="color:#888;font-size:.88em;margin-bottom:16px">更新：{today_str}　｜　模擬績效，僅供參考，不構成投資建議</p>
{winner}
<div style="display:flex;gap:16px;flex-wrap:wrap">
  {stat_block("選股 v2（波段/短線）", sv2, "#003366", v2_url, "持有 5 個交易日出場｜進場門檻 60 分｜假設 10% 倉位")}
  {stat_block("處置股當沖", sdisp, "#b35900", disp_url, "當日收盤出場｜停損 -5%｜15% 倉位｜最多 3 檔同時持倉")}
</div>
<p style="color:#aaa;font-size:.75em;margin-top:20px">
  ⚠ 模擬交易不含手續費/交易稅（台股來回約 0.6%），實際報酬請自行扣除。<br>
  兩套工具的「累計總報酬」皆以各自倉位比例計算（v2=10%，處置股=15%），供同等基準比較。
</p>
</body></html>"""

        with open(COMPARE_HTML, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"    比較報告：{COMPARE_HTML}")
    except Exception as e:
        print(f"    [警告] 比較報告產生失敗: {e}")


# ─────────────────────────────────────────────
# 報告儲存
# ─────────────────────────────────────────────
def save_reports(html, now):
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H%M")
    hourly_path = os.path.join(HOURLY_DIR, f"{date_str}_{time_str}.html")
    daily_path  = os.path.join(DAILY_DIR,  f"{date_str}.html")
    for path, content in [(hourly_path, html), (daily_path, html), (LATEST_HTML, html)]:
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
    print(f"    hourly : {hourly_path}")
    print(f"    latest : {LATEST_HTML}")
    return hourly_path


# ─────────────────────────────────────────────
# Email
# ─────────────────────────────────────────────
def send_email(results, today_str, run_time_str, market_open, sim, stats):
    try:
        if not os.path.exists(EMAIL_CFG):
            return
        with open(EMAIL_CFG, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        sender    = cfg["sender_email"]
        password  = cfg["sender_app_password"]
        import re
        _recip = cfg.get("recipient_email", "")
        if isinstance(_recip, list):
            receivers = [v.strip() for v in _recip if v.strip()]
        else:
            receivers = [v.strip() for v in re.split(r"[,;]", str(_recip)) if v.strip()]
        if not receivers:
            return

        top = sorted(results, key=lambda x: x["combined"], reverse=True)
        top1_str = f"{top[0]['code']}{top[0]['name']}({top[0]['combined']}分)" if top else "—"
        subject  = f"[處置股當沖] {today_str} {run_time_str} 勝率{stats['wr']}% 今日第1名:{top1_str}"

        td = "padding:6px 8px;border:1px solid #ddd;text-align:center;font-size:.85em"
        rows = ""
        for i, r in enumerate(top[:20], 1):
            rc = "#c0392b" if r["combined"] >= 70 else ("#e67e22" if r["combined"] >= 50 else "#aaa")
            rows += f"""<tr>
              <td style="{td}">{i}</td>
              <td style="{td}"><b style="color:#7b1900">{r['code']}</b></td>
              <td style="{td}">{r['name']}</td>
              <td style="{td}">{r['price']}</td>
              <td style="{td}">{r['vol_ratio']:.1f}x</td>
              <td style="{td}"><b style="color:{rc}">{r['combined']}</b></td>
            </tr>"""

        body = f"""<html><head><meta charset="UTF-8"></head>
<body style="font-family:'Microsoft JhengHei',Arial,sans-serif;padding:16px">
<h2 style="color:#7b1900">處置股當沖篩選 {today_str} {run_time_str}</h2>
<p>勝率：<b>{stats['wr']}%</b>　平均報酬：<b>{stats['avg_ret']:+.2f}%</b>　累計：<b>{stats['total_ret']:+.2f}%</b>（{stats['total']}筆）</p>
<table style="border-collapse:collapse;width:100%;background:#fff">
<thead><tr>
  <th style="padding:7px;border:1px solid #b35900;background:#b35900;color:#fff">#</th>
  <th style="padding:7px;border:1px solid #b35900;background:#b35900;color:#fff">代號</th>
  <th style="padding:7px;border:1px solid #b35900;background:#b35900;color:#fff">名稱</th>
  <th style="padding:7px;border:1px solid #b35900;background:#b35900;color:#fff">股價</th>
  <th style="padding:7px;border:1px solid #b35900;background:#b35900;color:#fff">量比</th>
  <th style="padding:7px;border:1px solid #b35900;background:#b35900;color:#fff">綜合分</th>
</tr></thead>
<tbody>{rows}</tbody>
</table>
<p style="color:#aaa;font-size:.78em;margin-top:12px">停損-5%｜當沖｜15%倉位｜不構成投資建議</p>
</body></html>"""

        msg = MIMEText(body, "html", "utf-8")
        msg["Subject"] = Header(subject, "utf-8")
        msg["From"]    = sender
        msg["To"]      = ", ".join(receivers)
        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as s:
            s.login(sender, password)
            s.send_message(msg)
        print(f"    email 已寄出 -> {', '.join(receivers)}")
    except Exception as e:
        print(f"    [警告] email 失敗: {e}")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
def main():
    init_dirs()
    now          = datetime.datetime.now()
    today_str    = now.strftime("%Y/%m/%d")
    run_time_str = now.strftime("%H:%M")

    # ── 假日檢查：國定假日或週末直接結束 ──────────
    if is_holiday(now):
        print(f"=== 處置股當沖篩選工具  {today_str} {run_time_str} ===")
        print("    今日為國定假日或週末，股市休市，不執行選股。")
        return

    market_open  = is_market_open()

    print(f"=== 處置股當沖篩選工具  {today_str} {run_time_str} ===")
    print(f"    策略：停損{STOP_LOSS_PCT}%｜倉位{int(POS_PCT*100)}%｜不留隔夜｜最多{MAX_POSITIONS}檔")

    # ── 1. 抓處置股清單 ──────────────────────────
    disp_list = fetch_disposition_list()
    if not disp_list:
        print("[警告] 無法取得處置股清單，結束")
        return

    # ── 2. 分析每檔 ──────────────────────────────
    print(f"[2/5] 分析 {len(disp_list)} 檔處置股（{MAX_WORKERS} 執行緒）...")
    results, done = [], 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = {pool.submit(analyze_one, item): item for item in disp_list}
        for fut in concurrent.futures.as_completed(futs):
            done += 1
            if done % 5 == 0 or done == len(disp_list):
                print(f"    進度: {done}/{len(disp_list)}")
            r = fut.result()
            if r:
                results.append(r)

    print(f"    完成：{len(results)} 檔取得資料")
    if not results:
        print("[警告] 無有效資料")
        return

    # ── 3. 模擬下單 ──────────────────────────────
    print("[3/5] 更新模擬倉位...")
    sim   = sim_update(results)
    stats = _calc_stats(sim.get("closed", []))

    # ── 4. 產生報告 ──────────────────────────────
    print("[4/5] 產生 HTML 報告...")
    html      = generate_html(results, today_str, run_time_str, market_open, sim)
    save_reports(html, now)

    # ── 5. 比較報告 + Email ───────────────────────
    print("[5/5] 產生比較報告 + 寄送 Email...")
    generate_compare_html()
    send_email(results, today_str, run_time_str, market_open, sim, stats)

    print(f"\n[完成] 報告位置: {LATEST_HTML}")


if __name__ == "__main__":
    main()
