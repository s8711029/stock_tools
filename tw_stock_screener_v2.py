#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
台股每日選股推薦工具 v2
- 雙模式：日線（波段）+ 60分線（短線）
- 每小時盤中更新，顯示評分變化
- 多執行緒加速下載
- 自動整理報告資料夾 + 寄送 Email
"""

import sys, os, warnings, datetime, json, webbrowser
import smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text      import MIMEText
from email.mime.base      import MIMEBase
from email               import encoders
from email.header        import Header
import requests, io
import pandas as pd
import numpy as np
import yfinance as yf
import ta
import concurrent.futures

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────
# 路徑設定
# ─────────────────────────────────────────────
TOOLS_DIR   = os.path.dirname(os.path.abspath(__file__))
DESKTOP     = os.path.dirname(TOOLS_DIR)
REPORT_DIR  = os.path.join(DESKTOP, "stock_reports")
DAILY_DIR   = os.path.join(REPORT_DIR, "daily")
HOURLY_DIR  = os.path.join(REPORT_DIR, "hourly")
LATEST_HTML = os.path.join(REPORT_DIR, "latest.html")
PREV_JSON   = os.path.join(REPORT_DIR, "prev_scores.json")
SIM_JSON       = os.path.join(REPORT_DIR, "sim_trades.json")
FUND_CACHE     = os.path.join(REPORT_DIR, "fundamental_cache.json")
WATCHLIST_JSON = os.path.join(REPORT_DIR, "candidate_watchlist.json")
EMAIL_CFG   = os.path.join(DESKTOP, "stock_email_config.json")

# ─────────────────────────────────────────────
# 分析設定
# ─────────────────────────────────────────────
TOP_N            = 10
MAX_WORKERS      = 10   # 並行執行緒
PASS1_THRESHOLD  = 30   # 日線分 >= 此門檻才進入第二階段

D_RSI_MAX = 60;  D_RSI_MIN = 25;  D_VOL_SURGE = 1.5
H_RSI_MAX = 65;  H_RSI_MIN = 20;  H_VOL_SURGE = 1.8

# 模擬下單設定
SIM_ENTRY_SCORE   = 60    # 綜合分 >= 此值才模擬進場
SIM_MAX_SCORE     = 85    # 綜合分 <= 此值（過高分反而追高，排除）
SIM_VOL_RATIO_MIN = 2.0   # 量增倍數門檻（vol/vma >= 此值才進場）
SIM_SKIP_SLOTS    = set()      # 暫無暫停時段（10:00 已恢復開倉）
SIM_HOLD_DAYS     = 5     # 持有幾個交易日後自動出場
SIM_MAX_OPEN      = 10    # 最多同時持有幾檔
SIM_MAX_NEW       = 3     # 每次最多新增幾檔（同日多次執行不重複進場）


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
# 上次結果讀寫
# ─────────────────────────────────────────────
def load_prev():
    try:
        if os.path.exists(PREV_JSON):
            with open(PREV_JSON, "r", encoding="utf-8") as f:
                return {r["code"]: r for r in json.load(f)}
    except:
        pass
    return {}

def save_curr(results):
    try:
        with open(PREV_JSON, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False)
    except:
        pass


# ─────────────────────────────────────────────
# 月營收基本面（FinMind 免費 API）
# ─────────────────────────────────────────────
def _fetch_revenue_one(code):
    """抓取單檔近13個月營收，計算近3個月 YoY 平均"""
    try:
        start = (datetime.date.today() - datetime.timedelta(days=420)).strftime("%Y-%m-%d")
        url = (f"https://api.finmindtrade.com/api/v4/data"
               f"?dataset=TaiwanStockMonthRevenue&data_id={code}&start_date={start}")
        r = requests.get(url, timeout=12, headers={"User-Agent": "Mozilla/5.0"})
        data = r.json()
        if data.get("status") != 200 or not data.get("data"):
            return None
        recs = sorted(data["data"], key=lambda x: (x["revenue_year"], x["revenue_month"]))
        if len(recs) < 4:
            return None

        yoy_list = []
        for i in range(max(0, len(recs) - 3), len(recs)):
            cur = recs[i]
            prev = next((x for x in recs
                         if x["revenue_year"]  == cur["revenue_year"] - 1
                         and x["revenue_month"] == cur["revenue_month"]), None)
            if prev and prev["revenue"] > 0:
                yoy_list.append((cur["revenue"] - prev["revenue"]) / prev["revenue"] * 100)

        if not yoy_list:
            return None

        latest = recs[-1]
        return {
            "rev_yoy":   round(sum(yoy_list) / len(yoy_list), 1),   # 近3月平均YoY
            "rev_yoy1":  round(yoy_list[-1], 1),                     # 最新月YoY
            "rev_month": f"{latest['revenue_year']}/{latest['revenue_month']:02d}",
        }
    except:
        return None

def load_fund_cache():
    try:
        if os.path.exists(FUND_CACHE):
            with open(FUND_CACHE, "r", encoding="utf-8") as f:
                c = json.load(f)
            if c.get("_month") == datetime.date.today().strftime("%Y-%m"):
                return c
    except:
        pass
    return {}

def save_fund_cache(cache):
    cache["_month"] = datetime.date.today().strftime("%Y-%m")
    cache.pop("_date", None)   # 移除舊格式 key
    with open(FUND_CACHE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False)

def fetch_all_revenues(codes):
    """批次抓取月營收，每日快取避免重複呼叫"""
    cache   = load_fund_cache()
    missing = [c for c in codes if c not in cache]
    if missing:
        print(f"    [月營收] 抓取 {len(missing)} 檔（FinMind 免費版）...")
        done = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
            futs = {pool.submit(_fetch_revenue_one, c): c for c in missing}
            for fut in concurrent.futures.as_completed(futs):
                try:
                    code = futs[fut]
                    res  = fut.result()
                    if res:
                        cache[code] = res
                except Exception:
                    pass
                done += 1
                if done % 100 == 0 or done == len(missing):
                    print(f"    [月營收] 進度: {done}/{len(missing)}")
        save_fund_cache(cache)
        ok = sum(1 for c in missing if c in cache)
        print(f"    [月營收] 成功 {ok}/{len(missing)} 檔")
    return cache

# ─────────────────────────────────────────────
# 三大法人資料（外資/投信）
# ─────────────────────────────────────────────
def _get_recent_trading_days(n):
    """取最近 n 個交易日（排除週末與國定假日）"""
    days = []
    d = datetime.date.today()
    while len(days) < n:
        if not is_holiday(d):
            days.append(d)
        d -= datetime.timedelta(days=1)
    return days  # 最新的在前

def _fetch_t86_one_day(date_obj):
    """抓取單日 TWSE T86，回傳 {code: {foreign_net, trust_net}}（單位：張）"""
    try:
        url = (f"https://www.twse.com.tw/rwd/zh/fund/T86"
               f"?response=json&date={date_obj.strftime('%Y%m%d')}&selectType=ALL")
        r = requests.get(url, timeout=15, verify=False,
                         headers={"User-Agent": "Mozilla/5.0"})
        j = r.json()
        if j.get("stat") not in ("OK",) and j.get("status") not in ("OK",):
            return {}
        result = {}
        def parse_num(s):
            try:
                return int(str(s).replace(",", "").replace("+", "").strip() or "0")
            except:
                return 0
        for row in j.get("data", []):
            if len(row) < 11:
                continue
            code = str(row[0]).strip()          # [0] 證券代號
            foreign_net = parse_num(row[4])     # [4] 外資及陸資買賣超股數
            trust_net   = parse_num(row[10])    # [10] 投信買賣超股數
            result[code] = {
                "foreign_net": foreign_net // 1000,
                "trust_net":   trust_net   // 1000,
            }
        return result
    except:
        return {}

def _fetch_tpex_insti_one_day(date_obj):
    """抓取單日 TPEX 上櫃三大法人，回傳 {code: {foreign_net, trust_net}}（單位：張）"""
    try:
        date_str = date_obj.strftime("%Y/%m/%d")
        url = (f"https://www.tpex.org.tw/web/stock/3insti/daily_trade/"
               f"3itrade_hedge_result.php?l=zh-tw&o=json&se=AL&t=D&d={date_str}")
        r = requests.get(url, timeout=15, verify=False,
                         headers={"User-Agent": "Mozilla/5.0"})
        j = r.json()
        data = j.get("aaData", [])
        if not data:
            return {}
        result = {}
        def parse_num(s):
            try:
                return int(str(s).replace(",", "").replace("+", "").strip() or "0")
            except:
                return 0
        for row in data:
            if len(row) < 8:
                continue
            code = str(row[0]).strip()
            if not re.match(r"^\d{4}$", code):
                continue
            foreign_net = parse_num(row[4])   # 外資買賣超（千股）
            trust_net   = parse_num(row[7])   # 投信買賣超（千股）
            result[code] = {
                "foreign_net": foreign_net // 1000,
                "trust_net":   trust_net   // 1000,
            }
        return result
    except:
        return {}


def fetch_institutional(n_days=5):
    """
    抓最近 n_days 個交易日外資/投信買賣超（上市 T86 + 上櫃 TPEX），計算連續買超天數。
    回傳 {code: {foreign_net, trust_net, consec_foreign, consec_trust, foreign_flag, trust_flag}}
    """
    trading_days = _get_recent_trading_days(n_days)  # 最新在前
    print(f"    [法人] 抓取 {n_days} 個交易日資料（{trading_days[-1]} ~ {trading_days[0]}）...")
    # 從舊到新
    daily_data = []
    for d in reversed(trading_days):
        twse_day = _fetch_t86_one_day(d)
        tpex_day = _fetch_tpex_insti_one_day(d)
        # 合併上市+上櫃（上市優先，上櫃補入沒有的代號）
        merged = dict(twse_day)
        for code, v in tpex_day.items():
            if code not in merged:
                merged[code] = v
        daily_data.append(merged)

    all_codes = set()
    for dd in daily_data:
        all_codes.update(dd.keys())

    result = {}
    for code in all_codes:
        # 找最近一天有此股資料的紀錄（今日盤中 T86 尚未發布時自動往前找）
        latest = {"foreign_net": 0, "trust_net": 0}
        for dd in reversed(daily_data):   # reversed = 新→舊
            if code in dd:
                latest = dd[code]
                break
        # 連續外資買超天數（從最新往回數）
        consec_f = 0
        for dd in reversed(daily_data):
            val = dd.get(code, {}).get("foreign_net", 0)
            if val > 0:
                consec_f += 1
            else:
                break
        # 連續投信買超天數
        consec_t = 0
        for dd in reversed(daily_data):
            val = dd.get(code, {}).get("trust_net", 0)
            if val > 0:
                consec_t += 1
            else:
                break
        result[code] = {
            "foreign_net":    latest["foreign_net"],
            "trust_net":      latest["trust_net"],
            "consec_foreign": consec_f,
            "consec_trust":   consec_t,
            "foreign_flag":   consec_f >= 3,
            "trust_flag":     consec_t >= 3,
        }
    print(f"    [法人] 取得 {len(result)} 檔資料")
    return result


def rev_score(fund):
    """月營收評分（0~20分）及說明標籤"""
    if fund is None:
        return 0, ""
    yoy = fund.get("rev_yoy", 0)
    mo  = fund.get("rev_month", "")
    tag = f"{mo} YoY {yoy:+.1f}%"
    if   yoy >  20: return 20, tag
    elif yoy >  10: return 15, tag
    elif yoy >   0: return  8, tag
    elif yoy > -10: return  0, tag
    else:           return -5, tag


# ─────────────────────────────────────────────
# 台股清單
# ─────────────────────────────────────────────
def fetch_twse_list():
    print("[1/6] 抓取台灣上市股票清單...")
    try:
        url = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2"
        r = requests.get(url, timeout=15, verify=False,
                         headers={"User-Agent": "Mozilla/5.0"})
        r.encoding = "big5"
        tables = pd.read_html(io.StringIO(r.text))
        df = tables[0]
        df.columns = df.iloc[0]
        df = df.iloc[1:]
        df = df[df.iloc[:, 0].str.match(r"^\d{4}\s", na=False)]
        df["code"] = df.iloc[:, 0].str.split().str[0]
        df["name"] = df.iloc[:, 0].str.split().str[1]
        # 產業別通常在第4欄（index 4）
        df["sector"] = df.iloc[:, 4].fillna("").astype(str).str.strip()
        df = df[["code", "name", "sector"]].dropna(subset=["code","name"])
        df = df[df["code"].str.len() == 4]
        print(f"    共 {len(df)} 檔（全部納入第一階段掃描）")
        return df.reset_index(drop=True)
    except Exception as e:
        print(f"    [警告] 抓取失敗，使用內建清單: {e}")
        fallback = [
            ("2330","台積電","半導體業"),("2317","鴻海","電子零組件業"),
            ("2454","聯發科","半導體業"),("2881","富邦金","金融保險業"),
            ("2882","國泰金","金融保險業"),("2412","中華電","通信網路業"),
            ("2308","台達電","電子零組件業"),("3711","日月光投控","半導體業"),
            ("2382","廣達","電腦及週邊設備業"),("2303","聯電","半導體業"),
            ("2357","華碩","電腦及週邊設備業"),("2379","瑞昱","半導體業"),
            ("6505","台塑化","油電燃氣業"),("1301","台塑","塑膠工業"),
            ("1303","南亞","塑膠工業"),("2002","中鋼","鋼鐵工業"),
            ("2886","兆豐金","金融保險業"),("2891","中信金","金融保險業"),
            ("2884","玉山金","金融保險業"),("2885","元大金","金融保險業"),
        ]
        return pd.DataFrame(fallback, columns=["code","name","sector"])


def fetch_tpex_list():
    """抓取台灣上櫃股票清單（strMode=4），失敗最多 retry 3 次"""
    url = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=4"
    for attempt in range(1, 4):
        try:
            r = requests.get(url, timeout=20, verify=False,
                             headers={"User-Agent": "Mozilla/5.0"})
            r.encoding = "big5"
            tables = pd.read_html(io.StringIO(r.text))
            df = tables[0]
            df.columns = df.iloc[0]
            df = df.iloc[1:]
            df = df[df.iloc[:, 0].str.match(r"^\d{4}\s", na=False)]
            df["code"]   = df.iloc[:, 0].str.split().str[0]
            df["name"]   = df.iloc[:, 0].str.split().str[1]
            df["sector"] = df.iloc[:, 4].fillna("").astype(str).str.strip()
            df = df[["code", "name", "sector"]].dropna(subset=["code","name"])
            df = df[df["code"].str.len() == 4]
            if len(df) > 100:   # 正常應有 800+ 檔，< 100 視為異常
                print(f"    共 {len(df)} 檔上櫃股票")
                return df.reset_index(drop=True)
            print(f"    [警告] 上櫃清單筆數異常（{len(df)} 檔），第 {attempt} 次重試...")
        except Exception as e:
            print(f"    [警告] 上櫃清單第 {attempt} 次抓取失敗: {e}")
        import time as _time; _time.sleep(3 * attempt)
    print("    [錯誤] 上櫃清單抓取失敗（已重試 3 次），本次略過上櫃掃描")
    return pd.DataFrame(columns=["code","name","sector"])


# ─────────────────────────────────────────────
# 指標計算
# ─────────────────────────────────────────────
def _calc_indicators(df):
    if df is None or len(df) < 30:
        return None
    close  = df["Close"].squeeze()
    open_  = df["Open"].squeeze()
    high   = df["High"].squeeze()
    low    = df["Low"].squeeze()
    volume = df["Volume"].squeeze()

    n60 = min(60, len(close) - 1)
    ma5   = ta.trend.sma_indicator(close, window=5)
    ma10  = ta.trend.sma_indicator(close, window=10)
    ma20  = ta.trend.sma_indicator(close, window=20)
    ma60  = ta.trend.sma_indicator(close, window=n60)
    rsi_s = ta.momentum.rsi(close, window=14)

    macd_obj  = ta.trend.MACD(close)
    macd_hist = macd_obj.macd_diff()

    stoch = ta.momentum.StochasticOscillator(high, low, close, window=9, smooth_window=3)
    k_s   = stoch.stoch()
    d_s   = stoch.stoch_signal()

    bb    = ta.volatility.BollingerBands(close, window=20, window_dev=2)
    bbl_s = bb.bollinger_lband()
    vol_ma20 = volume.rolling(20).mean()

    return dict(
        c=close.iloc[-1], c1=close.iloc[-2], o=open_.iloc[-1],
        m5=ma5.iloc[-1], m10=ma10.iloc[-1], m20=ma20.iloc[-1], m60=ma60.iloc[-1],
        r=rsi_s.iloc[-1],
        k=k_s.iloc[-1], d=d_s.iloc[-1], k1=k_s.iloc[-2], d1=d_s.iloc[-2],
        mch=macd_hist.iloc[-1], mch1=macd_hist.iloc[-2],
        vol=volume.iloc[-1], vma=vol_ma20.iloc[-1],
        bbl=bbl_s.iloc[-1],
        close_series=close,
    )

def _score_from_ind(ind, rsi_min, rsi_max, vol_surge):
    score, signals = 0, []
    c,  k,  d,  k1, d1  = ind["c"],  ind["k"],  ind["d"],  ind["k1"], ind["d1"]
    r,  mch, mch1        = ind["r"],  ind["mch"], ind["mch1"]
    m5, m10, m20, m60    = ind["m5"], ind["m10"], ind["m20"], ind["m60"]
    vol, vma, bbl, c1    = ind["vol"], ind["vma"], ind["bbl"], ind["c1"]

    if (k1 < d1) and (k > d):
        score += 25; signals.append("KD黃金交叉")
    if k < 30 and d < 30:
        score += 10; signals.append("KD低檔")
    if mch > 0 and mch1 <= 0:
        score += 20; signals.append("MACD柱翻正")
    elif mch > mch1 and mch > 0:
        score += 8;  signals.append("MACD增強")
    if rsi_min < r < rsi_max:
        score += 10; signals.append(f"RSI={r:.0f}")
    elif r < rsi_min:
        score -= 5
    if m5 > m10 > m20:
        score += 5;  signals.append("均線多頭")   # 降權：已漲一段，追高風險高
    elif m5 > m20:
        score += 2
    if c > m20:
        score += 10; signals.append("站上MA20")
    if vma > 0 and vol > vma * vol_surge:
        score += 10; signals.append(f"量增{vol/vma:.1f}x")
    if c1 < bbl and c > bbl:
        score += 10; signals.append("布林反彈")
    if r > 72:
        score -= 15; signals.append("RSI過熱")
    if c < m60:
        score -= 5

    return max(0, min(100, score)), signals


# ─────────────────────────────────────────────
# 盤整型態偵測
# ─────────────────────────────────────────────
def _detect_consolidation(df):
    """
    偵測「量縮整理支撐」型態：
    上漲一段後出現大量紅K → 之後縮量整理 → 現價沒跌破大量紅K低點
    Returns: (bool, signal_str, support_price)
    """
    try:
        if df is None or len(df) < 30:
            return False, "", None
        close  = df["Close"].squeeze().reset_index(drop=True)
        open_  = df["Open"].squeeze().reset_index(drop=True)
        low    = df["Low"].squeeze().reset_index(drop=True)
        volume = df["Volume"].squeeze().reset_index(drop=True)
        vma20  = volume.rolling(20).mean().reset_index(drop=True)

        window = min(60, len(close))
        cl  = close.iloc[-window:]
        op  = open_.iloc[-window:]
        lo  = low.iloc[-window:]
        vol = volume.iloc[-window:]
        vma = vma20.iloc[-window:]

        search_end = len(cl) - 5
        if search_end < 10:
            return False, "", None

        # 找大量紅K（收漲 close>open，量>2倍均量），排除最近5根與最前5根
        best_idx, best_vol = None, 0
        for i in range(5, search_end):
            is_red   = float(cl.iloc[i]) > float(op.iloc[i])
            vma_val  = float(vma.iloc[i]) if float(vma.iloc[i]) > 0 else 1
            vol_val  = float(vol.iloc[i])
            if is_red and vol_val > vma_val * 2.0 and vol_val > best_vol:
                best_vol = vol_val
                best_idx = i

        if best_idx is None:
            return False, "", None

        support_low   = float(lo.iloc[best_idx])
        big_red_close = float(cl.iloc[best_idx])
        curr_price    = float(cl.iloc[-1])

        # 現價需低於大量紅K收盤（在整理中）
        if curr_price >= big_red_close:
            return False, "", None

        # 大量紅K之後收盤未跌破低點（1%容忍）
        after_cl = cl.iloc[best_idx + 1:]
        if (after_cl.values < support_low * 0.99).any():
            return False, "", None

        # 整理期縮量
        after_vol = vol.iloc[best_idx + 1:]
        if len(after_vol) > 0 and float(after_vol.mean()) > best_vol * 0.7:
            return False, "", None

        days_consol = len(after_cl)
        signal_str  = f"支撐${support_low:.1f}整理{days_consol}天"
        return True, signal_str, support_low
    except:
        return False, "", None


# ─────────────────────────────────────────────
# yfinance 代碼後綴：上市用 .TW，上櫃用 .TWO
# ─────────────────────────────────────────────
def _tw_ticker(code, market="上市"):
    return code + (".TWO" if market == "上櫃" else ".TW")


# ─────────────────────────────────────────────
# 回塑性分析買點規則（2026-05-12 retro_analysis 衍生）
# ─────────────────────────────────────────────
def _retro_check(df):
    """套用 4 條買點規則 + 起漲前型態
    輸入：6 個月日線 df（Open/High/Low/Close/Volume 大寫欄位）
    回傳：{signal, price, pattern, level}
      level: gold(🏆) / today(✓) / retest(📉) / none(—)
    """
    try:
        if df is None or len(df) < 30:
            return {"signal": "—", "price": None, "pattern": "資料不足", "level": "none"}
        close  = df["Close"].astype(float).values
        open_  = df["Open"].astype(float).values
        high   = df["High"].astype(float).values
        low    = df["Low"].astype(float).values
        volume = df["Volume"].astype(float).values
        n = len(close)

        cs = pd.Series(close)
        ma5  = cs.rolling(5).mean().values
        ma10 = cs.rolling(10).mean().values
        ma20 = cs.rolling(20).mean().values
        vma20 = pd.Series(volume).rolling(20).mean().values
        lo9 = pd.Series(low).rolling(9).min().values
        hi9 = pd.Series(high).rolling(9).max().values
        with np.errstate(divide="ignore", invalid="ignore"):
            rsv = np.where(hi9 - lo9 > 0, (close - lo9) / (hi9 - lo9) * 100, 50.0)
        rsv = np.nan_to_num(rsv, nan=50.0)
        k = np.zeros(n); d = np.zeros(n); k[0] = 50.0; d[0] = 50.0
        for i in range(1, n):
            k[i] = (2/3) * k[i-1] + (1/3) * float(rsv[i])
            d[i] = (2/3) * d[i-1] + (1/3) * k[i]

        def trig_at(i):
            if i < 20 or np.isnan(ma5[i]) or np.isnan(ma20[i]) or np.isnan(vma20[i]):
                return []
            out = []
            # ① 量縮整理 ≥3 日後爆量站 5MA
            if i >= 3:
                v3, vm3 = volume[i-3:i], vma20[i-3:i]
                if not np.isnan(vm3).any() and (v3 < vm3).all():
                    if volume[i] > vma20[i] * 1.8 and close[i] > open_[i] and close[i] > ma5[i]:
                        out.append("①量縮後爆量")
            # ② KD<20 黃金交叉 + 站上 10MA
            if i >= 1 and not np.isnan(ma10[i]):
                if k[i-1] < 20 and k[i-1] < d[i-1] and k[i] > d[i] and close[i] > ma10[i]:
                    out.append("②KD低檔交叉")
            # ③ 縮量回測 20MA 不破紅K（前 5 日有破過）
            if i >= 5:
                p5_lo = low[i-5:i]; p5_m20 = ma20[i-5:i]
                broke = any((not np.isnan(m)) and (l < m) for l, m in zip(p5_lo, p5_m20))
                if broke and low[i] >= ma20[i] * 0.98 and close[i] > open_[i] and volume[i] < vma20[i]:
                    out.append("③回測20MA不破")
            # ④ 突破近 20 日高點 + 量增
            if i >= 20:
                recent_hi = float(np.max(high[i-20:i]))
                if close[i] > recent_hi and volume[i] > vma20[i] * 1.5 and close[i] > open_[i]:
                    out.append("④突破20日高")
            return out

        # 起漲前型態（今日往前 20 日）
        i = n - 1
        pat = "其他"
        pre_hi = float(np.max(high[max(0,i-20):i]))
        pre_lo = float(np.min(low[max(0,i-20):i]))
        ct = float(close[i])
        if pre_lo > 0:
            range_pct = (pre_hi - pre_lo) / pre_lo * 100
            top60 = float(np.max(high[max(0,i-60):i])) if i >= 30 else 0
            if top60 > 0 and (top60 - ct) / top60 > 0.20:
                pat = "跌深反彈"
            elif range_pct < 15 and ct >= pre_hi * 0.99:
                pat = "箱型突破"
            elif range_pct > 15 and (ct - pre_lo) / pre_lo * 100 > 5:
                pat = "W底/V反"
            elif (not np.isnan(ma20[i]) and not np.isnan(ma20[max(0,i-5)])
                  and ma20[i] > ma20[max(0,i-5)]
                  and low[i-1] <= ma20[i] * 1.03 and ct > ma20[i]):
                pat = "多頭續攻回測"

        # 今日訊號
        today_trig = trig_at(n - 1)
        if today_trig:
            rule = today_trig[0]
            price = round(float(close[-1]), 2)
            if pat == "W底/V反":
                return {"signal": f"🏆{rule}@${price}", "price": price, "pattern": pat, "level": "gold"}
            return {"signal": f"✓{rule}@${price}", "price": price, "pattern": pat, "level": "today"}

        # 回看 30 天找更低觸發點
        today_close = float(close[-1])
        best = None
        start = max(20, n - 1 - 30)
        for j in range(start, n - 1):
            trig = trig_at(j)
            if trig and float(close[j]) < today_close:
                if best is None or float(close[j]) < best[2]:
                    best = (j, trig[0], float(close[j]))
        if best is not None:
            idx, rule, price = best
            try:
                date_str = pd.Timestamp(df.index[idx]).strftime("%m/%d")
            except Exception:
                date_str = "?"
            return {"signal": f"📉等${price:.2f}({date_str}){rule}",
                    "price": round(price, 2), "pattern": pat, "level": "retest"}

        return {"signal": "—", "price": None, "pattern": pat, "level": "none"}
    except Exception:
        return {"signal": "—", "price": None, "pattern": "?", "level": "none"}


# ─────────────────────────────────────────────
# 下載評分：日線 + 60分線
# ─────────────────────────────────────────────
def _fetch_daily_only(args):
    """第一階段：只算日線分，速度快"""
    code, name, sector, market = args
    return _fetch_daily(code, name, sector, market)

def _fetch_daily(code, name, sector, market="上市"):
    try:
        # 使用 Ticker.history() 取代 yf.download()，避免多執行緒共用 session 導致資料錯亂
        ticker = yf.Ticker(_tw_ticker(code, market))
        df = ticker.history(period="6mo", interval="1d")
        ind = _calc_indicators(df)
        if not ind: return None
        # 過濾低成交量（平均日量 < 1000張 = 1,000,000股）
        if ind["vma"] < 1_000_000:
            return None
        score, signals = _score_from_ind(ind, D_RSI_MIN, D_RSI_MAX, D_VOL_SURGE)
        consol_flag, consol_signal, consol_support = _detect_consolidation(df)
        if consol_flag:
            signals.append(f"⚑{consol_signal}")
            score = min(100, score + 8)
        close = ind["close_series"]
        wchg  = (ind["c"]-close.iloc[-6]) /close.iloc[-6] *100 if len(close)>=6  else 0
        mchg  = (ind["c"]-close.iloc[-22])/close.iloc[-22]*100 if len(close)>=22 else 0
        day_chg = round((ind["c"] - ind["c1"]) / ind["c1"] * 100, 2) if ind["c1"] > 0 else 0
        retro = _retro_check(df)
        return dict(
            code=code, name=name, sector=sector, market=market,
            price=round(float(ind["c"]),2),
            open_price=round(float(ind["o"]),2),
            week_chg=round(float(wchg),2), month_chg=round(float(mchg),2),
            day_chg=day_chg,
            rsi=round(float(ind["r"]),1),
            k=round(float(ind["k"]),1), d=round(float(ind["d"]),1),
            macd_hist=round(float(ind["mch"]),4),
            ma5=round(float(ind["m5"]),2),
            ma20=round(float(ind["m20"]),2),
            vol_ratio=round(float(ind["vol"]/ind["vma"]),2) if ind["vma"]>0 else 0,
            vol_lots=int(ind["vol"] / 1000),
            vma_lots=int(ind["vma"] / 1000),
            d_score=score, d_signals=", ".join(signals) if signals else "-",
            h_score=None, h_signals="無資料", combined=0,
            rev_yoy=None, rev_month="", rev_score=0,
            consol_flag=consol_flag, consol_signal=consol_signal, consol_support=consol_support,
            foreign_net=None, trust_net=None,
            consec_foreign=0, consec_trust=0,
            foreign_flag=False, trust_flag=False,
            retro_signal=retro["signal"], retro_price=retro["price"],
            retro_pattern=retro["pattern"], retro_level=retro["level"],
        )
    except:
        return None

def _fetch_hourly(code, market="上市"):
    try:
        # 使用 Ticker.history() 取代 yf.download()，避免多執行緒共用 session 導致資料錯亂
        ticker = yf.Ticker(_tw_ticker(code, market))
        df = ticker.history(period="60d", interval="60m")
        ind = _calc_indicators(df)
        if not ind: return None, None, 0
        # 關鍵指標含 NaN 則略過，避免比較錯誤
        import math
        for key in ("r","k","d","k1","d1","mch","mch1","m5","m10","m20"):
            if ind[key] is None or (isinstance(ind[key], float) and math.isnan(ind[key])):
                return None, None, 0
        score, signals = _score_from_ind(ind, H_RSI_MIN, H_RSI_MAX, H_VOL_SURGE)
        h_vol_ratio = round(float(ind["vol"] / ind["vma"]), 2) if ind.get("vma", 0) > 0 else 0
        return score, (", ".join(signals) if signals else "訊號未觸發"), h_vol_ratio
    except:
        return None, None, 0

def _add_hourly(r):
    """第二階段：對已有日線結果的股票補上小時線評分"""
    code = r["code"]
    h_score, h_signals, h_vol_ratio = _fetch_hourly(code, r.get("market", "上市"))
    if h_score is not None:
        r["h_score"]   = h_score
        r["h_signals"] = h_signals
        # 盤中時用小時線 vol_ratio（當前小時量/20小時均量）取代日線的累計量比
        # 原因：盤中日線累計量遠低於全日均量，會導致 vol_ratio 偏低（如 0.2x）
        # 而小時線 vol_ratio 才能反映當下實際量能（如 2.6x）
        if is_market_open() and h_vol_ratio > 0:
            r["vol_ratio"] = h_vol_ratio
    hs = h_score if h_score is not None else 0
    if is_market_open() and h_score is not None:
        base = round(hs * 0.6 + r["d_score"] * 0.4)
    elif h_score is not None:
        base = round(r["d_score"] * 0.7 + hs * 0.3)
    else:
        base = r["d_score"]
    r["combined"] = min(100, base + r.get("rev_score", 0))
    return r

def analyze_stock(args):
    """舊版保留，供單股完整分析用"""
    code, name, sector = args
    r = _fetch_daily(code, name, sector)
    if r is None: return None
    return _add_hourly(r)


# ─────────────────────────────────────────────
# 模擬下單系統
# ─────────────────────────────────────────────
def _trading_days_between(d1_str, d2_str):
    """計算兩個 YYYY-MM-DD 之間的交易日數（排除週末與國定假日）"""
    d1 = datetime.datetime.strptime(d1_str, "%Y-%m-%d").date()
    d2 = datetime.datetime.strptime(d2_str, "%Y-%m-%d").date()
    count = 0
    cur = d1
    while cur < d2:
        cur += datetime.timedelta(days=1)
        if cur.weekday() < 5 and not is_holiday(cur):
            count += 1
    return count

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

_SLOT_ORDER = {"09:00": 1, "09:05": 1, "10:00": 2, "11:00": 3, "12:00": 4, "13:00": 5, "13:20": 5, "14:00": 6}

def _fetch_slot_price(code, market="上市"):
    """抓股票即時/盤中現價：優先用 fast_info 即時成交價，備用小時線，再備用日線"""
    tk_sym = _tw_ticker(code, market)
    try:
        ticker = yf.Ticker(tk_sym)
        # 1. 即時最後成交價（盤中最準）
        fi = ticker.fast_info
        lp = (fi.get("last_price") if hasattr(fi, "get") else None) or getattr(fi, "last_price", None) or getattr(fi, "lastPrice", None)
        if lp and lp > 0:
            return round(float(lp), 2)
    except Exception:
        pass
    try:
        # 2. 最新一根小時線收盤
        df = yf.Ticker(tk_sym).history(period="5d", interval="60m")
        if not df.empty:
            return round(float(df["Close"].iloc[-1]), 2)
    except Exception:
        pass
    try:
        # 3. 日線收盤（最後備用）
        df = yf.Ticker(tk_sym).history(period="2d", interval="1d")
        if not df.empty:
            return round(float(df["Close"].iloc[-1]), 2)
    except Exception:
        pass
    return None

def _fetch_ma5(code, market="上市"):
    """抓個股最新5日均線（供持倉跌破判斷）"""
    try:
        ticker = yf.Ticker(_tw_ticker(code, market))
        df = ticker.history(period="30d", interval="1d")
        if df is None or len(df) < 5:
            return None
        close = df["Close"].squeeze()
        ma5 = close.rolling(5).mean()
        val = float(ma5.iloc[-1])
        return round(val, 2) if val > 0 else None
    except Exception:
        return None

def _fetch_sell_data(code, market="上市"):
    """抓取13:20賣出訊號所需資料：ma5, vol_ma20, prev_close, open_today, high_today, vol_today"""
    try:
        ticker = yf.Ticker(_tw_ticker(code, market))
        df_d = ticker.history(period="35d", interval="1d")
        if df_d is None or len(df_d) < 6:
            return None
        close_d  = df_d["Close"].squeeze()
        vol_d    = df_d["Volume"].squeeze()
        ma5      = float(close_d.rolling(5).mean().iloc[-1])
        vol_ma20 = (float(vol_d.rolling(20).mean().iloc[-1])
                    if len(vol_d) >= 20 else float(vol_d.mean()))

        # 前日收盤（排除今日不完整的日線 bar）
        today_dt = datetime.date.today()
        dates = [d.date() if hasattr(d, "date") else d for d in df_d.index]
        prev_closes = [float(close_d.iloc[i]) for i, d in enumerate(dates) if d < today_dt]
        prev_close  = prev_closes[-1] if prev_closes else float(close_d.iloc[-1])

        # 今日盤中資料（1 小時線）
        df_h = ticker.history(period="1d", interval="60m")
        open_today = high_today = vol_today = None
        if df_h is not None and not df_h.empty:
            open_today = float(df_h["Open"].iloc[0])
            high_today = float(df_h["High"].max())
            vol_today  = float(df_h["Volume"].sum())

        return {
            "ma5":        round(ma5, 2),
            "vol_ma20":   round(vol_ma20, 0),
            "prev_close": round(prev_close, 2),
            "open_today": round(open_today, 2) if open_today else None,
            "high_today": round(high_today, 2) if high_today else None,
            "vol_today":  vol_today,
        }
    except Exception:
        return None


def _check_sell_signals(sd, curr_price):
    """
    三條件賣出判斷（任一觸發即納入，回傳已觸發的理由清單）。
    sd = _fetch_sell_data() 回傳 dict；curr_price = 最新現價。
    """
    if not sd or not curr_price:
        return []

    ma5       = sd["ma5"]
    vol_ma20  = sd["vol_ma20"]
    prev_close = sd["prev_close"]
    open_today = sd.get("open_today")
    high_today = sd.get("high_today")
    vol_today  = sd.get("vol_today")

    reasons = []
    daily_chg = (curr_price - prev_close) / prev_close if prev_close > 0 else 0
    vol_ratio  = vol_today / vol_ma20 if (vol_today and vol_ma20 > 0) else 0
    bias_5ma   = (curr_price - ma5) / ma5 if ma5 > 0 else 0

    # 條件1：爆量滯漲（量 > 2.5x 20MA均量，且漲幅 < 1%）
    if vol_ratio >= 2.5 and daily_chg < 0.01:
        reasons.append(f"爆量滯漲(量{vol_ratio:.1f}x,漲{daily_chg*100:.1f}%)")

    # 條件2：避雷針（上影線 > 2x實體，且正乖離 > 10%）
    if open_today and high_today:
        body       = abs(curr_price - open_today)
        upper_shad = high_today - max(curr_price, open_today)
        if body > 0 and upper_shad > 2 * body and bias_5ma > 0.10:
            reasons.append(
                f"避雷針(上影{upper_shad:.1f}/實體{body:.1f},乖離{bias_5ma*100:.1f}%)")

    # 條件3：大黑K + 跌破5MA（跌幅 > 4% 且 收盤 < 5MA）
    if daily_chg < -0.04 and curr_price < ma5:
        reasons.append(f"大黑K(跌{abs(daily_chg)*100:.1f}%,跌破5MA)")

    return reasons


def _batch_fetch_prices(code_market_pairs):
    """用 yf.download 批次抓多檔現價（比個別 Ticker 省大量 request）
    回傳 {code: price} dict，失敗的 code 不放入結果。"""
    pairs = list(code_market_pairs)
    if not pairs:
        return {}
    code_by_sym = {_tw_ticker(code, market): code for code, market in pairs}
    syms = list(code_by_sym.keys())
    result = {}
    for attempt in range(1, 4):
        try:
            import time as _t
            if attempt > 1:
                _t.sleep(3 * attempt)
            df = yf.download(
                syms, period="2d", interval="1d",
                auto_adjust=True, progress=False, threads=True
            )
            if df.empty:
                continue
            # 單檔時 columns = ["Close", ...]; 多檔時 columns = MultiIndex (field, symbol)
            if len(syms) == 1:
                if "Close" in df.columns:
                    val = df["Close"].dropna()
                    if not val.empty and float(val.iloc[-1]) > 0:
                        result[pairs[0][0]] = round(float(val.iloc[-1]), 2)
            else:
                close_df = df["Close"] if "Close" in df.columns else df.xs("Close", axis=1, level=0)
                for sym, code in code_by_sym.items():
                    if sym in close_df.columns:
                        try:
                            val = close_df[sym].dropna()
                            if not val.empty and float(val.iloc[-1]) > 0:
                                result[code] = round(float(val.iloc[-1]), 2)
                        except Exception:
                            pass
            if result:
                break  # 有資料就不再重試
        except Exception as e:
            print(f"    [批次現價] 第 {attempt} 次失敗: {e}")
    return result

def sim_update(results, allow_entry=True):
    """根據最新分析結果更新模擬倉位（5個時段分開追蹤），回傳最新 sim dict。
    allow_entry=False 時只更新現價/出場，不開新倉（手動執行時使用）。"""
    now   = datetime.datetime.now()
    today = now.strftime("%Y-%m-%d")
    # 非交易時段（測試/補跑）仍記為最近合理時段
    # 09:05 進場避免抓到前日收盤價；13:20 執行三條件賣出判斷；14:00 寄出推薦 Email
    if   now.hour <= 9:    slot = "09:05"
    elif now.hour == 10:   slot = "10:00"
    elif now.hour == 11:   slot = "11:00"
    elif now.hour == 12:   slot = "12:00"
    elif now.hour == 13:   slot = "13:20"
    else:                  slot = "14:00"

    price_map = {r["code"]: r for r in results}
    sim = load_sim()

    # ── 清除進場日為假日的持倉與出場紀錄（不應存在，防呆清理） ──
    def _not_holiday_entry(p):
        try:
            return not is_holiday(datetime.datetime.strptime(p["entry_date"], "%Y-%m-%d").date())
        except Exception:
            return True

    sim["open"]   = [p for p in sim["open"]   if _not_holiday_entry(p)]
    sim["closed"] = [p for p in sim["closed"] if _not_holiday_entry(p)]

    # ── 假日不開新倉（防禦性檢查，main() 已擋但防萬一） ──
    if is_holiday(now):
        save_sim(sim)
        return sim

    # ── 預先批次抓所有持倉股的即時現價 ──
    all_open_codes = list({pos["code"] for pos in sim["open"]})
    _open_mkt_map  = {pos["code"]: pos.get("market", "上市") for pos in sim["open"]}
    slot_price_map = {}
    if all_open_codes:
        # 優先用 yf.download 批次下載（省 request，不易觸發 rate limit）
        pairs = [(c, _open_mkt_map.get(c, "上市")) for c in all_open_codes]
        slot_price_map = _batch_fetch_prices(pairs)
        # 批次失敗的個股，再用個別方式補抓
        missing = [c for c in all_open_codes if c not in slot_price_map]
        if missing:
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(5, len(missing))) as pool:
                futs = {pool.submit(_fetch_slot_price, c, _open_mkt_map.get(c, "上市")): c for c in missing}
                for fut in concurrent.futures.as_completed(futs):
                    try:
                        c = futs[fut]
                        p = fut.result()
                        if p is not None:
                            slot_price_map[c] = p
                    except Exception:
                        pass
        print(f"    [持倉現價] 成功取得 {len(slot_price_map)}/{len(all_open_codes)} 檔現價")

    # ── 1. 出場：13:20 時段判斷三項賣出訊號，任一觸發即賣出 ──
    sell_signal_exits = []   # 本次觸發賣出的記錄，供 main() 寄通知信
    still_open = []

    # 只在 13:20 時段批次抓取賣出訊號所需資料
    sell_data_cache = {}
    if slot == "13:20" and sim["open"]:
        _mkt_map2 = {pos["code"]: pos.get("market", "上市") for pos in sim["open"]}
        open_codes = list(_mkt_map2.keys())
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(10, len(open_codes))) as pool:
            futs2 = {
                pool.submit(_fetch_sell_data, c, _mkt_map2[c]): c
                for c in open_codes
            }
            for fut2 in concurrent.futures.as_completed(futs2):
                try:
                    c2 = futs2[fut2]
                    v2 = fut2.result()
                    if v2:
                        sell_data_cache[c2] = v2
                except Exception:
                    pass
        print(f"    [賣出訊號] 已取得 {len(sell_data_cache)}/{len(sim['open'])} 檔資料")

    for pos in sim["open"]:
        days_held = _trading_days_between(pos["entry_date"], today)
        curr_price = (slot_price_map.get(pos["code"])
                      or (price_map.get(pos["code"]) or {}).get("price")
                      or pos["entry_price"])
        ret_pct = round((curr_price - pos["entry_price"]) / pos["entry_price"] * 100, 2)

        # 三條件賣出判斷（只在13:20執行）
        sell_reasons = []
        if slot == "13:20":
            sell_reasons = _check_sell_signals(sell_data_cache.get(pos["code"]), curr_price)

        if sell_reasons:
            exit_rec = {
                **{k: v for k, v in pos.items() if k not in ("curr_price", "curr_pct")},
                "exit_date":   today,
                "exit_slot":   slot,
                "exit_price":  curr_price,
                "return_pct":  ret_pct,
                "days_held":   days_held,
                "exit_reason": sell_reasons[0].split("(")[0],  # 主要原因（不含細節）
                "sell_detail": " + ".join(sell_reasons),
            }
            sim["closed"].append(exit_rec)
            sell_signal_exits.append(exit_rec)
        else:
            pos["curr_price"] = curr_price
            pos["curr_pct"]   = ret_pct
            pos["days_held"]  = days_held
            still_open.append(pos)

    sim["open"] = still_open
    sim["last_sell_exits"] = sell_signal_exits  # 供 main() 讀取後寄通知信

    # ── 2. 新進場：只在排程觸發時開倉（手動執行跳過） ──
    new_count = 0
    if not allow_entry:
        print(f"    [手動執行] 跳過新開倉，僅更新現價與出場結算")
    elif slot in SIM_SKIP_SLOTS:
        print(f"    [{slot}] 為暫停開倉時段，跳過新進場")
    else:
        # 當天該時段已進場的代號（避免同一時段重複進場）
        entered_this_slot = {
            p["code"] for p in sim["open"]
            if p.get("entry_date") == today and p.get("entry_slot") == slot
        }
        entered_this_slot |= {
            p["code"] for p in sim["closed"]
            if p.get("entry_date") == today and p.get("entry_slot") == slot
        }

        # 分市場各算本時段今日已進場數（防止重複執行超量）
        def _mkt_entered(mkt):
            return sum(
                1 for p in list(sim["open"]) + list(sim["closed"])
                if p.get("entry_date") == today and p.get("entry_slot") == slot
                and p.get("market", "上市") == mkt
            )
        rem_twse = max(0, SIM_MAX_NEW - _mkt_entered("上市"))
        rem_tpex = max(0, SIM_MAX_NEW - _mkt_entered("上櫃"))

        _base_ok = lambda r: (
            SIM_ENTRY_SCORE <= r["combined"] <= SIM_MAX_SCORE
            and r.get("vol_ratio", 0) >= SIM_VOL_RATIO_MIN
            and r["code"] not in entered_this_slot
        )
        candidates = (
            sorted([r for r in results if _base_ok(r) and r.get("market", "上市") == "上市"],
                   key=lambda x: x["combined"], reverse=True)[:rem_twse]
            + sorted([r for r in results if _base_ok(r) and r.get("market", "上市") == "上櫃"],
                     key=lambda x: x["combined"], reverse=True)[:rem_tpex]
        )

        # 批次抓新進場候選股的即時現價（並行）
        cand_codes = [r["code"] for r in candidates]
        _cand_mkt_map = {r["code"]: r.get("market", "上市") for r in candidates}
        entry_slot_prices = {}
        if cand_codes:
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(10, len(cand_codes))) as pool:
                futs = {pool.submit(_fetch_slot_price, c, _cand_mkt_map.get(c, "上市")): c for c in cand_codes}
                for fut in concurrent.futures.as_completed(futs):
                    try:
                        c = futs[fut]
                        p = fut.result()
                        if p is not None:
                            entry_slot_prices[c] = p
                    except Exception:
                        pass

        for r in candidates:
            ep = entry_slot_prices.get(r["code"]) or r["price"]
            sim["open"].append({
                "code":          r["code"],
                "name":          r["name"],
                "sector":        r.get("sector", ""),
                "market":        r.get("market", "上市"),
                "entry_date":    today,
                "entry_slot":    slot,
                "entry_price":   ep,
                "curr_price":    ep,
                "curr_pct":      0.0,
                "days_held":     0,
                "entry_score":   r["combined"],
                "entry_signals": r["d_signals"],
            })
            new_count += 1

    # 只保留最近 200 筆已平倉
    sim["closed"] = sim["closed"][-200:]

    # 記錄本次執行狀態（供 HTML 通知區塊使用）
    if not allow_entry:
        run_status = "manual"
    elif slot in SIM_SKIP_SLOTS:
        run_status = "skip_slot"
    elif new_count == 0:
        run_status = "no_qualify"
    else:
        run_status = "ok"
    sim["last_run"] = {
        "slot":      slot,
        "date":      today,
        "new_count": new_count,
        "status":    run_status,
    }

    save_sim(sim)
    print(f"    模擬倉位 [{slot}]：今日新增 {new_count} 檔  持倉合計 {len(sim['open'])} 檔  累計出場 {len(sim['closed'])} 筆")
    if run_status == "no_qualify":
        print(f"    [注意] [{slot}] 本時段無符合條件股票（分數{SIM_ENTRY_SCORE}~{SIM_MAX_SCORE} 且量增>={SIM_VOL_RATIO_MIN}x）")
    return sim

def generate_sim_section(sim, for_email=False, split=False):
    """產生模擬績效 HTML 區塊（5時段分開比較）。
    for_email=True 時移除 JavaScript 與 onclick（Gmail 相容），表格仍預先排序。
    split=True 時回傳 (notice, open, analysis, closed) 四個獨立 HTML 字串，供外部自訂排版順序。"""
    def _is_holiday_entry(p):
        try:
            return is_holiday(datetime.datetime.strptime(p.get("entry_date", "2000-01-01"), "%Y-%m-%d").date())
        except Exception:
            return False

    closed = [p for p in sim.get("closed", []) if not _is_holiday_entry(p)]
    opened = [p for p in sim.get("open",   []) if not _is_holiday_entry(p)]
    SLOTS  = ["09:00", "09:05", "10:00", "11:00", "12:00", "13:00", "13:20", "14:00"]
    _ls    = "font-size:.74em;padding:1px 4px;border-radius:2px;text-decoration:none;margin:1px;display:inline-block;color:#fff"

    def ret_col(v): return "#c0392b" if v > 0 else ("#27ae60" if v < 0 else "#555")
    td  = "padding:5px 7px;border:1px solid #ddd;text-align:center;font-size:.83em"
    tdl = "padding:5px 7px;border:1px solid #ddd;text-align:left;font-size:.83em"
    th  = "padding:6px 7px;border:1px solid #1a5276;background:#1a5276;color:#fff;font-size:.82em;white-space:nowrap"
    ths = th if for_email else (th + ";cursor:pointer;user-select:none")

    # ── 本次執行狀態通知 ──
    last_run   = sim.get("last_run", {})
    lr_slot    = last_run.get("slot", "")
    lr_status  = last_run.get("status", "")
    lr_date    = last_run.get("date", "")
    lr_new     = last_run.get("new_count", 0)
    today_str  = datetime.date.today().strftime("%Y-%m-%d")
    if lr_date == today_str and lr_status == "no_qualify":
        run_notice = (
            f'<div style="background:#fff3cd;border:1px solid #ffc107;border-radius:4px;'
            f'padding:8px 14px;margin-bottom:10px;font-size:.88em">'
            f'⚠️ <b>[{lr_slot}] 本時段無符合條件股票進場</b>'
            f'（分數 {SIM_ENTRY_SCORE}~{SIM_MAX_SCORE} 且量增 ≥ {SIM_VOL_RATIO_MIN}x）'
            f'</div>'
        )
    elif lr_date == today_str and lr_status == "ok" and lr_new > 0:
        run_notice = (
            f'<div style="background:#d4edda;border:1px solid #28a745;border-radius:4px;'
            f'padding:8px 14px;margin-bottom:10px;font-size:.88em">'
            f'✅ <b>[{lr_slot}] 本時段新增 {lr_new} 檔進場</b>'
            f'</div>'
        )
    elif lr_date == today_str and lr_status == "skip_slot":
        run_notice = (
            f'<div style="background:#e2e3e5;border:1px solid #adb5bd;border-radius:4px;'
            f'padding:8px 14px;margin-bottom:10px;font-size:.88em">'
            f'⏸ <b>[{lr_slot}] 為暫停開倉時段</b>，本時段不開新倉'
            f'</div>'
        )
    elif lr_date == today_str and lr_status == "manual":
        run_notice = (
            f'<div style="background:#e8f4fd;border:1px solid #90caf9;border-radius:4px;'
            f'padding:8px 14px;margin-bottom:10px;font-size:.88em">'
            f'🖐 <b>[{lr_slot}] 手動執行</b>：僅更新現價與出場結算，不開新倉'
            f'</div>'
        )
    else:
        run_notice = ""

    # email 模式：th 不帶 onclick
    def _th(label, col, tbl_id):
        if for_email:
            return f'<th style="{th}" data-label="{label}">{label}</th>'
        return (f'<th style="{ths}" data-label="{label}" '
                f'onclick="_simSort(\'{tbl_id}\',{col})">{label}</th>')

    # ── 解析 entry_signals，找最高得分訊號 ──
    _SIG_SCORE = {
        "KD黃金交叉": 25, "MACD柱翻正": 20, "均線多頭": 15,
        "KD低檔": 10, "站上MA20": 10, "布林反彈": 10, "MACD增強": 8,
    }
    def top_signal(sigs_str):
        if not sigs_str or sigs_str in ("-", "無資料", "訊號未觸發"):
            return "—"
        best_name, best_sc = "—", -999
        for s in sigs_str.split(","):
            s = s.strip()
            if s.startswith("RSI="):      sc = 10
            elif s.startswith("量增"):    sc = 10
            elif "盤整" in s:             sc = 8
            elif s == "RSI過熱":          sc = -15
            else:                         sc = _SIG_SCORE.get(s, 0)
            if sc > best_sc:
                best_sc, best_name = sc, s
        return f"{best_name}（+{best_sc}分）" if best_sc > 0 else best_name

    # ── 1. 時段比較表（已出場全量統計） ──
    slot_rows = ""
    best_slot = None; best_wr = -1
    for slot in SLOTS:
        sc = [c for c in closed if c.get("entry_slot") == slot]
        if not sc:
            slot_rows += f'<tr><td style="{td}">{slot}</td>' + f'<td style="{td};color:#bbb">—</td>'*6 + '</tr>'
            continue
        wins = [c for c in sc if c["return_pct"] > 0]
        wr   = round(len(wins)/len(sc)*100)
        ar   = round(sum(c["return_pct"] for c in sc)/len(sc), 2)
        if wr > best_wr: best_wr = wr; best_slot = slot
        bg  = "background:#fffbea" if wr == best_wr else ""
        arc = ret_col(ar)
        slot_rows += f"""<tr style="{bg}">
          <td style="{td};font-weight:bold">{slot}</td>
          <td style="{td}">{len(sc)}</td>
          <td style="{td}">{len(wins)} / {len(sc)-len(wins)}</td>
          <td style="{td};font-weight:bold;color:{'#c0392b' if wr>=50 else '#888'}">{wr}%</td>
          <td style="{td};color:{arc};font-weight:bold">{ar:+.2f}%</td>
          <td style="{td}">{f"{round(sum(c['return_pct'] for c in wins)/len(wins),2):+.2f}%" if wins else "—"}</td>
          <td style="{td};color:#27ae60">{round(sum(c['return_pct'] for c in sc if c['return_pct']<=0)/max(1,len(sc)-len(wins)),2):+.2f}%</td>
        </tr>"""

    # ── 2. 持倉中（完整顯示，預設依進場分數由高到低） ──
    opened_sorted = sorted(opened, key=lambda x: x.get("entry_score", 0), reverse=True)
    open_rows = ""
    for p in opened_sorted:
        cc  = ret_col(p.get("curr_pct", 0))
        _c  = p["code"]
        _mkt = p.get("market", "上市")
        _mkt_bg = "#1a5276" if _mkt == "上市" else "#7d6608"
        _mkt_b  = f'<span style="background:{_mkt_bg};color:#fff;padding:0 4px;border-radius:3px;font-size:.7em;margin-left:3px">{_mkt}</span>'
        _ex  = "TWSE" if _mkt == "上市" else "TPEX"
        _tv = f"https://www.tradingview.com/chart/?symbol={_ex}%3A{_c}"
        _gi = f"https://goodinfo.tw/tw/ShowK_Chart.asp?STOCK_ID={_c}"
        _links = (f'<a href="{_tv}" target="_blank" style="{_ls};background:#1565c0">TV</a>'
                  f'<a href="{_gi}" target="_blank" style="{_ls};background:#2e7d32">K線</a>')
        es = p.get("entry_score", 0)
        ts = top_signal(p.get("entry_signals", ""))
        open_rows += f"""<tr>
          <td style="{td}"><a href="{_gi}" target="_blank" style="font-weight:bold;color:#003366;text-decoration:none">{_c}</a>{_mkt_b}<br>{_links}</td>
          <td style="{tdl}">{p["name"]}</td>
          <td style="{td};color:#555;font-size:.8em">{p.get("sector","")}</td>
          <td style="{td}" data-val="{p["entry_date"].replace('-','')}">{p["entry_date"]}<br><span style="font-size:.78em;color:#888">{p.get("entry_slot","")}</span></td>
          <td style="{td}">{p["entry_price"]}</td>
          <td style="{td}">{p.get("curr_price", p["entry_price"])}</td>
          <td style="{td};color:{cc};font-weight:bold" data-val="{p.get('curr_pct',0)}">{p.get("curr_pct",0):+.2f}%</td>
          <td style="{td};font-weight:bold;color:#1565c0" data-val="{es}">{es}</td>
          <td style="{tdl};font-size:.79em;color:#555">{ts}</td>
        </tr>"""

    # ── 3. 已出場明細（完整顯示全部，預設依進場分數由高到低） ──
    closed_sorted = sorted(closed, key=lambda x: x.get("entry_score", 0), reverse=True)
    closed_rows = ""
    for c in closed_sorted:
        cc   = ret_col(c["return_pct"])
        code = c["code"]
        _cmkt   = c.get("market", "上市")
        _cmkt_bg = "#1a5276" if _cmkt == "上市" else "#7d6608"
        _cmkt_b  = f'<span style="background:{_cmkt_bg};color:#fff;padding:0 4px;border-radius:3px;font-size:.7em;margin-left:3px">{_cmkt}</span>'
        _cex    = "TWSE" if _cmkt == "上市" else "TPEX"
        tv_url = f"https://www.tradingview.com/chart/?symbol={_cex}%3A{code}"
        gi_url = f"https://goodinfo.tw/tw/ShowK_Chart.asp?STOCK_ID={code}"
        links  = (f'<a href="{tv_url}" target="_blank" style="{_ls};background:#1565c0">TV</a>'
                  f'<a href="{gi_url}" target="_blank" style="{_ls};background:#2e7d32">K線</a>')
        es = c.get("entry_score", 0)
        ts = top_signal(c.get("entry_signals", ""))
        closed_rows += f"""<tr>
          <td style="{td}"><a href="{gi_url}" target="_blank" style="font-weight:bold;color:#003366;text-decoration:none">{code}</a>{_cmkt_b}<br>{links}</td>
          <td style="{tdl}">{c["name"]}</td>
          <td style="{td}" data-val="{c["entry_date"].replace('-','')}">{c["entry_date"]}</td>
          <td style="{td};font-weight:bold">{c.get("entry_slot","—")}</td>
          <td style="{td}" data-val="{c.get("exit_date","").replace('-','')}">{c.get("exit_date","—")}</td>
          <td style="{td};font-weight:bold">{c.get("exit_slot","—")}</td>
          <td style="{td}">{c["entry_price"]}</td>
          <td style="{td}">{c["exit_price"]}</td>
          <td style="{td};color:{cc};font-weight:bold" data-val="{c["return_pct"]}">{c["return_pct"]:+.2f}%</td>
          <td style="{td};font-weight:bold;color:#1565c0" data-val="{es}">{es}</td>
          <td style="{tdl};font-size:.79em;color:#555">{ts}</td>
        </tr>"""

    total_closed = len(closed)
    total_open   = len(opened)
    best_label   = f"目前領先：<b style='color:#c0392b'>{best_slot}</b> 時段（勝率 {best_wr}%）" if best_slot else "累積資料中..."

    # ── script 區塊（email 模式略過） ──
    script_block = "" if for_email else """
<script>
function _simSort(tblId, col) {
  var tbl = document.getElementById(tblId);
  if (!tbl) return;
  var tbody = tbl.tBodies[0];
  var rows  = Array.from(tbody.rows);
  var asc   = tbl.dataset.sortCol == col ? tbl.dataset.sortAsc != "1" : false;
  rows.sort(function(a, b) {
    var va = (a.cells[col].getAttribute("data-val") || a.cells[col].innerText).trim();
    var vb = (b.cells[col].getAttribute("data-val") || b.cells[col].innerText).trim();
    var na = parseFloat(va.replace(/[^0-9.\\-]/g,"")), nb = parseFloat(vb.replace(/[^0-9.\\-]/g,""));
    if (!isNaN(na) && !isNaN(nb)) return asc ? na-nb : nb-na;
    return asc ? va.localeCompare(vb,"zh") : vb.localeCompare(va,"zh");
  });
  rows.forEach(function(r){ tbody.appendChild(r); });
  tbl.dataset.sortCol = col; tbl.dataset.sortAsc = asc ? "1" : "0";
  Array.from(tbl.tHead.rows[0].cells).forEach(function(th, i) {
    th.textContent = th.dataset.label || th.textContent.replace(/ [\\u25b2\\u25bc]$/,"");
    if (i == col) th.textContent += asc ? " \\u25b2" : " \\u25bc";
  });
}
</script>"""

    wrap_open  = '<div style="margin:12px 0;background:#fff;border:1px solid #b8c8d8;border-radius:6px;padding:10px 14px">' if for_email else '<details open style="margin:12px 0;background:#fff;border:1px solid #b8c8d8;border-radius:6px;padding:10px 14px">'
    wrap_title = '' if for_email else f'<summary style="font-weight:bold;color:#1a5276;font-size:1em;cursor:pointer">模擬下單 — 5時段進場比較（分數{SIM_ENTRY_SCORE}~{SIM_MAX_SCORE} / 量增≥{SIM_VOL_RATIO_MIN}x / 跳過{"/".join(SIM_SKIP_SLOTS)} / 爆量滯漲+避雷針+大黑K出場）</summary>'
    wrap_close = '</div>' if for_email else '</details>'
    sort_hint  = "" if for_email else "（點欄位標題可排序）"
    closed_label = "已出場明細（全部）" if for_email else "已出場明細（全部，點欄位標題可排序）"
    open_label   = "持倉中" if for_email else f"持倉中{sort_hint}"

    # ── split 模式：回傳 4 個獨立區塊，供外部決定排版順序 ──
    if split:
        _dv = "margin:12px 0;background:#fff;border:1px solid #b8c8d8;border-radius:6px;padding:10px 14px"
        _ph = "margin:6px 0 8px;font-size:.92em;font-weight:bold;color:#1a5276;border-bottom:2px solid #1a5276;padding-bottom:4px"
        notice_sec = f"""{run_notice}<div style="background:#eaf4fb;border-radius:4px;padding:8px 12px;font-size:.88em;margin:10px 0">
  持倉中：<b>{total_open}</b> 檔　累計出場：<b>{total_closed}</b> 筆　{best_label}
</div>"""
        open_sec = f"""{script_block}<div style="{_dv}">
<p style="{_ph}">▶ 持股中（{total_open} 檔）</p>
<table id="sim_open_tbl" style="border-collapse:collapse;width:100%;margin-bottom:12px" data-sort-col="7" data-sort-asc="0">
<thead><tr>
  {_th("代號",0,"sim_open_tbl")}
  {_th("名稱",1,"sim_open_tbl")}
  {_th("類股",2,"sim_open_tbl")}
  {_th("進場日",3,"sim_open_tbl")}
  {_th("進場價",4,"sim_open_tbl")}
  {_th("現價",5,"sim_open_tbl")}
  {_th("浮動損益",6,"sim_open_tbl")}
  {_th("進場分數 ▼",7,"sim_open_tbl")}
  {_th("主要得分項目",8,"sim_open_tbl")}
</tr></thead>
<tbody>{open_rows if open_rows else f'<tr><td colspan="9" style="{td};color:#aaa">目前無持倉</td></tr>'}</tbody>
</table>
</div>"""
        analysis_sec = f"""<div style="{_dv}">
<p style="{_ph}">▶ 分析獲利（各時段績效比較 / 已出場全量）</p>
<table style="border-collapse:collapse;width:100%;margin-bottom:12px">
<thead><tr>
  <th style="{th}">進場時段</th><th style="{th}">筆數</th><th style="{th}">勝/敗</th>
  <th style="{th}">勝率</th><th style="{th}">平均報酬</th>
  <th style="{th}">平均獲利</th><th style="{th}">平均虧損</th>
</tr></thead>
<tbody>{slot_rows}</tbody>
</table>
<p style="font-size:.78em;color:#888;margin:0 0 10px">黃底 = 目前勝率最高時段</p>
</div>"""
        closed_sec = f"""<div style="{_dv}">
<p style="{_ph}">▶ 出場股（{closed_label}）</p>
<table id="sim_closed_tbl" style="border-collapse:collapse;width:100%;margin-bottom:16px" data-sort-col="9" data-sort-asc="0">
<thead><tr>
  {_th("代號",0,"sim_closed_tbl")}
  {_th("名稱",1,"sim_closed_tbl")}
  {_th("進場日",2,"sim_closed_tbl")}
  {_th("進場時段",3,"sim_closed_tbl")}
  {_th("出場日",4,"sim_closed_tbl")}
  {_th("出場時段",5,"sim_closed_tbl")}
  {_th("進場價",6,"sim_closed_tbl")}
  {_th("出場價",7,"sim_closed_tbl")}
  {_th("報酬率",8,"sim_closed_tbl")}
  {_th("進場分數 ▼",9,"sim_closed_tbl")}
  {_th("主要得分項目",10,"sim_closed_tbl")}
</tr></thead>
<tbody>{closed_rows if closed_rows else f'<tr><td colspan="11" style="{td};color:#aaa">尚無出場紀錄</td></tr>'}</tbody>
</table>
</div>"""
        return (notice_sec, open_sec, analysis_sec, closed_sec)

    return f"""{script_block}
{wrap_open}
{wrap_title}
<div style="margin-top:8px">

{run_notice}<div style="background:#eaf4fb;border-radius:4px;padding:8px 12px;font-size:.88em;margin-bottom:10px">
  持倉中：<b>{total_open}</b> 檔　累計出場：<b>{total_closed}</b> 筆　{best_label}
</div>

<p style="margin:6px 0 4px;font-size:.85em;font-weight:bold;color:#1a5276">各時段績效比較（已出場全量）</p>
<table style="border-collapse:collapse;width:100%;margin-bottom:12px">
<thead><tr>
  <th style="{th}">進場時段</th><th style="{th}">筆數</th><th style="{th}">勝/敗</th>
  <th style="{th}">勝率</th><th style="{th}">平均報酬</th>
  <th style="{th}">平均獲利</th><th style="{th}">平均虧損</th>
</tr></thead>
<tbody>{slot_rows}</tbody>
</table>
<p style="font-size:.78em;color:#888;margin:0 0 10px">黃底 = 目前勝率最高時段</p>

<p style="margin:6px 0 4px;font-size:.85em;font-weight:bold;color:#1a5276">{closed_label}</p>
<table id="sim_closed_tbl" style="border-collapse:collapse;width:100%;margin-bottom:16px" data-sort-col="9" data-sort-asc="0">
<thead><tr>
  {_th("代號",0,"sim_closed_tbl")}
  {_th("名稱",1,"sim_closed_tbl")}
  {_th("進場日",2,"sim_closed_tbl")}
  {_th("進場時段",3,"sim_closed_tbl")}
  {_th("出場日",4,"sim_closed_tbl")}
  {_th("出場時段",5,"sim_closed_tbl")}
  {_th("進場價",6,"sim_closed_tbl")}
  {_th("出場價",7,"sim_closed_tbl")}
  {_th("報酬率",8,"sim_closed_tbl")}
  {_th("進場分數 ▼",9,"sim_closed_tbl")}
  {_th("主要得分項目",10,"sim_closed_tbl")}
</tr></thead>
<tbody>{closed_rows if closed_rows else f'<tr><td colspan="11" style="{td};color:#aaa">尚無出場紀錄</td></tr>'}</tbody>
</table>

<p style="margin:6px 0 4px;font-size:.85em;font-weight:bold;color:#1a5276">{open_label}</p>
<table id="sim_open_tbl" style="border-collapse:collapse;width:100%;margin-bottom:12px" data-sort-col="7" data-sort-asc="0">
<thead><tr>
  {_th("代號",0,"sim_open_tbl")}
  {_th("名稱",1,"sim_open_tbl")}
  {_th("類股",2,"sim_open_tbl")}
  {_th("進場日",3,"sim_open_tbl")}
  {_th("進場價",4,"sim_open_tbl")}
  {_th("現價",5,"sim_open_tbl")}
  {_th("浮動損益",6,"sim_open_tbl")}
  {_th("進場分數 ▼",7,"sim_open_tbl")}
  {_th("主要得分項目",8,"sim_open_tbl")}
</tr></thead>
<tbody>{open_rows if open_rows else f'<tr><td colspan="9" style="{td};color:#aaa">目前無持倉</td></tr>'}</tbody>
</table>

</div>
{wrap_close}"""


# ─────────────────────────────────────────────
# 產生 HTML
# ─────────────────────────────────────────────
def generate_html(results, prev_map, today_str, market_open, run_time_str, sim=None):
    top_twse = sorted([r for r in results if r.get("market", "上市") == "上市"],
                      key=lambda x: x["combined"], reverse=True)[:TOP_N]
    top_tpex = sorted([r for r in results if r.get("market", "上市") == "上櫃"],
                      key=lambda x: x["combined"], reverse=True)[:TOP_N]

    def badge(s, kind="combined"):
        if s is None:
            return '<span style="background:#ddd;color:#999;padding:1px 7px;border-radius:3px">—</span>'
        hi, mid = {"combined":(60,40),"d_score":(55,35),"h_score":(55,35)}.get(kind,(60,40))
        bg = "#28a745" if s >= hi else ("#fd7e14" if s >= mid else "#aaa")
        return f'<span style="background:{bg};color:#fff;padding:1px 7px;border-radius:3px;font-weight:bold">{s}</span>'

    def chg_td(v):
        col = "#d62728" if v > 0 else ("#2ca02c" if v < 0 else "#333")
        return f'<td style="color:{col}">{v:+.2f}%</td>'

    def diff_str(code, field, curr):
        if curr is None: return ""
        old = prev_map.get(code, {}).get(field)
        if old is None: return ""
        delta = curr - old
        if delta > 3:  return f' <span style="color:#d62728;font-size:.8em">▲{delta:+.0f}</span>'
        if delta < -3: return f' <span style="color:#2ca02c;font-size:.8em">▼{delta:+.0f}</span>'
        return ""

    def _rev_td(r):
        yoy = r.get("rev_yoy")
        mo  = r.get("rev_month", "")
        if yoy is None:
            return '<span style="color:#bbb;font-size:.8em">—</span>'
        col = "#c0392b" if yoy > 0 else ("#27ae60" if yoy < 0 else "#555")
        return (f'<span style="color:{col};font-weight:bold">{yoy:+.1f}%</span>'
                f'<br><span style="font-size:.72em;color:#888">{mo}</span>')

    def chart_links(code, market="上市"):
        exchange = "TWSE" if market == "上市" else "TPEX"
        tv  = f"https://www.tradingview.com/chart/?symbol={exchange}%3A{code}"
        gi  = f"https://goodinfo.tw/tw/ShowK_Chart.asp?STOCK_ID={code}"
        s = "font-size:.78em;padding:1px 5px;border-radius:2px;text-decoration:none;margin:1px;display:inline-block;color:#fff"
        return (f'<a href="{tv}" target="_blank" style="{s};background:#1565c0">TV</a>'
                f'<a href="{gi}" target="_blank" style="{s};background:#2e7d32">K線</a>')

    def inst_td(net, flag):
        if net is None:
            return '<td style="color:#bbb;font-size:.8em">—</td>'
        col = "#c0392b" if net > 0 else ("#27ae60" if net < 0 else "#888")
        mark = "**" if flag else ""
        return f'<td style="color:{col};font-weight:{"bold" if flag else "normal"}">{mark}{net:+,}{mark}</td>'

    def retro_td_web(r):
        lvl = r.get("retro_level", "none")
        sig = r.get("retro_signal", "—")
        pat = r.get("retro_pattern", "")
        if lvl == "gold":
            bg, fg = "#f57c00", "#fff"
        elif lvl == "today":
            bg, fg = "#2e7d32", "#fff"
        elif lvl == "retest":
            bg, fg = "#1565c0", "#fff"
        else:
            return f'<td style="color:#bbb;font-size:.85em">—</td>'
        pat_html = f'<br><span style="font-size:.7em;color:#888">{pat}</span>' if pat else ""
        return (f'<td style="background:{bg};color:{fg};font-size:.78em;font-weight:bold;'
                f'white-space:nowrap;padding:4px 6px">{sig}{pat_html}</td>')

    def _build_rows(stock_list):
        _rows = ""
        for i, r in enumerate(stock_list, 1):
            code = r["code"]
            mkt  = r.get("market", "上市")
            dual      = r["d_score"] >= 40 and (r["h_score"] or 0) >= 40
            can_enter = (SIM_ENTRY_SCORE <= r["combined"] <= SIM_MAX_SCORE
                         and r.get("vol_ratio", 0) >= SIM_VOL_RATIO_MIN)
            if can_enter:
                rs = ' style="background:#eafaf1;border-left:4px solid #27ae60"'
            elif dual:
                rs = ' style="background:#fffbea"'
            else:
                rs = ""
            if can_enter:
                entry_tag = '<br><span style="font-size:.72em;background:#27ae60;color:#fff;padding:1px 6px;border-radius:3px;white-space:nowrap">★ 可進場</span>'
            elif r["combined"] > SIM_MAX_SCORE:
                entry_tag = f'<br><span style="font-size:.72em;color:#fd7e14">分數偏高(&gt;{SIM_MAX_SCORE})</span>'
            elif r.get("vol_ratio", 0) < SIM_VOL_RATIO_MIN:
                entry_tag = f'<br><span style="font-size:.72em;color:#aaa">量增不足({r.get("vol_ratio",0):.1f}x&lt;{SIM_VOL_RATIO_MIN}x)</span>'
            else:
                entry_tag = ""
            vol_td     = f'<td>{r.get("vol_lots", 0):,}</td>'
            foreign_td = inst_td(r.get("foreign_net"), r.get("foreign_flag", False))
            trust_td   = inst_td(r.get("trust_net"),   r.get("trust_flag",   False))
            _rows += f"""
        <tr{rs}>
          <td>{i}</td>
          <td style="font-size:.8em;color:#555;white-space:nowrap">{today_str}<br>{run_time_str}</td>
          <td><a href="https://www.tradingview.com/chart/?symbol={'TWSE' if mkt=='上市' else 'TPEX'}%3A{code}" target="_blank"
               style="font-weight:bold;color:#003366;text-decoration:none">{code}</a><br>
              {chart_links(code, mkt)}</td>
          <td style="text-align:left">{r['name']}</td>
          <td style="text-align:left;font-size:.82em;color:#555">{r.get('sector','')}</td>
          <td><b>{r['price']}</b></td>
          {chg_td(r.get('day_chg', 0))}{chg_td(r['week_chg'])}{chg_td(r['month_chg'])}
          <td>{r['rsi']}</td><td>{r['k']}/{r['d']}</td>
          <td>{_rev_td(r)}</td>
          {vol_td}{foreign_td}{trust_td}
          <td>{badge(r['d_score'],'d_score')}{diff_str(code,'d_score',r['d_score'])}<br>
              <span style="font-size:.75em;color:#666">{r['d_signals']}</span></td>
          <td>{badge(r['h_score'],'h_score')}{diff_str(code,'h_score',r['h_score'])}<br>
              <span style="font-size:.75em;color:#666">{r['h_signals']}</span></td>
          <td>{badge(r['combined'])}{diff_str(code,'combined',r['combined'])}{entry_tag}</td>
          <td style="font-size:.8em;color:#e65100;white-space:nowrap">{'<b>⚑</b> ' + r.get('consol_signal','') if r.get('consol_flag') else '—'}</td>
          {retro_td_web(r)}
        </tr>"""
        return _rows

    rows_twse = _build_rows(top_twse)
    rows_tpex = _build_rows(top_tpex)

    # 🎯 Web 版回塑性訊號摘要
    _gold = [r for r in (top_twse + top_tpex) if r.get("retro_level") == "gold"]
    _today = [r for r in (top_twse + top_tpex) if r.get("retro_level") == "today"]
    if _gold or _today:
        _gold_rows = ""
        for r in _gold + _today:
            tag = "🏆" if r.get("retro_level") == "gold" else "✓"
            _gold_rows += (
                f'<tr><td style="padding:5px 8px;border:1px solid #ffd180;font-weight:bold">{tag} {r["code"]}</td>'
                f'<td style="padding:5px 8px;border:1px solid #ffd180">{r["name"]}</td>'
                f'<td style="padding:5px 8px;border:1px solid #ffd180;font-size:.85em">{r.get("retro_signal","")}</td>'
                f'<td style="padding:5px 8px;border:1px solid #ffd180;font-size:.85em">{r.get("retro_pattern","")}</td>'
                f'<td style="padding:5px 8px;border:1px solid #ffd180;color:#c0392b">${r.get("retro_price","-")}</td></tr>')
        retro_summary_web = (
            f'<div style="background:#fff8e1;border:2px solid #f57c00;border-radius:6px;'
            f'padding:12px;margin:10px 0">'
            f'<b style="color:#e65100;font-size:1.05em">🎯 回塑性訊號摘要</b>'
            f'<span style="color:#666;font-size:.85em">（黃金 {len(_gold)} 檔 / 今日訊號 {len(_today)} 檔）</span>'
            f'<table style="border-collapse:collapse;width:100%;margin-top:8px;background:#fff">'
            f'<thead><tr style="background:#f57c00;color:#fff">'
            f'<th style="padding:5px 8px">代號</th><th style="padding:5px 8px">名稱</th>'
            f'<th style="padding:5px 8px">觸發訊號</th><th style="padding:5px 8px">起漲前型態</th>'
            f'<th style="padding:5px 8px">建議買價</th>'
            f'</tr></thead><tbody>{_gold_rows}</tbody></table></div>')
    else:
        retro_summary_web = ""

    mstr = ('<span style="color:#28a745;font-weight:bold">● 盤中</span>'
            if market_open else '<span style="color:#aaa">● 收盤</span>')

    if sim is not None:
        _sim_notice, _sim_open, _sim_analysis, _sim_closed = generate_sim_section(sim, split=True)
    else:
        _sim_notice = _sim_open = _sim_analysis = _sim_closed = ""

    return f"""<!DOCTYPE html>
<html lang="zh-Hant">
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="3600">
<title>台股選股 {today_str}</title>
<style>
  body{{font-family:'Microsoft JhengHei',Arial,sans-serif;margin:16px;background:#f4f6f9}}
  h1{{color:#003366;margin-bottom:4px}}
  .bar{{display:flex;gap:18px;align-items:center;flex-wrap:wrap;margin-bottom:10px;font-size:.9em}}
  .note{{background:#fff3cd;border:1px solid #ffc107;padding:10px 14px;border-radius:4px;margin:10px 0;font-size:.83em}}
  .legend{{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:10px}}
  .leg{{padding:3px 10px;border-radius:3px;color:#fff;font-weight:bold;font-size:.82em}}
  table{{border-collapse:collapse;width:100%;background:#fff;box-shadow:0 1px 5px rgba(0,0,0,.1);border-radius:6px;overflow:hidden}}
  th,td{{border:1px solid #e0e0e0;padding:7px 9px;text-align:center;font-size:.87em}}
  th{{background:#003366;color:#fff;position:sticky;top:0;z-index:1;white-space:nowrap}}
  tr:nth-child(even){{background:#f8f9fb}}
  tr:hover{{background:#e8f0ff}}
  footer{{color:#aaa;font-size:.78em;margin-top:12px}}
</style>
</head>
<body>
<h1>台股每日選股推薦 v2</h1>
<div class="bar">
  <span>日期：{today_str}</span>
  <span>市場：{mstr}</span>
  <span>更新：{run_time_str}</span>
  <span style="color:#888">（每小時自動重整）</span>
</div>
<div class="note">
  <b>免責聲明：</b>本工具僅供技術分析參考，不構成投資建議。股市有風險，投資須謹慎。<br>
  日線=波段(6月日K)｜60分線=短線(60天小時K)｜<span style="background:#fffbea;padding:0 4px">黃底</span>=波段+短線共振(皆≥40)
</div>
<div class="legend">
  <span class="leg" style="background:#28a745">綜合≥60</span>
  <span class="leg" style="background:#fd7e14">綜合40-59</span>
  <span class="leg" style="background:#aaa">綜合&lt;40</span>
  <span style="color:#d62728;font-weight:bold">▲上升</span>
  <span style="color:#2ca02c;font-weight:bold">▼下降</span>
  <span style="background:#eafaf1;border:1px solid #27ae60;border-left:4px solid #27ae60;padding:3px 8px;border-radius:3px;font-size:.82em">綠底＝符合進場條件（分數{SIM_ENTRY_SCORE}~{SIM_MAX_SCORE} 且量增≥{SIM_VOL_RATIO_MIN}x）</span>
</div>
{retro_summary_web}
<h2 style="color:#003366;margin:14px 0 6px;font-size:1em;border-left:4px solid #003366;padding-left:8px">▶ 上市推薦股（前 {TOP_N} 名）</h2>
<table>
<thead>
<tr>
  <th>#</th><th>執行時間</th><th>代號</th><th>名稱</th><th>類股</th><th>股價</th>
  <th>今日</th><th>週漲幅</th><th>月漲幅</th><th>RSI</th><th>KD</th>
  <th>月營收YoY</th><th>今日量(張)</th><th>外資(張)</th><th>投信(張)</th>
  <th>日線(波段)</th><th>60分線(短線)</th><th>綜合分</th><th>⚑整理</th><th>🎯回塑訊號</th>
</tr>
</thead>
<tbody>{rows_twse}</tbody>
</table>
<h2 style="color:#7d6608;margin:18px 0 6px;font-size:1em;border-left:4px solid #7d6608;padding-left:8px">▶ 上櫃推薦股（前 {TOP_N} 名）</h2>
<table>
<thead>
<tr>
  <th>#</th><th>執行時間</th><th>代號</th><th>名稱</th><th>類股</th><th>股價</th>
  <th>今日</th><th>週漲幅</th><th>月漲幅</th><th>RSI</th><th>KD</th>
  <th>月營收YoY</th><th>今日量(張)</th><th>外資(張)</th><th>投信(張)</th>
  <th>日線(波段)</th><th>60分線(短線)</th><th>綜合分</th><th>⚑整理</th><th>🎯回塑訊號</th>
</tr>
</thead>
<tbody>{rows_tpex}</tbody>
</table>
{_sim_notice}
{_sim_open}
{_sim_analysis}
{_sim_closed}
<footer>
  分析範圍：全市場上市+上櫃股票（日線 ≥ {PASS1_THRESHOLD} 分者進入精算）｜ 資料來源：Yahoo Finance ｜ 產生：{run_time_str}
</footer>
</body></html>"""


# ─────────────────────────────────────────────
# 報告儲存（daily / hourly）
# ─────────────────────────────────────────────
def save_reports(html, now):
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H%M")

    # 每次都存 hourly
    hourly_path = os.path.join(HOURLY_DIR, f"{date_str}_{time_str}.html")
    with open(hourly_path, "w", encoding="utf-8") as f:
        f.write(html)

    # 每天第一次（或覆蓋最新）存 daily
    daily_path = os.path.join(DAILY_DIR, f"{date_str}.html")
    with open(daily_path, "w", encoding="utf-8") as f:
        f.write(html)

    # 更新 latest.html
    with open(LATEST_HTML, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"    hourly : {hourly_path}")
    print(f"    daily  : {daily_path}")
    print(f"    latest : {LATEST_HTML}")
    return hourly_path


# ─────────────────────────────────────────────
# Email 寄送
# ─────────────────────────────────────────────
def load_email_cfg():
    if not os.path.exists(EMAIL_CFG):
        return None
    try:
        with open(EMAIL_CFG, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return None

def build_email_body(results, today_str, run_time_str, market_open, sim=None, buy_alerts=None):
    """完整報告直接嵌入 email，開信即可看到所有內容"""
    top_twse = sorted([r for r in results if r.get("market", "上市") == "上市"],
                      key=lambda x: x["combined"], reverse=True)[:TOP_N]
    top_tpex = sorted([r for r in results if r.get("market", "上市") == "上櫃"],
                      key=lambda x: x["combined"], reverse=True)[:TOP_N]
    mstr = "盤中" if market_open else "收盤"

    def badge_cell(s, hi=60, mid=40):
        if s is None:
            return '<span style="background:#ddd;color:#999;padding:1px 6px;border-radius:3px;font-size:.9em">—</span>'
        bg = "#28a745" if s >= hi else ("#fd7e14" if s >= mid else "#aaa")
        return f'<span style="background:{bg};color:#fff;padding:1px 6px;border-radius:3px;font-weight:bold;font-size:.9em">{s}</span>'

    def chg_span(v):
        col = "#c0392b" if v > 0 else ("#27ae60" if v < 0 else "#333")
        return f'<span style="color:{col}">{v:+.2f}%</span>'

    td = "padding:6px 8px;border:1px solid #ddd;text-align:center;font-size:.85em"
    tdl = "padding:6px 8px;border:1px solid #ddd;text-align:left;font-size:.85em"

    def _email_rev(r):
        yoy = r.get("rev_yoy")
        mo  = r.get("rev_month", "")
        if yoy is None:
            return '<span style="color:#bbb;font-size:.8em">—</span>'
        col = "#c0392b" if yoy > 0 else ("#27ae60" if yoy < 0 else "#555")
        return (f'<span style="color:{col};font-weight:bold">{yoy:+.1f}%</span>'
                f'<br><span style="font-size:.72em;color:#888">{mo}</span>')

    def email_chart_links(code, market="上市"):
        exchange = "TWSE" if market == "上市" else "TPEX"
        tv = f"https://www.tradingview.com/chart/?symbol={exchange}%3A{code}"
        gi = f"https://goodinfo.tw/tw/ShowK_Chart.asp?STOCK_ID={code}"
        s  = "font-size:.75em;padding:1px 5px;border-radius:2px;text-decoration:none;margin:1px;display:inline-block;color:#fff"
        return (f'<a href="{tv}" style="{s};background:#1565c0">TV</a>'
                f'<a href="{gi}" style="{s};background:#2e7d32">K線</a>')

    def retro_td_email(r):
        lvl = r.get("retro_level", "none")
        sig = r.get("retro_signal", "—")
        pat = r.get("retro_pattern", "")
        if lvl == "gold":
            bg, fg = "#f57c00", "#fff"
        elif lvl == "today":
            bg, fg = "#2e7d32", "#fff"
        elif lvl == "retest":
            bg, fg = "#1565c0", "#fff"
        else:
            return f'<td style="{td};color:#bbb">—</td>'
        pat_html = f'<br><span style="font-size:.7em;color:#fff;opacity:.85">{pat}</span>' if pat else ""
        return (f'<td style="{td};background:{bg};color:{fg};font-weight:bold;'
                f'font-size:.78em;white-space:nowrap">{sig}{pat_html}</td>')

    def _build_email_rows(stock_list):
        _rows = ""
        for i, r in enumerate(stock_list, 1):
            code = r["code"]
            emkt  = r.get("market", "上市")
            emkt_bg = "#1a5276" if emkt == "上市" else "#7d6608"
            emkt_b  = f'<span style="background:{emkt_bg};color:#fff;padding:0 5px;border-radius:3px;font-size:.75em">{emkt}</span>'
            dual      = r["d_score"] >= 40 and (r["h_score"] or 0) >= 40
            can_enter = (SIM_ENTRY_SCORE <= r["combined"] <= SIM_MAX_SCORE
                         and r.get("vol_ratio", 0) >= SIM_VOL_RATIO_MIN)
            if can_enter:
                rbg = "background:#eafaf1;border-left:4px solid #27ae60"
            elif dual:
                rbg = "background:#fffbea"
            else:
                rbg = ""
            if can_enter:
                entry_tag = '<br><span style="font-size:.72em;background:#27ae60;color:#fff;padding:1px 6px;border-radius:3px">★ 可進場</span>'
            elif r["combined"] > SIM_MAX_SCORE:
                entry_tag = f'<br><span style="font-size:.72em;color:#fd7e14">分數偏高(&gt;{SIM_MAX_SCORE})</span>'
            elif r.get("vol_ratio", 0) < SIM_VOL_RATIO_MIN:
                entry_tag = f'<br><span style="font-size:.72em;color:#aaa">量增不足({r.get("vol_ratio",0):.1f}x&lt;{SIM_VOL_RATIO_MIN}x)</span>'
            else:
                entry_tag = ""
            _rows += f"""<tr style="{rbg}">
          <td style="{td}">{i}</td>
          <td style="{td};font-size:.8em;color:#555;white-space:nowrap">{today_str}<br>{run_time_str}</td>
          <td style="{td}"><a href="https://goodinfo.tw/tw/ShowK_Chart.asp?STOCK_ID={code}"
              style="font-weight:bold;color:#003366;text-decoration:none">{code}</a> {emkt_b}<br>
              {email_chart_links(code, emkt)}</td>
          <td style="{tdl}">{r['name']}</td>
          <td style="{tdl};color:#555;font-size:.82em">{r.get('sector','')}</td>
          <td style="{td}"><b>{r['price']}</b></td>
          <td style="{td}">{chg_span(r['week_chg'])}</td>
          <td style="{td}">{chg_span(r['month_chg'])}</td>
          <td style="{td}">{r['rsi']}</td>
          <td style="{td}">{r['k']}/{r['d']}</td>
          <td style="{td}">{_email_rev(r)}</td>
          <td style="{td}">{badge_cell(r['d_score'],55,35)}<br><span style="font-size:.78em;color:#666">{r['d_signals']}</span></td>
          <td style="{td}">{badge_cell(r['h_score'],55,35)}<br><span style="font-size:.78em;color:#666">{r['h_signals']}</span></td>
          <td style="{td}">{badge_cell(r['combined'])}{entry_tag}</td>
          <td style="{td};font-size:.8em;color:#e65100;white-space:nowrap">{'<b>⚑</b> ' + r.get('consol_signal','') if r.get('consol_flag') else '—'}</td>
          {retro_td_email(r)}
        </tr>"""
        return _rows

    rows_twse = _build_email_rows(top_twse)
    rows_tpex = _build_email_rows(top_tpex)

    # 🏆 黃金訊號摘要（W底/V反 + 規則觸發）
    gold_list = [r for r in (top_twse + top_tpex) if r.get("retro_level") == "gold"]
    today_list = [r for r in (top_twse + top_tpex) if r.get("retro_level") == "today"]
    if gold_list or today_list:
        gold_rows = ""
        for r in gold_list + today_list:
            tag = "🏆" if r.get("retro_level") == "gold" else "✓"
            gold_rows += (f'<tr><td style="padding:5px 8px;border:1px solid #ffd180;font-weight:bold">{tag} {r["code"]}</td>'
                          f'<td style="padding:5px 8px;border:1px solid #ffd180">{r["name"]}</td>'
                          f'<td style="padding:5px 8px;border:1px solid #ffd180;font-size:.85em">{r.get("retro_signal","")}</td>'
                          f'<td style="padding:5px 8px;border:1px solid #ffd180;font-size:.85em">{r.get("retro_pattern","")}</td>'
                          f'<td style="padding:5px 8px;border:1px solid #ffd180;color:#c0392b">${r.get("retro_price","-")}</td></tr>')
        retro_summary = f"""
    <div style="background:#fff8e1;border:2px solid #f57c00;border-radius:6px;padding:12px;margin-bottom:14px">
      <b style="color:#e65100;font-size:1.05em">🎯 回塑性訊號摘要</b>
      <span style="color:#666;font-size:.85em">（黃金 {len(gold_list)} 檔 / 今日訊號 {len(today_list)} 檔）</span>
      <table style="border-collapse:collapse;width:100%;margin-top:8px;background:#fff">
      <thead><tr style="background:#f57c00;color:#fff">
        <th style="padding:5px 8px">代號</th><th style="padding:5px 8px">名稱</th>
        <th style="padding:5px 8px">觸發訊號</th><th style="padding:5px 8px">起漲前型態</th>
        <th style="padding:5px 8px">建議買價</th>
      </tr></thead>
      <tbody>{gold_rows}</tbody>
      </table>
    </div>"""
    else:
        retro_summary = ""

    th = "padding:7px 8px;border:1px solid #1a3a6e;background:#003366;color:#fff;white-space:nowrap"
    if sim is not None:
        _sim_notice, _sim_open, _sim_analysis, _sim_closed = generate_sim_section(sim, for_email=True, split=True)
    else:
        _sim_notice = _sim_open = _sim_analysis = _sim_closed = ""
    if buy_alerts:
        alert_rows = ""
        for r in buy_alerts:
            alert_rows += f"""
        <tr>
          <td style="padding:6px 8px;border:1px solid #e0e0e0;font-weight:bold">{r['code']}</td>
          <td style="padding:6px 8px;border:1px solid #e0e0e0">{r['name']}</td>
          <td style="padding:6px 8px;border:1px solid #e0e0e0;color:#c0392b">{r.get('price')}</td>
          <td style="padding:6px 8px;border:1px solid #e0e0e0;color:#c0392b">{r.get('vol_ratio', 0):.1f}x</td>
          <td style="padding:6px 8px;border:1px solid #e0e0e0">{r.get('d_signals','')}</td>
        </tr>"""
        alert_section = f"""
    <div style="background:#fff3cd;border:2px solid #ff9800;border-radius:6px;padding:12px;margin-bottom:16px">
      <b style="color:#d32f2f;font-size:1.05em">🔔 13:20 買入提醒（量>2倍+紅K）</b>
      <table style="border-collapse:collapse;width:100%;margin-top:8px">
      <thead><tr style="background:#ff9800;color:#fff">
        <th style="padding:6px 8px">代號</th><th style="padding:6px 8px">名稱</th>
        <th style="padding:6px 8px">現價</th><th style="padding:6px 8px">量比</th>
        <th style="padding:6px 8px">訊號</th>
      </tr></thead>
      <tbody>{alert_rows}</tbody>
      </table>
    </div>"""
    else:
        alert_section = ""
    _sec_title      = "font-size:.95em;font-weight:bold;color:#003366;border-left:4px solid #003366;padding-left:8px;margin:14px 0 6px"
    _sec_title_tpex = "font-size:.95em;font-weight:bold;color:#7d6608;border-left:4px solid #7d6608;padding-left:8px;margin:18px 0 6px"
    _thead = f"""<thead><tr>
  <th style="{th}">#</th><th style="{th}">執行時間</th><th style="{th}">代號</th><th style="{th}">名稱</th>
  <th style="{th}">類股</th><th style="{th}">股價</th>
  <th style="{th}">週漲幅</th><th style="{th}">月漲幅</th>
  <th style="{th}">RSI</th><th style="{th}">KD</th>
  <th style="{th}">月營收YoY</th>
  <th style="{th}">日線(波段)</th><th style="{th}">60分線(短線)</th>
  <th style="{th}">綜合分</th><th style="{th}">⚑整理</th><th style="{th}">🎯回塑訊號</th>
</tr></thead>"""
    return f"""<html>
<head><meta charset="UTF-8"></head>
<body style="font-family:'Microsoft JhengHei',Arial,sans-serif;margin:0;padding:16px;background:#f4f6f9">
<div style="max-width:1100px;margin:0 auto">
<h2 style="color:#003366;margin-bottom:4px">台股選股推薦 {today_str} {run_time_str} [{mstr}]</h2>
{alert_section}
{retro_summary}
<p style="{_sec_title}">▶ 上市推薦股（前 {TOP_N} 名）</p>
<p style="color:#666;font-size:.88em;margin:4px 0 10px">
  上市分析{sum(1 for r in results if r.get('market','上市')=='上市')}檔 &nbsp;|&nbsp;
  <span style="background:#28a745;color:#fff;padding:1px 6px;border-radius:3px;font-size:.85em">綜合≥60</span>&nbsp;
  <span style="background:#fd7e14;color:#fff;padding:1px 6px;border-radius:3px;font-size:.85em">40-59</span>&nbsp;
  <span style="background:#fffbea;padding:1px 6px;border-radius:3px;border:1px solid #ddd;font-size:.85em">黃底=波段+短線共振</span>
</p>
<table style="border-collapse:collapse;width:100%;background:#fff;box-shadow:0 1px 4px rgba(0,0,0,.1)">
{_thead}
<tbody>{rows_twse}</tbody>
</table>
<p style="{_sec_title_tpex}">▶ 上櫃推薦股（前 {TOP_N} 名）</p>
<p style="color:#666;font-size:.88em;margin:4px 0 10px">
  上櫃分析{sum(1 for r in results if r.get('market','上市')=='上櫃')}檔
</p>
<table style="border-collapse:collapse;width:100%;background:#fff;box-shadow:0 1px 4px rgba(0,0,0,.1)">
{_thead}
<tbody>{rows_tpex}</tbody>
</table>
{_sim_notice}
{_sim_open}
{_sim_analysis}
{_sim_closed}
<p style="color:#aaa;font-size:.78em;margin-top:12px">
  資料來源：Yahoo Finance &nbsp;|&nbsp; 產生：{today_str} {run_time_str}<br>
  <b>免責聲明：</b>本郵件僅供技術分析參考，不構成任何投資建議。
</p>
</div>
</body></html>"""

def _parse_recipients(val):
    """接受字串（逗號分隔）或清單，回傳去空白的 email list"""
    if isinstance(val, list):
        return [v.strip() for v in val if v.strip()]
    # 字串：支援逗號或分號分隔
    import re
    return [v.strip() for v in re.split(r"[,;]", str(val)) if v.strip()]

def send_email(cfg, results, html_path, today_str, run_time_str, market_open, sim=None, buy_alerts=None):
    try:
        sender    = cfg["sender_email"]
        password  = cfg["sender_app_password"]
        receivers = _parse_recipients(cfg.get("recipient_email", "wic0935@gmail.com"))

        if not results:
            print("    [略過] 無分析結果，不寄送 Email")
            return
        top1 = sorted(results, key=lambda x: x["combined"], reverse=True)[0]
        subject_str = f"[台股選股] {today_str} {run_time_str} - 第1名:{top1['code']}{top1['name']}({top1['combined']}分)"

        # 組裝郵件（內文 + HTML 附件）
        body_html = build_email_body(results, today_str, run_time_str, market_open, sim, buy_alerts=buy_alerts)
        msg = MIMEMultipart("mixed")
        msg["Subject"] = Header(subject_str, "utf-8")
        msg["From"]    = sender
        msg["To"]      = ", ".join(receivers)

        # 內文
        msg.attach(MIMEText(body_html, "html", "utf-8"))

        # 附加完整 HTML 報告（含排序功能，可下載後在瀏覽器開啟）
        if html_path and os.path.exists(html_path):
            with open(html_path, "rb") as f:
                attach = MIMEBase("application", "octet-stream")
                attach.set_payload(f.read())
            encoders.encode_base64(attach)
            fname = os.path.basename(html_path)
            attach.add_header("Content-Disposition", "attachment", filename=fname)
            msg.attach(attach)

        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as s:
            s.login(sender, password)
            s.send_message(msg)

        print(f"    email 已寄出 -> {', '.join(receivers)}（附件：{os.path.basename(html_path) if html_path else '無'}）")
    except Exception as e:
        print(f"    [警告] email 寄送失敗: {e}")


def send_sell_signal_email(cfg, exits, today_str, run_time_str):
    """寄出賣出訊號通知信（爆量滯漲/避雷針/大黑K）"""
    try:
        sender    = cfg["sender_email"]
        password  = cfg["sender_app_password"]
        receivers = _parse_recipients(cfg.get("recipient_email", "wic0935@gmail.com"))

        subject = f"[台股] 賣出訊號通知 {today_str} {run_time_str}"

        rows_html = ""
        for p in exits:
            ret = p.get("return_pct", 0)
            ret_color = "#c0392b" if ret < 0 else "#27ae60"
            detail = p.get("sell_detail") or p.get("exit_reason", "")
            rows_html += f"""
<tr>
  <td style="padding:6px 10px;border:1px solid #ddd">{p.get('name','')}</td>
  <td style="padding:6px 10px;border:1px solid #ddd;text-align:center">{p.get('code','')}</td>
  <td style="padding:6px 10px;border:1px solid #ddd;text-align:center">{p.get('entry_date','')}</td>
  <td style="padding:6px 10px;border:1px solid #ddd;text-align:right">{p.get('entry_price','')}</td>
  <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;font-weight:bold">{p.get('exit_price','')}</td>
  <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:{ret_color};font-weight:bold">{ret:+.2f}%</td>
  <td style="padding:6px 10px;border:1px solid #ddd;color:#8e44ad">{detail}</td>
  <td style="padding:6px 10px;border:1px solid #ddd;text-align:center">{today_str} {run_time_str}</td>
</tr>"""

        body = f"""<html><body style="font-family:Arial,sans-serif;font-size:14px">
<h2 style="color:#c0392b">⚠️ 賣出訊號通知</h2>
<p>以下持倉於 {today_str} {run_time_str} 觸發賣出條件，已自動賣出：</p>
<p style="font-size:.85em;color:#555">觸發條件：①爆量滯漲（量&gt;2.5x均量且漲幅&lt;1%）　②避雷針（上影&gt;2x實體且正乖離&gt;10%）　③大黑K（跌幅&gt;4%且跌破5MA）</p>
<table style="border-collapse:collapse;width:100%">
<thead><tr style="background:#1a5276;color:#fff">
  <th style="padding:7px 10px;border:1px solid #1a5276">股票名稱</th>
  <th style="padding:7px 10px;border:1px solid #1a5276">代號</th>
  <th style="padding:7px 10px;border:1px solid #1a5276">買入日期</th>
  <th style="padding:7px 10px;border:1px solid #1a5276">買入價格</th>
  <th style="padding:7px 10px;border:1px solid #1a5276">賣出價格</th>
  <th style="padding:7px 10px;border:1px solid #1a5276">報酬率</th>
  <th style="padding:7px 10px;border:1px solid #1a5276">觸發原因</th>
  <th style="padding:7px 10px;border:1px solid #1a5276">賣出時間</th>
</tr></thead>
<tbody>{rows_html}</tbody>
</table>
</body></html>"""

        msg = MIMEMultipart("mixed")
        msg["Subject"] = Header(subject, "utf-8")
        msg["From"]    = sender
        msg["To"]      = ", ".join(receivers)
        msg.attach(MIMEText(body, "html", "utf-8"))

        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as s:
            s.login(sender, password)
            s.send_message(msg)
        print(f"    [賣出通知] email 已寄出 -> {', '.join(receivers)}")
    except Exception as e:
        print(f"    [警告] 賣出通知 email 失敗: {e}")


def send_telegram_sell_signal(cfg, exits, today_str, run_time_str):
    """發送賣出訊號通知到 Telegram"""
    import urllib.request, urllib.parse, ssl as _ssl

    bots = cfg.get("telegram_bots") or []
    if not bots:
        t = cfg.get("telegram_bot_token", "")
        c = cfg.get("telegram_chat_id", "")
        if t and c:
            bots = [{"token": t, "chat_id": c}]
    if not bots:
        return

    _ctx = _ssl.create_default_context()
    _ctx.check_hostname = False
    _ctx.verify_mode = _ssl.CERT_NONE

    lines = [f"⚠️ 賣出訊號通知 {today_str} {run_time_str}"]
    for p in exits:
        ret    = p.get("return_pct", 0)
        sign   = "+" if ret >= 0 else ""
        detail = p.get("sell_detail") or p.get("exit_reason", "")
        lines.append(
            f"  {p.get('code','')} {p.get('name','')}  "
            f"買:{p.get('entry_price','')} 出:{p.get('exit_price','')}  "
            f"{sign}{ret:.2f}%  [{detail}]"
        )
    text = "\n".join(lines)

    for bot in bots:
        token   = bot.get("token", "")
        chat_id = bot.get("chat_id", "")
        if not token or not chat_id:
            continue
        try:
            data = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode()
            urllib.request.urlopen(
                f"https://api.telegram.org/bot{token}/sendMessage",
                data=data, timeout=15, context=_ctx)
            print(f"    [賣出通知] TG bot {chat_id} 已發送")
        except Exception as e:
            print(f"    [警告] 賣出通知 TG 失敗: {e}")


def send_telegram(cfg, results, html_path, today_str, run_time_str):
    import urllib.request, urllib.parse, ssl as _ssl

    # 支援新格式 telegram_bots（陣列）或舊格式單組 token/chat_id
    bots = cfg.get("telegram_bots") or []
    if not bots:
        t = cfg.get("telegram_bot_token", "")
        c = cfg.get("telegram_chat_id", "")
        if t and c:
            bots = [{"token": t, "chat_id": c}]
    if not bots:
        return

    _ctx = _ssl.create_default_context()
    _ctx.check_hostname = False
    _ctx.verify_mode = _ssl.CERT_NONE

    def _can_enter(r):
        return SIM_ENTRY_SCORE <= r["combined"] <= SIM_MAX_SCORE and r.get("vol_ratio", 0) >= SIM_VOL_RATIO_MIN

    if results:
        top_twse = sorted([r for r in results if r.get("market", "上市") == "上市"],
                          key=lambda x: x["combined"], reverse=True)[:TOP_N]
        top_tpex = sorted([r for r in results if r.get("market", "上市") == "上櫃"],
                          key=lambda x: x["combined"], reverse=True)[:TOP_N]
        top_all  = sorted(top_twse + top_tpex, key=lambda x: x["combined"], reverse=True)

        top3 = top_all[:3]
        lines = [f"📊 台股選股報告 {today_str} {run_time_str}"]
        for i, r in enumerate(top3, 1):
            entry = "★可進場 " if _can_enter(r) else ""
            mkt = f"[{r.get('market','上市')}] " if r.get("market") == "上櫃" else ""
            lines.append(f"{i}. {mkt}{r['code']} {r['name']}  {entry}{r['combined']}分  ${r['price']}")
        text = "\n".join(lines)

        # 重點注意：前10名中有 ★可進場 或 ⚑支撐整理 的股票
        alert_lines = []
        for r in top_all:
            tags = []
            if _can_enter(r):
                tags.append("★可進場")
            if r.get("consol_flag"):
                tags.append(f"⚑{r.get('consol_signal','支撐整理')}")
            if tags:
                mkt = f"[{r.get('market','上市')}] "
                alert_lines.append(f"  {mkt}{r['code']} {r['name']}  {r['combined']}分  ${r['price']}  {'  '.join(tags)}")
        alert_text = f"🔔 重點注意 {today_str} {run_time_str}\n" + "\n".join(alert_lines) if alert_lines else None
    else:
        text = f"📊 台股選股 {today_str} {run_time_str} — 無選股結果"
        alert_text = None

    html_data = None
    if html_path and os.path.exists(html_path):
        with open(html_path, "rb") as f:
            html_data = f.read()

    for bot in bots:
        token   = bot.get("token", "")
        chat_id = bot.get("chat_id", "")
        if not token or not chat_id:
            continue
        try:
            # 文字訊息（摘要）
            data = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode()
            urllib.request.urlopen(
                f"https://api.telegram.org/bot{token}/sendMessage",
                data=data, timeout=15, context=_ctx)

            # 額外提醒（可進場 / 支撐整理）
            if alert_text:
                adata = urllib.parse.urlencode({"chat_id": chat_id, "text": alert_text}).encode()
                urllib.request.urlopen(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    data=adata, timeout=15, context=_ctx)

            # HTML 附件
            if html_data is not None:
                boundary = "TGboundary"
                fname = os.path.basename(html_path)
                body = (
                    f"--{boundary}\r\n"
                    f'Content-Disposition: form-data; name="chat_id"\r\n\r\n{chat_id}\r\n'
                    f"--{boundary}\r\n"
                    f'Content-Disposition: form-data; name="document"; filename="{fname}"\r\n'
                    f"Content-Type: text/html\r\n\r\n"
                ).encode() + html_data + f"\r\n--{boundary}--\r\n".encode()
                req = urllib.request.Request(
                    f"https://api.telegram.org/bot{token}/sendDocument",
                    data=body,
                    headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
                )
                urllib.request.urlopen(req, timeout=30, context=_ctx)

            print(f"    Telegram 已發送 -> chat_id:{chat_id}（附件：{os.path.basename(html_path) if html_path else '無'}）")
        except Exception as e:
            print(f"    [警告] Telegram 發送失敗 (chat_id:{chat_id}): {e}")


# ─────────────────────────────────────────────
# 隔日進場價提醒
# ─────────────────────────────────────────────
def _suggested_entry(r):
    """建議進場價：有支撐取支撐價，否則取昨收 -1%"""
    if r.get("consol_flag") and r.get("consol_support", 0) > 0:
        return round(r["consol_support"], 1), "支撐價"
    return round(r["price"] * 0.99, 1), "昨收-1%"

def save_candidate_watchlist(results, today_str, slot):
    """排程執行後，將當次★可進場候選股存檔，供隔日09:05提醒用（每次覆寫）"""
    candidates = []
    for r in results:
        if SIM_ENTRY_SCORE <= r["combined"] <= SIM_MAX_SCORE and r.get("vol_ratio", 0) >= SIM_VOL_RATIO_MIN:
            ep, eb = _suggested_entry(r)
            candidates.append({
                "code":           r["code"],
                "name":           r["name"],
                "market":         r.get("market", "上市"),
                "combined":       r["combined"],
                "price":          r["price"],
                "consol_flag":    r.get("consol_flag", False),
                "consol_signal":  r.get("consol_signal", ""),
                "suggested_entry": ep,
                "entry_basis":    eb,
            })
    with open(WATCHLIST_JSON, "w", encoding="utf-8") as f:
        json.dump({"date": today_str, "slot": slot, "candidates": candidates},
                  f, ensure_ascii=False, indent=2)
    print(f"    [候選股] 儲存 {len(candidates)} 支★可進場 → candidate_watchlist.json")

def send_telegram_watchlist(cfg, today_str):
    """13:20 掃完後直接發送今日收盤候選股建議進場價（不影響原本兩則訊息）"""
    if not os.path.exists(WATCHLIST_JSON):
        return
    try:
        with open(WATCHLIST_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return
    if data.get("date") != today_str:
        return  # 只發今日資料
    candidates = data.get("candidates", [])
    if not candidates:
        return

    lines = [f"📌 今日收盤候選股 明日建議進場價（{today_str}）"]
    for r in candidates:
        mkt    = f"[{r['market']}] " if r.get("market") == "上櫃" else ""
        consol = f"  ⚑{r['consol_signal']}" if r.get("consol_flag") else ""
        lines.append(
            f"  {mkt}{r['code']} {r['name']}  {r['combined']}分"
            f"  昨收${r['price']}  建議進場:${r['suggested_entry']}({r['entry_basis']}){consol}"
        )
    text = "\n".join(lines)

    import urllib.request, urllib.parse, ssl as _ssl
    _ctx = _ssl.create_default_context()
    _ctx.check_hostname = False
    _ctx.verify_mode = _ssl.CERT_NONE

    bots = cfg.get("telegram_bots") or []
    if not bots:
        t = cfg.get("telegram_bot_token", "")
        c = cfg.get("telegram_chat_id", "")
        if t and c:
            bots = [{"token": t, "chat_id": c}]
    for bot in bots:
        token   = bot.get("token", "")
        chat_id = bot.get("chat_id", "")
        if not token or not chat_id:
            continue
        try:
            enc = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode()
            urllib.request.urlopen(
                f"https://api.telegram.org/bot{token}/sendMessage",
                data=enc, timeout=15, context=_ctx)
            print(f"    [候選股提醒] Telegram 已發送 -> chat_id:{chat_id}")
        except Exception as e:
            print(f"    [警告] 候選股提醒 Telegram 失敗 (chat_id:{chat_id}): {e}")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
def main():
    init_dirs()

    # --scheduled 旗標：由 Windows 工作排程器傳入，代表整點自動執行
    # 手動執行 BAT 不傳此旗標，故不開新倉，只更新現價與出場結算
    is_scheduled = "--scheduled" in sys.argv

    now          = datetime.datetime.now()
    today_str    = now.strftime("%Y/%m/%d")
    run_time_str = now.strftime("%H:%M")

    # ── 假日檢查：國定假日或週末直接結束 ──────────
    if is_holiday(now):
        print(f"=== 台股選股工具 v2  {today_str} {run_time_str} ===")
        print("    今日為國定假日或週末，股市休市，不執行選股。")
        return

    market_open  = is_market_open()

    print(f"=== 台股選股工具 v2  {today_str} {run_time_str} ===")
    print(f"    市場: {'盤中' if market_open else '收盤'}")

    # 台股清單：上市 + 上櫃 合併
    stock_twse = fetch_twse_list()
    stock_twse["market"] = "上市"
    stock_tpex = fetch_tpex_list()
    stock_tpex["market"] = "上櫃"
    stock_list = pd.concat([stock_twse, stock_tpex], ignore_index=True)
    all_pairs  = list(zip(stock_list["code"], stock_list["name"],
                          stock_list["sector"], stock_list["market"]))
    total      = len(all_pairs)
    n_twse     = len(stock_twse)
    n_tpex     = len(stock_tpex)
    if n_tpex == 0:
        print(f"    [注意] 上櫃清單為空，本次僅掃描上市 {n_twse} 檔")
    else:
        print(f"    合計：上市 {n_twse} + 上櫃 {n_tpex} = {total} 檔")

    # ── 第一階段：日線快速掃描全部股票 ──────────────────
    print(f"[2/6] 第一階段：日線掃描全市場（上市+上櫃）{total} 檔（{MAX_WORKERS} 執行緒）...")
    pass1_results, done = [], 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = {pool.submit(_fetch_daily_only, p): p for p in all_pairs}
        for fut in concurrent.futures.as_completed(futs):
            done += 1
            if sys.stdout.isatty():
                sys.stdout.write(f"\r    進度: {done}/{total}  ")
                sys.stdout.flush()
            elif done % 100 == 0 or done == total:
                print(f"    進度: {done}/{total}")
            try:
                r = fut.result()
                if r and r["d_score"] >= PASS1_THRESHOLD:
                    pass1_results.append(r)
            except Exception:
                pass

    pass1_results.sort(key=lambda x: x["d_score"], reverse=True)
    print(f"\n    日線分 >= {PASS1_THRESHOLD} 的候選股：{len(pass1_results)} 檔")

    # ── 月營收基本面（每日快取，不重複呼叫）─────────────
    print(f"[3/6] 抓取候選股月營收資料...")
    codes     = [r["code"] for r in pass1_results]
    fund_map  = fetch_all_revenues(codes)
    for r in pass1_results:
        fund = fund_map.get(r["code"])
        if fund:
            rs_val, _ = rev_score(fund)
            r["rev_yoy"]   = fund["rev_yoy"]
            r["rev_month"] = fund["rev_month"]
            r["rev_score"] = rs_val

    # ── 法人資料 ──────────────────────────────────
    print("[3.5/6] 抓取三大法人資料...")
    inst_map = fetch_institutional(5)
    for r in pass1_results:
        inst = inst_map.get(r["code"], {})
        r["foreign_net"]    = inst.get("foreign_net", None)
        r["trust_net"]      = inst.get("trust_net", None)
        r["consec_foreign"] = inst.get("consec_foreign", 0)
        r["consec_trust"]   = inst.get("consec_trust", 0)
        r["foreign_flag"]   = inst.get("foreign_flag", False)
        r["trust_flag"]     = inst.get("trust_flag", False)

    # ── 第二階段：只對候選股下載小時線 ─────────────────
    print(f"[4/6] 第二階段：小時線精算 {len(pass1_results)} 檔...")
    results, done = [], 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = {pool.submit(_add_hourly, r): r for r in pass1_results}
        for fut in concurrent.futures.as_completed(futs):
            done += 1
            if sys.stdout.isatty():
                sys.stdout.write(f"\r    進度: {done}/{len(pass1_results)}  ")
                sys.stdout.flush()
            elif done % 50 == 0 or done == len(pass1_results):
                print(f"    進度: {done}/{len(pass1_results)}")
            try:
                r2 = fut.result()
                if r2 is not None:
                    results.append(r2)
            except Exception:
                pass

    print(f"\n    完成，顯示前 {TOP_N} 名...")
    prev_map = load_prev()
    save_curr(results)

    print(f"[5/6] 更新模擬下單紀錄...{'（排程觸發，允許開倉）' if is_scheduled else '（手動執行，僅更新現價，不開新倉）'}")
    sim = sim_update(results, allow_entry=is_scheduled)

    print("[6/6] 產生 HTML 報告並儲存...")
    html       = generate_html(results, prev_map, today_str, market_open, run_time_str, sim)
    saved_path = save_reports(html, now)

    # ── 時段計算（供後續 Email / Telegram 分支使用） ──
    _cur_slot = ("09:05" if now.hour <= 9 else
                 "10:00" if now.hour == 10 else
                 "11:00" if now.hour == 11 else
                 "12:00" if now.hour == 12 else
                 "13:20" if now.hour == 13 else "14:00")

    # ── 13:20：收盤買入提醒 + 跌破5MA賣出通知 ──────────
    buy_alerts_1300 = []
    if now.hour == 13:
        for r in results:
            vol_r  = r.get("vol_ratio", 0)
            is_red = r.get("price", 0) > r.get("open_price", r.get("price", 0))
            if vol_r >= 2.0 and is_red and r.get("combined", 0) >= 50:
                buy_alerts_1300.append(r)
        if buy_alerts_1300:
            print(f"\n[13:20提醒] 共 {len(buy_alerts_1300)} 檔觸發買入訊號（量>2x+紅K+分>=50）")
            for r in buy_alerts_1300:
                print(f"    {r['code']} {r['name']}  量比:{r['vol_ratio']}x  現價:{r['price']}")

    cfg = load_email_cfg()

    # ── 13:20：寄出賣出訊號通知（如有） ──────────────────
    if now.hour == 13 and cfg:
        sell_exits = sim.get("last_sell_exits", [])
        if sell_exits:
            print(f"    [賣出訊號] 本次共 {len(sell_exits)} 檔觸發賣出，寄出通知...")
            send_sell_signal_email(cfg, sell_exits, today_str, run_time_str)
            send_telegram_sell_signal(cfg, sell_exits, today_str, run_time_str)
        else:
            print("    [賣出訊號] 本次無持倉觸發賣出條件")

    # ── 每次排程：寄出選股推薦 Email + Telegram ─────────
    if is_scheduled and cfg:
        send_email(cfg, results, saved_path, today_str, run_time_str, market_open, sim,
                   buy_alerts=buy_alerts_1300 if buy_alerts_1300 else None)
        send_telegram(cfg, results, saved_path, today_str, run_time_str)
    elif not cfg:
        print(f"    [提示] 未找到 {EMAIL_CFG}，跳過寄信")

    # ── 儲存候選名單 / 隔日進場價 Telegram ──────────────
    if is_scheduled and cfg:
        save_candidate_watchlist(results, today_str, _cur_slot)
        if now.hour == 13:  # 只在13:20排程發送（避免14:00重複發送）
            send_telegram_watchlist(cfg, today_str)

    print(f"\n[完成] 報告資料夾: {REPORT_DIR}")


if __name__ == "__main__":
    main()
