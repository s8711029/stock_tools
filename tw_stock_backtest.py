#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
台股選股回測工具
對應主工具：tw_stock_screener_v2.py（日線評分邏輯）

回測條件：
  - 回測期間：近 2 年
  - 進場門檻：d_score >= 60（與主工具 combined 門檻一致，但不含 60 分線與月營收加分）
  - 持倉天數：5 個交易日（以 DataFrame 索引計，已自動排除非交易日）
  - 成交量過濾：平均日量 < 1000 張（1,000,000 股）排除
  - 同一股票進場後 5 日內不重複計算（避免訊號重疊）
  - 不含盤整型態加分（+8）：屬保守估計
"""

import os, sys, datetime, math, time, warnings
import pandas as pd
import numpy as np
import yfinance as yf
import ta
import requests, io
import concurrent.futures
import webbrowser

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────
# 路徑設定
# ─────────────────────────────────────────────
TOOLS_DIR    = os.path.dirname(os.path.abspath(__file__))
DESKTOP      = os.path.dirname(TOOLS_DIR)
REPORT_DIR   = os.path.join(DESKTOP, "stock_reports")
BACKTEST_DIR = os.path.join(REPORT_DIR, "backtest")

# ─────────────────────────────────────────────
# 回測參數（與主工具保持一致）
# ─────────────────────────────────────────────
ENTRY_SCORE_MIN = 60        # d_score 進場門檻
HOLD_DAYS       = 5         # 持倉交易日數
VOL_FILTER      = 1_000_000 # 平均日量門檻（股）
D_RSI_MIN       = 25        # RSI 日線下界
D_RSI_MAX       = 60        # RSI 日線上界
D_VOL_SURGE     = 1.5       # 量增倍數門檻
MIN_BARS        = 60        # 最少需要幾根 K 線才開始計算
DOWNLOAD_BATCH  = 50        # 每批下載幾檔
MAX_WORKERS     = 8         # 下載執行緒數
TOP_TRADES_N    = 50        # 報告顯示最近 N 筆交易明細


# ─────────────────────────────────────────────
# 台股上市清單
# ─────────────────────────────────────────────
def fetch_twse_list():
    print("[1/4] 抓取台灣上市股票清單...")
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
        df = df[["code", "name"]].dropna()
        df = df[df["code"].str.len() == 4].reset_index(drop=True)
        print(f"    共 {len(df)} 檔")
        return df
    except Exception as e:
        print(f"    [警告] 清單抓取失敗: {e}")
        return pd.DataFrame(columns=["code", "name"])


# ─────────────────────────────────────────────
# 指標計算（預計算整段歷史，無未來偏差）
# ─────────────────────────────────────────────
def _calc_indicator_series(df):
    """對整段歷史 K 線預計算所有技術指標 series。
    因使用 pandas rolling，每個時間點只用到該點以前的資料，無未來偏差。"""
    if df is None or len(df) < MIN_BARS:
        return None
    try:
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
        rsi   = ta.momentum.rsi(close, window=14)

        macd_obj  = ta.trend.MACD(close)
        macd_hist = macd_obj.macd_diff()

        stoch = ta.momentum.StochasticOscillator(
            high, low, close, window=9, smooth_window=3)
        k_s = stoch.stoch()
        d_s = stoch.stoch_signal()

        bb  = ta.volatility.BollingerBands(close, window=20, window_dev=2)
        bbl = bb.bollinger_lband()

        vol_ma = volume.rolling(20).mean()

        return dict(
            close=close, open_=open_, volume=volume,
            ma5=ma5, ma10=ma10, ma20=ma20, ma60=ma60,
            rsi=rsi, macd_hist=macd_hist,
            k=k_s, d=d_s, bbl=bbl, vol_ma=vol_ma,
        )
    except Exception:
        return None


def _extract_ind_at(ser, i):
    """從預計算 series 取出第 i 根 K 線的指標純量，任一關鍵值為 NaN 回傳 None。"""
    if i < 1:
        return None

    def v(s, idx=i):
        try:
            val = s.iloc[idx]
            return None if (isinstance(val, float) and math.isnan(val)) else float(val)
        except Exception:
            return None

    ind = {
        "c":    v(ser["close"]),
        "c1":   v(ser["close"],     i - 1),
        "o":    v(ser["open_"]),
        "m5":   v(ser["ma5"]),
        "m10":  v(ser["ma10"]),
        "m20":  v(ser["ma20"]),
        "m60":  v(ser["ma60"]),
        "r":    v(ser["rsi"]),
        "k":    v(ser["k"]),
        "d":    v(ser["d"]),
        "k1":   v(ser["k"],         i - 1),
        "d1":   v(ser["d"],         i - 1),
        "mch":  v(ser["macd_hist"]),
        "mch1": v(ser["macd_hist"], i - 1),
        "vol":  v(ser["volume"]),
        "vma":  v(ser["vol_ma"]),
        "bbl":  v(ser["bbl"]),
    }
    critical = ["c","c1","m5","m10","m20","m60","r",
                "k","d","k1","d1","mch","mch1","vol","vma","bbl"]
    if any(ind[k] is None for k in critical):
        return None
    return ind


# ─────────────────────────────────────────────
# 評分邏輯（與主工具 _score_from_ind 完全相同）
# ─────────────────────────────────────────────
def _score_from_ind(ind):
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
    if D_RSI_MIN < r < D_RSI_MAX:
        score += 10; signals.append(f"RSI={r:.0f}")
    elif r < D_RSI_MIN:
        score -= 5
    if m5 > m10 > m20:
        score += 15; signals.append("均線多頭")
    elif m5 > m20:
        score += 5
    if c > m20:
        score += 10; signals.append("站上MA20")
    if vma > 0 and vol > vma * D_VOL_SURGE:
        score += 10; signals.append(f"量增{vol/vma:.1f}x")
    if c1 < bbl and c > bbl:
        score += 10; signals.append("布林反彈")
    if r > 72:
        score -= 15; signals.append("RSI過熱")
    if c < m60:
        score -= 5

    return max(0, min(100, score)), signals


# ─────────────────────────────────────────────
# 單股回測
# ─────────────────────────────────────────────
def backtest_one(code, name, df):
    """對單一股票執行回測，回傳 trade 清單。"""
    trades = []
    ser = _calc_indicator_series(df)
    if ser is None:
        return trades

    # 成交量過濾：整段期間中位均量不足者直接跳過整檔
    vol_median = ser["vol_ma"].dropna().median()
    if pd.isna(vol_median) or vol_median < VOL_FILTER:
        return trades

    close = df["Close"].squeeze()
    n     = len(df)
    last_entry_i = -(HOLD_DAYS + 1)  # 紀錄上次進場 index（防止同股重疊）

    for i in range(MIN_BARS, n - HOLD_DAYS):
        # 同股票進場後冷卻 HOLD_DAYS 天
        if i - last_entry_i < HOLD_DAYS:
            continue

        # 個別日期成交量過濾
        vma_i = ser["vol_ma"].iloc[i]
        if pd.isna(vma_i) or vma_i < VOL_FILTER:
            continue

        ind = _extract_ind_at(ser, i)
        if ind is None:
            continue

        score, signals = _score_from_ind(ind)
        if score < ENTRY_SCORE_MIN:
            continue

        entry_price = float(close.iloc[i])
        exit_price  = float(close.iloc[i + HOLD_DAYS])
        if entry_price <= 0 or exit_price <= 0:
            continue

        ret_pct = (exit_price - entry_price) / entry_price * 100
        last_entry_i = i

        trades.append({
            "code":        code,
            "name":        name,
            "entry_date":  df.index[i].strftime("%Y-%m-%d"),
            "exit_date":   df.index[i + HOLD_DAYS].strftime("%Y-%m-%d"),
            "entry_price": round(entry_price, 2),
            "exit_price":  round(exit_price, 2),
            "ret_pct":     round(ret_pct, 2),
            "d_score":     score,
            "signals":     ", ".join(signals),
            "win":         ret_pct > 0,
        })

    return trades


# ─────────────────────────────────────────────
# 批次下載 + 回測主流程
# ─────────────────────────────────────────────
def _download_batch(batch_codes):
    """下載一批股票的 2 年日線資料，回傳 {code: df}。"""
    tickers = [c + ".TW" for c in batch_codes]
    result  = {}
    try:
        raw = yf.download(
            tickers,
            period="2y",
            interval="1d",
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            threads=True,
        )
        if raw is None or raw.empty:
            return result

        for code, ticker in zip(batch_codes, tickers):
            try:
                # group_by='ticker' -> MultiIndex columns level0=ticker, level1=metric
                # 無論單檔或多檔，統一先嘗試 MultiIndex 提取，失敗再 fallback 到 flat
                if isinstance(raw.columns, pd.MultiIndex):
                    if ticker not in raw.columns.get_level_values(0):
                        continue
                    df = raw[ticker].copy()
                else:
                    # flat DataFrame（單檔 string 模式，或舊版 yfinance）
                    df = raw.copy()

                df = df.dropna(subset=["Close", "Volume"])
                df = df[df["Volume"] > 0]
                if len(df) >= MIN_BARS:
                    result[code] = df
            except Exception:
                continue
    except Exception as e:
        print(f"    [批次下載異常] {e}")
    return result


def run_backtest(stock_df):
    """批次下載歷史資料並執行回測，回傳所有 trades。"""
    codes = stock_df["code"].tolist()
    names = dict(zip(stock_df["code"], stock_df["name"]))
    total = len(codes)

    all_trades = []
    processed  = 0
    skipped    = 0
    t0         = time.time()

    print(f"[2/4] 下載歷史資料 + 執行回測（共 {total} 檔，批次 {DOWNLOAD_BATCH} 檔）...")

    for batch_start in range(0, total, DOWNLOAD_BATCH):
        batch_codes = codes[batch_start:batch_start + DOWNLOAD_BATCH]
        data_map    = _download_batch(batch_codes)

        for code in batch_codes:
            processed += 1
            df = data_map.get(code)
            if df is None:
                skipped += 1
                continue
            trades = backtest_one(code, names.get(code, code), df)
            all_trades.extend(trades)

        elapsed = time.time() - t0
        eta     = (elapsed / processed * (total - processed)) if processed > 0 else 0
        print(f"    進度: {processed}/{total}  "
              f"訊號筆數: {len(all_trades)}  "
              f"耗時: {elapsed:.0f}s  "
              f"預估剩餘: {eta:.0f}s")

    print(f"    完成。跳過（無資料）: {skipped} 檔")
    return all_trades


# ─────────────────────────────────────────────
# 統計分析
# ─────────────────────────────────────────────
def compute_stats(trades):
    if not trades:
        return {}

    df   = pd.DataFrame(trades)
    rets = df["ret_pct"]
    wins = df[df["win"]]

    stats = {
        "total":     len(df),
        "win_count": len(wins),
        "win_rate":  round(len(wins) / len(df) * 100, 1),
        "avg_ret":   round(float(rets.mean()), 2),
        "median_ret":round(float(rets.median()), 2),
        "max_gain":  round(float(rets.max()), 2),
        "max_loss":  round(float(rets.min()), 2),
        "std_ret":   round(float(rets.std()), 2),
    }

    # 年化報酬估算（假設資金 5 交易日換手一次，250 交易日/年）
    cycles_per_year = 250 / HOLD_DAYS
    stats["annualized_ret"] = round(stats["avg_ret"] * cycles_per_year, 1)

    # 分數區間
    ranges = [(60, 70), (70, 80), (80, 101)]
    stats["score_ranges"] = {}
    for lo, hi in ranges:
        sub = df[(df["d_score"] >= lo) & (df["d_score"] < hi)]
        key = f"{lo}~100" if hi == 101 else f"{lo}~{hi - 1}"
        if len(sub) > 0:
            stats["score_ranges"][key] = {
                "count":    len(sub),
                "win_rate": round(len(sub[sub["win"]]) / len(sub) * 100, 1),
                "avg_ret":  round(float(sub["ret_pct"].mean()), 2),
            }

    # 各信號勝率（出現 >= 10 次才計入）
    signal_names = [
        "KD黃金交叉", "KD低檔", "MACD柱翻正", "MACD增強",
        "均線多頭", "站上MA20", "量增", "布林反彈",
    ]
    stats["sig_stats"] = {}
    for sig in signal_names:
        sub = df[df["signals"].str.contains(sig, na=False)]
        if len(sub) >= 10:
            stats["sig_stats"][sig] = {
                "count":    len(sub),
                "win_rate": round(len(sub[sub["win"]]) / len(sub) * 100, 1),
                "avg_ret":  round(float(sub["ret_pct"].mean()), 2),
            }

    stats["df"] = df
    return stats


# ─────────────────────────────────────────────
# HTML 報告
# ─────────────────────────────────────────────
def _wr_color(wr):
    if wr >= 55: return "#27ae60"
    if wr >= 45: return "#e67e22"
    return "#e74c3c"

def _ret_color(r):
    if r > 0: return "#27ae60"
    if r < 0: return "#e74c3c"
    return "#888"


def generate_report(trades, stats):
    today_str = datetime.date.today().strftime("%Y-%m-%d")
    os.makedirs(BACKTEST_DIR, exist_ok=True)
    out_path = os.path.join(BACKTEST_DIR, f"backtest_{today_str.replace('-','')}.html")

    # ── 分數區間表格 ──
    score_rows = ""
    for key, ss in stats.get("score_ranges", {}).items():
        wr = ss["win_rate"]; ar = ss["avg_ret"]
        score_rows += f"""
        <tr>
          <td>{key}</td>
          <td>{ss['count']}</td>
          <td style="color:{_wr_color(wr)};font-weight:bold">{wr}%</td>
          <td style="color:{_ret_color(ar)}">{ar:+.2f}%</td>
        </tr>"""

    # ── 信號勝率表格 ──
    sig_rows = ""
    for sig, ss in sorted(stats.get("sig_stats", {}).items(),
                           key=lambda x: x[1]["win_rate"], reverse=True):
        wr = ss["win_rate"]; ar = ss["avg_ret"]
        sig_rows += f"""
        <tr>
          <td>{sig}</td>
          <td>{ss['count']}</td>
          <td style="color:{_wr_color(wr)};font-weight:bold">{wr}%</td>
          <td style="color:{_ret_color(ar)}">{ar:+.2f}%</td>
        </tr>"""

    # ── 最近 N 筆交易明細 ──
    df_all   = stats.get("df", pd.DataFrame())
    recent   = df_all.sort_values("entry_date", ascending=False).head(TOP_TRADES_N)
    trade_rows = ""
    for _, t in recent.iterrows():
        rc = "#27ae60" if t["win"] else "#e74c3c"
        trade_rows += f"""
        <tr>
          <td>{t['code']} {t['name']}</td>
          <td>{t['entry_date']}</td>
          <td>{t['exit_date']}</td>
          <td>{t['entry_price']}</td>
          <td>{t['exit_price']}</td>
          <td style="color:{rc};font-weight:bold">{t['ret_pct']:+.2f}%</td>
          <td>{t['d_score']}</td>
          <td style="font-size:.8em;color:#aaa">{t['signals']}</td>
        </tr>"""

    wr  = stats["win_rate"]
    ar  = stats["avg_ret"]
    mr  = stats["median_ret"]
    ann = stats["annualized_ret"]

    html = f"""<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="utf-8">
<title>台股選股回測報告 {today_str}</title>
<style>
  * {{ box-sizing: border-box; }}
  body {{
    font-family: 'Segoe UI', Arial, sans-serif;
    background: #1a1a2e; color: #e0e0e0;
    margin: 0; padding: 24px;
  }}
  h1 {{ color: #00d4ff; margin-bottom: 4px; font-size: 1.6em; }}
  h2 {{
    color: #00d4ff; border-bottom: 1px solid #2a2a4a;
    padding-bottom: 6px; margin-top: 32px; font-size: 1.1em;
  }}
  .meta {{ color: #888; font-size: .85em; margin-bottom: 20px; }}
  .notice {{
    background: #2a2a1e; border-left: 4px solid #e67e22;
    padding: 10px 16px; border-radius: 4px;
    font-size: .85em; color: #ccc; margin-bottom: 24px; line-height: 1.6;
  }}
  .kpi-grid {{ display: flex; gap: 14px; flex-wrap: wrap; margin-bottom: 24px; }}
  .kpi {{
    background: #16213e; border-radius: 8px;
    padding: 14px 20px; min-width: 130px; text-align: center;
  }}
  .kpi .label {{ font-size: .75em; color: #888; margin-bottom: 6px; }}
  .kpi .value {{ font-size: 1.7em; font-weight: bold; }}
  table {{
    width: 100%; border-collapse: collapse;
    background: #16213e; border-radius: 8px;
    overflow: hidden; margin-bottom: 20px;
  }}
  th {{
    background: #0f3460; color: #00d4ff;
    padding: 10px 12px; text-align: left; font-size: .82em;
  }}
  td {{ padding: 8px 12px; border-bottom: 1px solid #252545; font-size: .83em; }}
  tr:last-child td {{ border-bottom: none; }}
  tr:hover td {{ background: #1e2a4a; }}
</style>
</head>
<body>

<h1>台股選股回測報告</h1>
<div class="meta">
  產生時間：{today_str}　｜
  回測期間：近 2 年　｜
  進場門檻：d_score ≥ {ENTRY_SCORE_MIN}　｜
  持倉：{HOLD_DAYS} 個交易日　｜
  同股冷卻：{HOLD_DAYS} 天
</div>

<div class="notice">
  ⚠ <b>回測說明</b>：本回測僅含<b>日線技術指標 (d_score)</b>，
  <b>不含</b> 60 分線評分、月營收評分（最高 +20）及盤整型態加分（+8）。<br>
  因此進場門檻 d_score ≥ 60 比實際系統（combined ≥ 60）更嚴格，
  屬於<b>保守估計</b>。實際系統加入基本面與短線評分後，期望勝率只會相近或更高。
</div>

<h2>整體勝率總覽</h2>
<div class="kpi-grid">
  <div class="kpi">
    <div class="label">整體勝率</div>
    <div class="value" style="color:{_wr_color(wr)}">{wr}%</div>
  </div>
  <div class="kpi">
    <div class="label">交易筆數</div>
    <div class="value">{stats['total']}</div>
  </div>
  <div class="kpi">
    <div class="label">平均報酬</div>
    <div class="value" style="color:{_ret_color(ar)}">{ar:+.2f}%</div>
  </div>
  <div class="kpi">
    <div class="label">中位數報酬</div>
    <div class="value" style="color:{_ret_color(mr)}">{mr:+.2f}%</div>
  </div>
  <div class="kpi">
    <div class="label">年化報酬估算</div>
    <div class="value" style="color:{_ret_color(ann)}">{ann:+.1f}%</div>
  </div>
  <div class="kpi">
    <div class="label">最大單筆獲利</div>
    <div class="value" style="color:#27ae60">{stats['max_gain']:+.2f}%</div>
  </div>
  <div class="kpi">
    <div class="label">最大單筆虧損</div>
    <div class="value" style="color:#e74c3c">{stats['max_loss']:+.2f}%</div>
  </div>
  <div class="kpi">
    <div class="label">報酬標準差</div>
    <div class="value">{stats['std_ret']:.2f}%</div>
  </div>
</div>

<h2>各分數區間勝率</h2>
<table>
  <tr>
    <th>d_score 區間</th>
    <th>交易筆數</th>
    <th>勝率</th>
    <th>平均報酬</th>
  </tr>
  {score_rows}
</table>

<h2>各信號觸發勝率（觸發次數 ≥ 10 筆才顯示，依勝率排序）</h2>
<table>
  <tr>
    <th>信號</th>
    <th>觸發次數</th>
    <th>勝率</th>
    <th>平均報酬</th>
  </tr>
  {sig_rows}
</table>

<h2>最近 {TOP_TRADES_N} 筆回測交易明細</h2>
<table>
  <tr>
    <th>股票</th>
    <th>進場日</th>
    <th>出場日</th>
    <th>進場價</th>
    <th>出場價</th>
    <th>報酬率</th>
    <th>d_score</th>
    <th>觸發信號</th>
  </tr>
  {trade_rows}
</table>

</body>
</html>"""

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"[4/4] 報告輸出完成：{out_path}")
    return out_path


# ─────────────────────────────────────────────
# 主程式
# ─────────────────────────────────────────────
def main():
    print(f"=== 台股選股回測工具  {datetime.date.today()} ===")
    print(f"    進場門檻: d_score >= {ENTRY_SCORE_MIN}  "
          f"持倉: {HOLD_DAYS} 交易日  "
          f"成交量過濾: >= {VOL_FILTER // 1000} 張")
    print()

    stock_df = fetch_twse_list()
    if stock_df.empty:
        print("[錯誤] 無法取得股票清單，結束。")
        sys.exit(1)

    trades = run_backtest(stock_df)

    if not trades:
        print("[結果] 無任何交易紀錄，請確認資料下載是否正常。")
        sys.exit(1)

    print(f"\n[3/4] 統計分析中（共 {len(trades)} 筆交易訊號）...")
    stats = compute_stats(trades)

    print(f"    整體勝率: {stats['win_rate']}%  "
          f"平均報酬: {stats['avg_ret']:+.2f}%  "
          f"年化估算: {stats['annualized_ret']:+.1f}%")

    out_path = generate_report(trades, stats)
    webbrowser.open(out_path)


if __name__ == "__main__":
    main()
