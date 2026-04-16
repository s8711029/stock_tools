#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
台股進場時間回測分析
分析 09:00 / 10:00 / 11:00 / 12:00 / 13:00 哪個時間進場，
持有 5 個交易日後勝率最高、平均報酬最佳。

資料來源：Yahoo Finance 60 天小時線 + 日線
"""

import os, sys, warnings, datetime, math, webbrowser
import concurrent.futures
import pandas as pd
import yfinance as yf
import requests, io

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────
# 設定
# ─────────────────────────────────────────────
HOLD_DAYS   = 5     # 持有幾個交易日後出場
MAX_WORKERS = 8     # 平行執行緒
try:
    import ctypes, ctypes.wintypes as _wt
    _buf = ctypes.create_unicode_buffer(_wt.MAX_PATH)
    ctypes.windll.shell32.SHGetFolderPathW(None, 0, None, 0, _buf)
    _DESKTOP = _buf.value
except Exception:
    _DESKTOP = os.path.join(os.path.expanduser("~"), "Desktop")
OUTPUT_HTML = os.path.join(_DESKTOP, "stock_reports", "time_analysis.html")

# 分析標的：流動性較高的上市股票（涵蓋各類股）
DEFAULT_STOCKS = [
    ("2330","台積電","半導體"),  ("2454","聯發科","半導體"),
    ("2303","聯電","半導體"),    ("3711","日月光投控","半導體"),
    ("2379","瑞昱","半導體"),    ("2317","鴻海","電子"),
    ("2382","廣達","電子"),      ("2357","華碩","電子"),
    ("2353","宏碁","電子"),      ("2376","技嘉","電子"),
    ("3008","大立光","光學"),    ("2395","研華","電子"),
    ("2409","友達","面板"),      ("2408","南亞科","記憶體"),
    ("2881","富邦金","金融"),    ("2882","國泰金","金融"),
    ("2886","兆豐金","金融"),    ("2891","中信金","金融"),
    ("2884","玉山金","金融"),    ("2885","元大金","金融"),
    ("2412","中華電","電信"),    ("4904","遠傳","電信"),
    ("3045","台灣大","電信"),    ("2308","台達電","電子"),
    ("2002","中鋼","鋼鐵"),      ("1301","台塑","塑膠"),
    ("1303","南亞","塑膠"),      ("6505","台塑化","石化"),
    ("2207","和泰車","汽車"),    ("9910","豐泰","橡膠"),
]

HOURS = [9, 10, 11, 12, 13]
HOUR_LABELS = {9: "09:00", 10: "10:00", 11: "11:00", 12: "12:00", 13: "13:00"}
# yfinance 回傳 UTC 時間；台灣 UTC+8，09:00 TW = 01:00 UTC
UTC_TO_TW = {1: 9, 2: 10, 3: 11, 4: 12, 5: 13}

# ─────────────────────────────────────────────
# 載入分析標的（優先讀取選股結果）
# ─────────────────────────────────────────────
def load_stock_list():
    prev_json = os.path.join(os.path.expanduser("~"), "Desktop",
                             "stock_reports", "prev_scores.json")
    try:
        import json
        if os.path.exists(prev_json):
            with open(prev_json, "r", encoding="utf-8") as f:
                data = json.load(f)
            # 取綜合分前 40 名
            top = sorted(data, key=lambda x: x.get("combined", 0), reverse=True)[:40]
            stocks = [(r["code"], r["name"], r.get("sector","")) for r in top]
            print(f"    從選股結果載入 {len(stocks)} 檔（綜合分前40）")
            return stocks
    except:
        pass
    print(f"    使用預設清單 {len(DEFAULT_STOCKS)} 檔")
    return DEFAULT_STOCKS

# ─────────────────────────────────────────────
# 單股回測
# ─────────────────────────────────────────────
def _get_col(df, col, ticker):
    """處理 yfinance 單層或多層欄位"""
    if (col, ticker) in df.columns:
        return df[(col, ticker)]
    if col in df.columns:
        return df[col]
    # 嘗試第一個符合的欄位
    matches = [c for c in df.columns if (isinstance(c, tuple) and c[0] == col) or c == col]
    if matches:
        return df[matches[0]]
    return None

def analyze_one(args):
    code, name, sector = args
    ticker = code + ".TW"
    try:
        # 用 Ticker().history() 避免多執行緒 yfinance 快取衝突
        t    = yf.Ticker(ticker)
        df_h = t.history(period="60d", interval="60m", auto_adjust=True)
        df_d = t.history(period="60d", interval="1d",  auto_adjust=True)

        if df_h is None or len(df_h) < 20: return None
        if df_d is None or len(df_d) < 10: return None

        # Ticker.history() 回傳單層欄位 Open/Close
        if "Open"  not in df_h.columns: return None
        if "Close" not in df_d.columns: return None

        # 統一轉換為台灣時區（Asia/Taipei），不管 yfinance 回 UTC 或本地
        import pytz
        tw_tz = pytz.timezone("Asia/Taipei")
        if df_h.index.tzinfo is not None:
            df_h.index = df_h.index.tz_convert(tw_tz)
        if df_d.index.tzinfo is not None:
            df_d.index = df_d.index.tz_convert(tw_tz)

        open_s  = df_h["Open"]
        close_d = df_d["Close"]

        # 日線：{台灣日期: 收盤價}
        close_by_day = {}
        for ts, price in close_d.items():
            try:
                close_by_day[ts.date()] = float(price)
            except: pass

        trading_days = sorted(close_by_day.keys())
        if len(trading_days) < HOLD_DAYS + 2: return None

        # 小時線：{台灣日期: {台灣時 9-13: open_price}}
        hourly_by_day = {}
        for ts, price in open_s.items():
            try:
                tw_h = ts.hour          # 已是台灣時區，09:00=9, 10:00=10 ...
                if tw_h not in HOURS: continue
                p = float(price)
                if math.isnan(p) or p <= 0: continue
                hourly_by_day.setdefault(ts.date(), {})[tw_h] = p
            except: continue

        records = []
        for i, entry_day in enumerate(trading_days):
            exit_idx = i + HOLD_DAYS
            if exit_idx >= len(trading_days): break
            exit_day   = trading_days[exit_idx]
            exit_price = close_by_day.get(exit_day)
            if not exit_price: continue

            hours_on_day = hourly_by_day.get(entry_day, {})
            for h in HOURS:
                entry_price = hours_on_day.get(h)
                if not entry_price: continue
                ret = (exit_price - entry_price) / entry_price * 100
                records.append({
                    "code": code, "name": name, "sector": sector,
                    "entry_date": str(entry_day),
                    "hour": h,
                    "entry_price": round(entry_price, 2),
                    "exit_price":  round(exit_price, 2),
                    "return_pct":  round(ret, 3),
                    "win": 1 if ret > 0 else 0,
                })

        return records if records else None
    except:
        return None

# ─────────────────────────────────────────────
# 聚合統計
# ─────────────────────────────────────────────
def aggregate(all_records):
    df = pd.DataFrame(all_records)
    stats = []
    for h in HOURS:
        sub = df[df["hour"] == h]
        if len(sub) == 0: continue
        wins   = sub[sub["win"] == 1]
        losses = sub[sub["win"] == 0]
        rets   = sub["return_pct"]
        stats.append({
            "hour":        h,
            "label":       HOUR_LABELS[h],
            "count":       len(sub),
            "win_count":   len(wins),
            "loss_count":  len(losses),
            "win_rate":    round(len(wins) / len(sub) * 100, 1),
            "avg_ret":     round(rets.mean(), 3),
            "median_ret":  round(rets.median(), 3),
            "avg_win":     round(wins["return_pct"].mean(), 3) if len(wins) > 0 else 0,
            "avg_loss":    round(losses["return_pct"].mean(), 3) if len(losses) > 0 else 0,
            "best":        round(rets.max(), 2),
            "worst":       round(rets.min(), 2),
            "std":         round(rets.std(), 3),
        })

    # 各類股 x 時間
    sector_stats = []
    for sector in df["sector"].unique():
        for h in HOURS:
            sub = df[(df["sector"] == sector) & (df["hour"] == h)]
            if len(sub) < 5: continue
            sector_stats.append({
                "sector":   sector,
                "hour":     HOUR_LABELS[h],
                "count":    len(sub),
                "win_rate": round(sub["win"].mean() * 100, 1),
                "avg_ret":  round(sub["return_pct"].mean(), 3),
            })

    return pd.DataFrame(stats), pd.DataFrame(sector_stats), df

# ─────────────────────────────────────────────
# 產生 HTML 報告
# ─────────────────────────────────────────────
def generate_html(stats_df, sector_df, raw_df, stock_count, run_time):
    def bar(pct, max_w=200):
        w = int(pct / 100 * max_w)
        color = "#28a745" if pct >= 55 else ("#fd7e14" if pct >= 48 else "#dc3545")
        return f'<div style="background:#eee;border-radius:3px;width:{max_w}px;display:inline-block"><div style="background:{color};width:{w}px;height:12px;border-radius:3px"></div></div>'

    def ret_span(v):
        col = "#c0392b" if v > 0 else ("#27ae60" if v < 0 else "#555")
        return f'<span style="color:{col};font-weight:bold">{v:+.3f}%</span>'

    th = "padding:8px 10px;border:1px solid #1a3a6e;background:#003366;color:#fff;white-space:nowrap"
    td = "padding:7px 10px;border:1px solid #ddd;text-align:center"

    # 找最佳時間
    best_win  = stats_df.loc[stats_df["win_rate"].idxmax()]
    best_ret  = stats_df.loc[stats_df["avg_ret"].idxmax()]

    # 主統計表
    main_rows = ""
    for _, r in stats_df.iterrows():
        is_best = r["hour"] == best_win["hour"]
        bg = ' style="background:#fffbea"' if is_best else ""
        main_rows += f"""<tr{bg}>
          <td style="{td};font-size:1.05em;font-weight:bold">{r['label']}</td>
          <td style="{td}">{r['count']}</td>
          <td style="{td}">{r['win_count']} / {r['loss_count']}</td>
          <td style="{td}">{bar(r['win_rate'])} <b>{r['win_rate']}%</b></td>
          <td style="{td}">{ret_span(r['avg_ret'])}</td>
          <td style="{td}">{ret_span(r['median_ret'])}</td>
          <td style="{td};color:#c0392b">{r['avg_win']:+.3f}%</td>
          <td style="{td};color:#27ae60">{r['avg_loss']:+.3f}%</td>
          <td style="{td}">{r['best']:+.2f}%</td>
          <td style="{td}">{r['worst']:+.2f}%</td>
          <td style="{td}">{r['std']:.3f}</td>
        </tr>"""

    # 類股表
    sector_rows = ""
    if len(sector_df) > 0:
        pivot = sector_df.pivot_table(index="sector", columns="hour",
                                      values=["win_rate","avg_ret"], aggfunc="first")
        for sector in pivot.index:
            sector_rows += f'<tr><td style="{td};text-align:left;font-weight:bold">{sector}</td>'
            for hl in [HOUR_LABELS[h] for h in HOURS]:
                try:
                    wr  = pivot[("win_rate", hl)][sector]
                    ar  = pivot[("avg_ret",  hl)][sector]
                    col = "#28a745" if wr >= 55 else ("#fd7e14" if wr >= 48 else "#dc3545")
                    sector_rows += f'<td style="{td};background:{col}22">{wr:.0f}%<br><span style="font-size:.8em">{ar:+.2f}%</span></td>'
                except:
                    sector_rows += f'<td style="{td};color:#bbb">—</td>'
            sector_rows += "</tr>"

    # 每日各時段分布（最近20交易日，以第一檔為代表）
    sample_code = raw_df["code"].value_counts().index[0]
    sample_df = raw_df[raw_df["code"] == sample_code].copy()
    sample_rows = ""
    for _, row in sample_df.tail(25).iterrows():
        rc = "#fef9e7" if row["win"] else "#fdf2f2"
        rr = f'+{row["return_pct"]:.2f}%' if row["win"] else f'{row["return_pct"]:.2f}%'
        col = "#c0392b" if row["win"] else "#27ae60"
        sample_rows += f'<tr style="background:{rc}"><td style="{td}">{row["entry_date"]}</td><td style="{td}">{HOUR_LABELS[row["hour"]]}</td><td style="{td}">{row["entry_price"]}</td><td style="{td}">{row["exit_price"]}</td><td style="{td};color:{col};font-weight:bold">{rr}</td></tr>'

    total_records = len(raw_df)
    best_time_summary = (
        f"<b style='color:#c0392b'>{best_win['label']}</b> 勝率最高（{best_win['win_rate']}%）"
        + ("，與最佳平均報酬時段相同" if best_win["hour"] == best_ret["hour"]
           else f"｜<b style='color:#1565c0'>{best_ret['label']}</b> 平均報酬最高（{best_ret['avg_ret']:+.3f}%）")
    )

    return f"""<!DOCTYPE html>
<html lang="zh-Hant">
<head>
<meta charset="UTF-8">
<title>台股進場時間回測分析</title>
<style>
  body{{font-family:'Microsoft JhengHei',Arial,sans-serif;margin:20px;background:#f4f6f9}}
  h1{{color:#003366}} h2{{color:#1a5276;margin-top:28px;border-bottom:2px solid #d6eaf8;padding-bottom:6px}}
  .summary{{background:#fff;border:1px solid #aed6f1;border-radius:6px;padding:14px 18px;margin:12px 0;font-size:.95em}}
  .highlight{{background:#fffbea;border:2px solid #f39c12;border-radius:6px;padding:12px 16px;margin:12px 0}}
  table{{border-collapse:collapse;width:100%;background:#fff;box-shadow:0 1px 4px rgba(0,0,0,.1);border-radius:6px;overflow:hidden;margin-bottom:20px}}
  th{{padding:8px 10px;border:1px solid #1a3a6e;background:#003366;color:#fff;white-space:nowrap}}
  tr:nth-child(even){{background:#f8f9fb}} tr:hover{{background:#eaf2ff}}
  footer{{color:#aaa;font-size:.8em;margin-top:20px}}
  .note{{background:#fff3cd;border:1px solid #ffc107;padding:10px 14px;border-radius:4px;font-size:.85em;margin:10px 0}}
</style>
</head>
<body>
<h1>台股進場時間回測分析</h1>
<div class="summary">
  分析標的：<b>{stock_count} 檔</b>｜
  回測資料：<b>近 60 天</b>（Yahoo Finance 小時線）｜
  出場條件：<b>持有 {HOLD_DAYS} 個交易日後以收盤價出場</b>｜
  總樣本數：<b>{total_records:,} 筆</b>｜
  分析時間：{run_time}
</div>

<div class="highlight">
  結論：{best_time_summary}
</div>

<div class="note">
  <b>說明</b>：「進場價」為該時段開盤價，「出場價」為 {HOLD_DAYS} 個交易日後當天收盤價。
  勝率 = 報酬率 > 0 的比例。週末、假日不計入交易日。
</div>

<h2>各時段整體統計</h2>
<table>
<thead><tr>
  <th>進場時間</th><th>樣本數</th><th>勝/敗</th><th>勝率</th>
  <th>平均報酬</th><th>中位數報酬</th>
  <th>平均獲利</th><th>平均虧損</th>
  <th>最大獲利</th><th>最大虧損</th><th>標準差</th>
</tr></thead>
<tbody>{main_rows}</tbody>
</table>
<p style="font-size:.82em;color:#888">黃底 = 勝率最高時段</p>

<h2>各類股 × 時段勝率熱圖</h2>
<p style="font-size:.85em;color:#555">格內：勝率% / 平均報酬%（樣本 &lt; 5 筆者略去）</p>
<table>
<thead><tr>
  <th>類股</th>
  {''.join(f'<th>{HOUR_LABELS[h]}</th>' for h in HOURS)}
</tr></thead>
<tbody>{sector_rows if sector_rows else f'<tr><td colspan="6" style="text-align:center;color:#aaa;padding:12px">資料不足</td></tr>'}</tbody>
</table>

<h2>個股明細樣本（{sample_code} 最近 25 筆）</h2>
<table>
<thead><tr>
  <th>進場日</th><th>進場時間</th><th>進場價</th><th>出場價</th><th>報酬率</th>
</tr></thead>
<tbody>{sample_rows}</tbody>
</table>

<footer>
  資料來源：Yahoo Finance｜分析工具：tw_stock_time_analysis.py｜{run_time}<br>
  <b>免責聲明：</b>本分析為歷史回測，不保證未來績效，不構成投資建議。
</footer>
</body></html>"""

# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
def main():
    run_time = datetime.datetime.now().strftime("%Y/%m/%d %H:%M")
    print(f"=== 台股進場時間回測分析 {run_time} ===\n")

    os.makedirs(os.path.dirname(OUTPUT_HTML), exist_ok=True)

    stocks = load_stock_list()
    print(f"[1/3] 下載 {len(stocks)} 檔小時線資料（{MAX_WORKERS} 執行緒）...")

    all_records = []
    done = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = {pool.submit(analyze_one, s): s for s in stocks}
        for fut in concurrent.futures.as_completed(futs):
            done += 1
            sys.stdout.write(f"\r    進度: {done}/{len(stocks)}  ")
            sys.stdout.flush()
            result = fut.result()
            if result:
                all_records.extend(result)

    print(f"\n    共取得 {len(all_records):,} 筆回測紀錄")

    if len(all_records) < 50:
        print("[錯誤] 資料量不足，請確認網路連線後重試")
        return

    print("[2/3] 統計各時段績效...")
    stats_df, sector_df, raw_df = aggregate(all_records)

    print("\n── 各時段勝率 ──")
    for _, r in stats_df.iterrows():
        print(f"  {r['label']}  勝率={r['win_rate']}%  平均報酬={r['avg_ret']:+.3f}%  樣本={r['count']}")

    print("\n[3/3] 產生 HTML 報告...")
    html = generate_html(stats_df, sector_df, raw_df, len(stocks), run_time)
    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"[完成] 報告已儲存：{OUTPUT_HTML}")
    webbrowser.open(f"file:///{OUTPUT_HTML.replace(os.sep, '/')}")


if __name__ == "__main__":
    main()
