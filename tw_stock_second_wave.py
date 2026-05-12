#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
台股第二波買點分析工具 (建立日: 1150502)
- 分析目前持倉及已賣出股
- 周線12週：確認第一波上漲(>20%+大量) + 第二波量縮回檔不破支撐
- 日線確認：量增(5/10/20MA交叉向上) + 20MA及60MA向上
- 每天17:00執行，發送Email + Telegram
"""

import os, sys, json, datetime, smtplib, ssl, warnings
import urllib.request, urllib.parse
import concurrent.futures
import yfinance as yf
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text      import MIMEText
from email.mime.base      import MIMEBase
from email               import encoders
from email.header        import Header

warnings.filterwarnings("ignore")

# ─── 路徑 ───
TOOLS_DIR  = os.path.dirname(os.path.abspath(__file__))
DESKTOP    = os.path.dirname(TOOLS_DIR)
REPORT_DIR = os.path.join(DESKTOP, "stock_reports")
SIM_JSON   = os.path.join(REPORT_DIR, "sim_trades.json")
EMAIL_CFG  = os.path.join(DESKTOP, "stock_email_config.json")

MAX_WORKERS    = 8
WAVE1_MIN_RISE = 0.20   # 第一波漲幅門檻
VOL_CONTRACT_RATIO = 0.70  # 量縮門檻：本週量 < 第一波最高週量的70%


# ─── 工具函數 ───

def _tw_ticker(code, market="上市"):
    return f"{code}.TW" if market == "上市" else f"{code}.TWO"


def _load_config():
    try:
        with open(EMAIL_CFG, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _parse_recipients(val):
    if isinstance(val, list):
        return [v.strip() for v in val if v.strip()]
    return [v.strip() for v in str(val).split(",") if v.strip()]


def _load_watchlist():
    """從 sim_trades.json 取出所有 open + closed 股票（去重）"""
    try:
        with open(SIM_JSON, "r", encoding="utf-8") as f:
            sim = json.load(f)
    except Exception:
        print("    [警告] 無法讀取 sim_trades.json")
        return []

    seen = {}
    for pos in sim.get("open", []) + sim.get("closed", []):
        code = pos.get("code", "")
        if not code:
            continue
        if code not in seen:
            seen[code] = {
                "code":   code,
                "name":   pos.get("name", code),
                "market": pos.get("market", "上市"),
            }
    return list(seen.values())


# ─── 核心分析 ───

def _analyze_second_wave(code, market, name):
    """
    分析個股是否符合第二波買點條件。
    回傳分析結果 dict（符合）或 None（不符合）。
    """
    try:
        ticker = yf.Ticker(_tw_ticker(code, market))

        # ══ 第一階段：周線分析（近26週 = 6個月）══

        df_w = ticker.history(period="7mo", interval="1wk")
        if df_w is None or len(df_w) < 10:
            return None
        df_w = df_w.tail(26).copy().reset_index(drop=True)

        close_w = df_w["Close"].tolist()
        low_w   = df_w["Low"].tolist()
        vol_w   = df_w["Volume"].tolist()
        n_w     = len(close_w)

        # 5週均量
        def _vol_ma5(i):
            s = max(0, i - 4)
            return sum(vol_w[s:i+1]) / (i - s + 1)

        # 10週均價
        def _ma10(i):
            s = max(0, i - 9)
            return sum(close_w[s:i+1]) / (i - s + 1)

        # ── 在前(n_w-1)週內找最高收盤（確保高點不在最後一週）──
        search_end = n_w - 1
        if search_end < 4:
            return None

        peak_idx   = max(range(search_end), key=lambda i: close_w[i])
        peak_price = close_w[peak_idx]

        # 高點後至少要有1根回跌周線
        if peak_idx >= n_w - 1:
            return None

        # 高點前的最低點 = 第一波起漲點
        if peak_idx < 1:
            return None
        start_idx   = min(range(peak_idx + 1), key=lambda i: close_w[i])
        start_price = close_w[start_idx]

        # 第一波漲幅 > 20%
        if start_price <= 0:
            return None
        wave1_rise = (peak_price - start_price) / start_price
        if wave1_rise < WAVE1_MIN_RISE:
            return None

        # 第一波期間是否有大量（任一週量 > 5週均量）
        wave1_vols = vol_w[start_idx:peak_idx + 1]
        if not wave1_vols:
            return None
        wave1_peak_vol = max(wave1_vols)
        wave1_has_big_vol = any(
            vol_w[i] > _vol_ma5(i)
            for i in range(start_idx, peak_idx + 1)
        )
        if not wave1_has_big_vol:
            return None

        # ── 第二波回跌確認 ──
        # 現價低於高點至少 1%（確認已出現回跌）
        if close_w[-1] >= peak_price * 0.99:
            return None

        # ── 量縮確認 ──
        cur_vol     = vol_w[-1]
        vol_ma5_cur = _vol_ma5(n_w - 1)

        pullback_vols = vol_w[peak_idx + 1:]
        if not pullback_vols:
            return None
        wave1_avg_vol    = sum(wave1_vols) / len(wave1_vols)
        pullback_avg_vol = sum(pullback_vols) / len(pullback_vols)
        vol_shrink_period = pullback_avg_vol < wave1_avg_vol     # 回跌期均量 < 第一波均量
        vol_shrink_cur    = cur_vol < vol_ma5_cur                # 本週量 < 5週均量
        vol_shrink_peak   = (wave1_peak_vol > 0
                             and cur_vol < wave1_peak_vol * VOL_CONTRACT_RATIO)  # 本週量 < 峰量70%

        # 三個量縮條件至少滿足兩個
        if sum([vol_shrink_period, vol_shrink_cur, vol_shrink_peak]) < 2:
            return None

        # ── 支撐不破（20週均線，代表大趨勢支撐；或20週均線 OR 起漲點取高者）──
        def _ma_n(n, i):
            s = max(0, i - n + 1)
            return sum(close_w[s:i+1]) / (i - s + 1)

        ma20w_cur  = _ma_n(20, n_w - 1)
        recent_low = min(low_w[max(0, n_w - 4):])

        if recent_low < ma20w_cur:
            return None  # 跌破20週均線
        if recent_low < start_price:
            return None  # 跌破第一波起漲點

        # ══ 第二階段：日線確認 ══

        df_d = ticker.history(period="4mo", interval="1d")
        if df_d is None or len(df_d) < 30:
            return None

        close_d = df_d["Close"].squeeze()
        vol_d   = df_d["Volume"].squeeze()

        vol_ma5_d  = float(vol_d.rolling(5).mean().iloc[-1])
        vol_ma10_d = float(vol_d.rolling(10).mean().iloc[-1])
        vol_ma20_d = float(vol_d.rolling(20).mean().iloc[-1])

        # 量增確認：5日均量 > 10日均量 > 20日均量
        vol_cross_up = (vol_ma5_d > vol_ma10_d > vol_ma20_d
                        and vol_ma5_d > 0 and vol_ma10_d > 0 and vol_ma20_d > 0)

        # 20MA方向向上
        ma20_series = close_d.rolling(20).mean().dropna()
        if len(ma20_series) < 6:
            return None
        ma20_up = float(ma20_series.iloc[-1]) > float(ma20_series.iloc[-6])

        # 60MA方向向上
        ma60_series = close_d.rolling(60).mean().dropna()
        if len(ma60_series) < 6:
            return None
        ma60_up = float(ma60_series.iloc[-1]) > float(ma60_series.iloc[-6])

        # 均線方向必須向上
        if not (ma20_up and ma60_up):
            return None

        # 建議支撐價位 = max(第一波起漲點, 20週均線)
        support     = round(max(start_price, ma20w_cur), 2)
        vol_ratio_d = round(float(vol_d.iloc[-1]) / vol_ma20_d, 2) if vol_ma20_d > 0 else 0
        current_price = round(float(close_d.iloc[-1]), 2)

        # ── 今日 K線 ──
        open_d_val   = df_d["Open"].squeeze()
        high_d_val   = df_d["High"].squeeze()
        low_d_val    = df_d["Low"].squeeze()
        open_today   = float(open_d_val.iloc[-1])
        prev_close_d = float(close_d.iloc[-2]) if len(close_d) >= 2 else open_today
        daily_chg    = (current_price - prev_close_d) / prev_close_d * 100 if prev_close_d > 0 else 0
        kline_type   = "紅K" if current_price >= open_today else "黑K"

        # ── 昨日 K線 (YT) ──
        if len(close_d) >= 3:
            open_yt       = float(open_d_val.iloc[-2])
            close_yt      = float(close_d.iloc[-2])
            prev_close_yt = float(close_d.iloc[-3])
            yt_chg        = (close_yt - prev_close_yt) / prev_close_yt * 100 if prev_close_yt > 0 else 0
            yt_type       = "紅K" if close_yt >= open_yt else "黑K"
        else:
            yt_type = "—"
            yt_chg  = 0.0

        return {
            "code":           code,
            "name":           name,
            "market":         market,
            "price":          current_price,
            "support":        support,
            "vol_ratio_d":    vol_ratio_d,
            "vol_cross_up":   vol_cross_up,   # True=量增已確認，False=觀察中
            "ma20_up":        ma20_up,
            "ma60_up":        ma60_up,
            "wave1_rise_pct": round(wave1_rise * 100, 1),
            "peak_price":     round(peak_price, 2),
            "start_price":    round(start_price, 2),
            "ma20w":          round(ma20w_cur, 2),
            "kline_type":     kline_type,
            "daily_chg":      round(daily_chg, 2),
            "yt_type":        yt_type,
            "yt_chg":         round(yt_chg, 2),
        }

    except Exception:
        return None


# ─── 報告建立 ───

def _chart_links(code, market="上市"):
    exchange = "TWSE" if market == "上市" else "TPEX"
    tv = f"https://www.tradingview.com/chart/?symbol={exchange}%3A{code}"
    gi = f"https://goodinfo.tw/tw/ShowK_Chart.asp?STOCK_ID={code}"
    s  = "font-size:.78em;padding:1px 5px;border-radius:2px;text-decoration:none;margin:1px;display:inline-block;color:#fff"
    return (f'<a href="{tv}" target="_blank" style="{s};background:#1565c0">TV</a>'
            f'<a href="{gi}" target="_blank" style="{s};background:#2e7d32">K線</a>')


def _build_table(rows_data, empty_msg):
    td  = "padding:6px 10px;border:1px solid #ddd;text-align:center;font-size:.85em"
    tdl = "padding:6px 10px;border:1px solid #ddd;text-align:left;font-size:.85em"
    if not rows_data:
        return f'<tr><td colspan="9" style="{td};color:#888">{empty_msg}</td></tr>'
    rows = ""
    for r in rows_data:
        mkt_label = "[OTC] " if r.get("market") == "上櫃" else ""
        market    = r.get("market", "上市")
        code      = r["code"]
        gi_url    = f"https://goodinfo.tw/tw/ShowK_Chart.asp?STOCK_ID={code}"
        links     = _chart_links(code, market)
        ma20_str  = '<span style="color:#1a7c30;font-weight:bold">↑ 向上</span>' if r["ma20_up"] else '<span style="color:#c0392b">↓ 向下</span>'
        ma60_str  = '<span style="color:#1a7c30;font-weight:bold">↑ 向上</span>' if r["ma60_up"] else '<span style="color:#c0392b">↓ 向下</span>'
        vol_color = "#1a7c30" if r["vol_ratio_d"] >= 2 else ("#d67c00" if r["vol_ratio_d"] >= 1.5 else "#555")
        vol_tag   = r["vol_ratio_d"]

        # K線欄（今日）
        ktype = r.get("kline_type", "—")
        dchg  = r.get("daily_chg", 0.0)
        kline_color = "#c0392b" if ktype == "紅K" else "#1a7c30"
        dchg_sign   = "+" if dchg >= 0 else ""
        kline_str   = f'<span style="color:{kline_color};font-weight:bold">{ktype}</span><br><span style="color:{kline_color};font-size:.8em">{dchg_sign}{dchg}%</span>'

        # YT欄（昨日）
        yt_type = r.get("yt_type", "—")
        yt_chg  = r.get("yt_chg", 0.0)
        if yt_type != "—":
            yt_color = "#c0392b" if yt_type == "紅K" else "#1a7c30"
            yt_sign  = "+" if yt_chg >= 0 else ""
            yt_str   = f'<span style="color:{yt_color};font-weight:bold">{yt_type}</span><br><span style="color:{yt_color};font-size:.8em">{yt_sign}{yt_chg}%</span>'
        else:
            yt_str = "—"

        rows += f"""
<tr>
  <td style="{tdl}">{mkt_label}{r['name']}</td>
  <td style="{td}"><a href="{gi_url}" target="_blank" style="font-weight:bold;color:#003366;text-decoration:none">{code}</a><br>{links}</td>
  <td style="{td};color:#1a5276;font-weight:bold">${r['support']}</td>
  <td style="{td};color:{vol_color};font-weight:bold">{vol_tag}x</td>
  <td style="{td}">{ma20_str}</td>
  <td style="{td}">{ma60_str}</td>
  <td style="{td}">{kline_str}</td>
  <td style="{td}">{yt_str}</td>
  <td style="{td};color:#888;font-size:.8em">+{r['wave1_rise_pct']}%<br>${r['start_price']}→${r['peak_price']}</td>
</tr>"""
    return rows


def _build_html(confirmed, watching, run_date_str, run_time_str):
    th = "padding:7px 10px;border:1px solid #1a5276;background:#1a5276;color:#fff;font-size:.84em"
    thead = f"""<thead><tr>
  <th style="{th}">股票名稱</th><th style="{th}">代號</th>
  <th style="{th}">建議支撐價位</th><th style="{th}">量增倍數</th>
  <th style="{th}">20MA</th><th style="{th}">60MA</th>
  <th style="{th}">K線（今日）</th><th style="{th}">YT（昨日）</th>
  <th style="{th}">第一波參考</th>
</tr></thead>"""

    confirmed_rows = _build_table(confirmed, "本次無量增確認的買點")
    watching_rows  = _build_table(watching,  "本次無觀察中股票")

    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="font-family:Arial,sans-serif;font-size:14px;max-width:960px;margin:0 auto">
<h2 style="color:#1a5276">📈 台股第二波買點分析報告</h2>
<p style="color:#555">執行時間：{run_date_str} {run_time_str}　|　分析對象：目前持倉 + 已出場股票（共 {len(confirmed)+len(watching)} 筆符合周線條件）</p>
<p style="color:#555;font-size:.85em">
  周線條件：①26週內第一波漲&gt;20%(大量) ②回跌量縮(回跌期均量&lt;第一波均量) ③不破20週均線及起漲點<br>
  日線確認：④20MA及60MA均線向上　⑤量增確認：5/10/20日均量交叉向上
</p>

<h3 style="color:#1a7c30;margin-top:18px">✅ 確認買點（量增已確認，建議進場）— {len(confirmed)} 檔</h3>
<table style="border-collapse:collapse;width:100%;margin-top:6px">
{thead}<tbody>{confirmed_rows}</tbody></table>

<h3 style="color:#d67c00;margin-top:22px">👁 觀察中（周線條件+均線向上，等待量增確認）— {len(watching)} 檔</h3>
<table style="border-collapse:collapse;width:100%;margin-top:6px">
{thead}<tbody>{watching_rows}</tbody></table>

<p style="color:#888;font-size:.8em;margin-top:16px">
  ※ 建議支撐價位 = max(第一波起漲低點, 20週均線)。<br>
  ※ 量增倍數 = 今日成交量 / 20日平均量。<br>
  ※ 本報告僅供參考，實際進場請自行評估風險。
</p>
</body></html>"""


# ─── Email 發送 ───

def send_report_email(cfg, confirmed, watching, run_date_str, run_time_str, html_path):
    try:
        sender    = cfg["sender_email"]
        password  = cfg["sender_app_password"]
        receivers = _parse_recipients(cfg.get("recipient_email", ""))

        subject = (f"[台股] 第二波買點 {run_date_str} {run_time_str}"
                   f"（確認{len(confirmed)}檔 / 觀察{len(watching)}檔）")
        body    = _build_html(confirmed, watching, run_date_str, run_time_str)

        msg = MIMEMultipart("mixed")
        msg["Subject"] = Header(subject, "utf-8")
        msg["From"]    = sender
        msg["To"]      = ", ".join(receivers)
        msg.attach(MIMEText(body, "html", "utf-8"))

        # HTML 附件
        with open(html_path, "wb") as f:
            f.write(body.encode("utf-8"))
        with open(html_path, "rb") as f:
            att = MIMEBase("application", "octet-stream")
            att.set_payload(f.read())
        encoders.encode_base64(att)
        att.add_header("Content-Disposition", "attachment",
                       filename=os.path.basename(html_path))
        msg.attach(att)

        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as s:
            s.login(sender, password)
            s.send_message(msg)
        print(f"    Email 已寄出 -> {', '.join(receivers)}")
    except Exception as e:
        print(f"    [警告] Email 寄送失敗: {e}")


# ─── Telegram 發送 ───

def send_telegram(cfg, confirmed, watching, run_date_str, run_time_str, html_path):
    import ssl as _ssl
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

    lines = [f"📈 第二波買點 {run_date_str} {run_time_str}"]
    if confirmed:
        lines.append(f"✅ 確認買點（{len(confirmed)}檔）：")
        for r in confirmed:
            mkt = f"[{r['market']}] " if r.get("market") == "上櫃" else ""
            lines.append(f"  {mkt}{r['code']} {r['name']}  支撐:${r['support']}  量增:{r['vol_ratio_d']}x")
    else:
        lines.append("✅ 確認買點：本次無")
    if watching:
        lines.append(f"👁 觀察中（{len(watching)}檔）：")
        for r in watching[:5]:  # Telegram 最多顯示5筆
            mkt = f"[{r['market']}] " if r.get("market") == "上櫃" else ""
            lines.append(f"  {mkt}{r['code']} {r['name']}  支撐:${r['support']}")
        if len(watching) > 5:
            lines.append(f"  ...另{len(watching)-5}檔，詳見報告")
    text = "\n".join(lines)

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
            data = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode()
            urllib.request.urlopen(
                f"https://api.telegram.org/bot{token}/sendMessage",
                data=data, timeout=15, context=_ctx)

            if html_data is not None:
                boundary = "TGboundary"
                fname    = os.path.basename(html_path)
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

            print(f"    Telegram 已發送 -> chat_id:{chat_id}")
        except Exception as e:
            print(f"    [警告] Telegram 發送失敗 (chat_id:{chat_id}): {e}")


# ─── 主程式 ───

def main():
    now          = datetime.datetime.now()
    run_date_str = now.strftime("%Y/%m/%d")
    run_time_str = now.strftime("%H:%M")
    date_tag     = now.strftime("%Y%m%d")

    print(f"=== 台股第二波買點分析  {run_date_str} {run_time_str} ===")

    watchlist = _load_watchlist()
    if not watchlist:
        print("    [略過] sim_trades.json 中尚無持倉或已出場記錄")
        return

    print(f"    分析股票池：{len(watchlist)} 檔（持倉 + 已出場）")

    results = []
    done    = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = {
            pool.submit(_analyze_second_wave,
                        p["code"], p.get("market", "上市"), p["name"]): p
            for p in watchlist
        }
        for fut in concurrent.futures.as_completed(futs):
            done += 1
            print(f"\r    分析進度: {done}/{len(watchlist)}", end="", flush=True)
            try:
                r = fut.result()
                if r:
                    results.append(r)
            except Exception:
                pass
    print()

    # 分成兩組：量增已確認 vs 觀察中（周線OK但量增尚未觸發）
    confirmed = sorted(
        [r for r in results if r.get("vol_cross_up")],
        key=lambda x: x.get("vol_ratio_d", 0), reverse=True)
    watching  = sorted(
        [r for r in results if not r.get("vol_cross_up")],
        key=lambda x: x.get("wave1_rise_pct", 0), reverse=True)

    print(f"    確認買點（量增已觸發）：{len(confirmed)} 檔")
    for r in confirmed:
        print(f"      {r['code']} {r['name']}  支撐:${r['support']}  量增:{r['vol_ratio_d']}x")
    print(f"    觀察中（等待量增確認）：{len(watching)} 檔")
    for r in watching:
        print(f"      {r['code']} {r['name']}  支撐:${r['support']}")

    html_path = os.path.join(REPORT_DIR, f"second_wave_{date_tag}.html")

    cfg = _load_config()
    if cfg:
        send_report_email(cfg, confirmed, watching, run_date_str, run_time_str, html_path)
        send_telegram(cfg, confirmed, watching, run_date_str, run_time_str, html_path)
    else:
        body = _build_html(confirmed, watching, run_date_str, run_time_str)
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(body)
        print(f"    [提示] 未找到 {EMAIL_CFG}，報告已儲存至 {html_path}")

    print(f"\n[完成] 報告儲存：{html_path}")


if __name__ == "__main__":
    main()
