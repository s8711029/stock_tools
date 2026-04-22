#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
台股慣性支撐均線分析工具
- 分析因跌破5MA而賣出的股票
- 回測2年走勢，找出個股慣性支撐均線
- 每三週六上午11:00執行，寄送分析報告
"""

import os, sys, json, datetime, smtplib, ssl
import warnings
import yfinance as yf
import pandas as pd
import ta
import concurrent.futures
from email.mime.multipart import MIMEMultipart
from email.mime.text      import MIMEText
from email.mime.base      import MIMEBase
from email               import encoders
from email.header        import Header

warnings.filterwarnings("ignore")

TOOLS_DIR  = os.path.dirname(os.path.abspath(__file__))
DESKTOP    = os.path.dirname(TOOLS_DIR)
REPORT_DIR = os.path.join(DESKTOP, "stock_reports")
SIM_JSON   = os.path.join(REPORT_DIR, "sim_trades.json")
EMAIL_CFG  = os.path.join(DESKTOP, "stock_email_config.json")

MA_PERIODS   = [5, 10, 20, 60, 120]
TOUCH_PCT    = 0.03   # 低點距均線 <= 3% 視為「觸碰」
BOUNCE_PCT   = 0.02   # 觸碰後 5 日內漲幅 >= 2% 視為「止跌反彈」
BOUNCE_DAYS  = 5      # 觀察反彈的天數
MIN_TOUCHES  = 2      # 最少需有幾次觸碰才納入統計
MAX_WORKERS  = 8


def _tw_ticker(code, market="上市"):
    suffix = ".TW" if market == "上市" else ".TWO"
    return f"{code}{suffix}"


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


def _load_5ma_exits():
    """從 sim_trades.json 取出所有因跌破5MA賣出的股票（去重）"""
    try:
        with open(SIM_JSON, "r", encoding="utf-8") as f:
            sim = json.load(f)
    except Exception:
        return []
    closed = sim.get("closed", [])
    seen = {}
    for p in closed:
        if p.get("exit_reason") == "5MA_break":
            code = p["code"]
            # 同代號出現多次時保留最新的賣出記錄
            if code not in seen or p["exit_date"] > seen[code]["exit_date"]:
                seen[code] = p
    return list(seen.values())


def _analyze_support_ma(code, market, name):
    """回測個股2年走勢，找出慣性支撐均線。回傳分析結果 dict 或 None。"""
    try:
        ticker = yf.Ticker(_tw_ticker(code, market))
        df = ticker.history(period="2y", interval="1d")
        if df is None or len(df) < 60:
            return None

        close  = df["Close"].squeeze()
        low    = df["Low"].squeeze()
        volume = df["Volume"].squeeze()
        vma20  = volume.rolling(20).mean()

        current_price = round(float(close.iloc[-1]), 2)
        results_by_ma = {}

        for period in MA_PERIODS:
            if len(close) < period + BOUNCE_DAYS:
                continue
            ma = close.rolling(period).mean()
            touch_count   = 0
            bounce_count  = 0
            vol_ok_count  = 0

            for i in range(period, len(close) - BOUNCE_DAYS):
                ma_val = float(ma.iloc[i])
                if ma_val <= 0:
                    continue
                low_val = float(low.iloc[i])
                # 觸碰條件：當日低點距均線 <= TOUCH_PCT，且收盤在均線附近（未跌穿太深）
                if low_val <= ma_val * (1 + TOUCH_PCT) and float(close.iloc[i]) >= ma_val * (1 - TOUCH_PCT):
                    touch_count += 1
                    # 量能條件：觸碰日成交量 >= 0.8 倍 20MA
                    vol = float(volume.iloc[i])
                    vma = float(vma20.iloc[i]) if not pd.isna(vma20.iloc[i]) else 0
                    if vma > 0 and vol >= vma * 0.8:
                        vol_ok_count += 1
                    # 反彈條件：後 BOUNCE_DAYS 日內有任一日收盤漲幅 >= BOUNCE_PCT
                    base = float(close.iloc[i])
                    bounced = any(
                        float(close.iloc[i + j]) >= base * (1 + BOUNCE_PCT)
                        for j in range(1, BOUNCE_DAYS + 1)
                    )
                    if bounced:
                        bounce_count += 1

            if touch_count >= MIN_TOUCHES:
                success_rate = round(bounce_count / touch_count * 100, 1)
                results_by_ma[period] = {
                    "touches":      touch_count,
                    "bounces":      bounce_count,
                    "success_rate": success_rate,
                    "vol_ok":       vol_ok_count,
                    "ma_value":     round(float(ma.iloc[-1]), 2),
                }

        if not results_by_ma:
            return {
                "code": code, "name": name, "market": market,
                "current_price": current_price,
                "support_ma": None, "ma_label": "暫無符合之均線",
                "ma_value": None, "success_rate": None,
                "vol_relation": "—", "suggestion": "觀察",
            }

        # 找出 bounce_count 最多的均線；若相同則取更長週期（慣性更顯著）
        best_period = max(
            results_by_ma,
            key=lambda p: (results_by_ma[p]["bounces"], p)
        )
        best = results_by_ma[best_period]

        # 量價關係描述
        vol_ratio_ok = best["vol_ok"] / best["touches"] if best["touches"] else 0
        if vol_ratio_ok >= 0.6:
            vol_relation = "縮量觸底反彈"
        elif best["success_rate"] >= 70:
            vol_relation = "均量支撐有效"
        else:
            vol_relation = "量能不一致"

        # 建議
        if best["success_rate"] >= 75:
            suggestion = "可分批布局"
        elif best["success_rate"] >= 60:
            suggestion = "可觀察"
        else:
            suggestion = "謹慎"

        return {
            "code":          code,
            "name":          name,
            "market":        market,
            "current_price": current_price,
            "support_ma":    best_period,
            "ma_label":      f"{best_period}MA",
            "ma_value":      best["ma_value"],
            "success_rate":  best["success_rate"],
            "touches":       best["touches"],
            "bounces":       best["bounces"],
            "vol_relation":  vol_relation,
            "suggestion":    suggestion,
            "all_ma":        results_by_ma,
        }
    except Exception as e:
        print(f"    [{code}] 分析失敗: {e}")
        return None


def _build_report_html(analysis_list, run_date_str, run_time_str):
    td  = "padding:6px 10px;border:1px solid #ddd;text-align:center;font-size:.85em"
    tdl = "padding:6px 10px;border:1px solid #ddd;text-align:left;font-size:.85em"
    th  = "padding:7px 10px;border:1px solid #1a5276;background:#1a5276;color:#fff;font-size:.84em"

    rows = ""
    for r in analysis_list:
        ma_label = r.get("ma_label", "—")
        ma_val   = r.get("ma_value")
        sr       = r.get("success_rate")
        ma_val_str = f"{ma_val}" if ma_val else "—"
        sr_str     = f"{sr}%" if sr is not None else "—"
        sugg_color = "#1a7c30" if r.get("suggestion") == "可分批布局" else ("#d67c00" if r.get("suggestion") == "可觀察" else "#c0392b")
        mkt = f"[{r['market']}] " if r.get("market") == "上櫃" else ""
        rows += f"""
<tr>
  <td style="{tdl}">{mkt}{r['name']}</td>
  <td style="{td}">{r['code']}</td>
  <td style="{td};font-weight:bold">{r['current_price']}</td>
  <td style="{td};color:#1a5276;font-weight:bold">{ma_label}</td>
  <td style="{td}">{ma_val_str}</td>
  <td style="{td};font-weight:bold">{sr_str}</td>
  <td style="{td}">{r.get('vol_relation','—')}</td>
  <td style="{td};color:{sugg_color};font-weight:bold">{r.get('suggestion','—')}</td>
</tr>"""

    return f"""<html><body style="font-family:Arial,sans-serif;font-size:14px;max-width:900px;margin:0 auto">
<h2 style="color:#1a5276">📊 台股慣性支撐均線分析報告</h2>
<p style="color:#555">分析日期：{run_date_str} {run_time_str}　|　分析對象：曾因跌破5日均線賣出之持股</p>
<p style="color:#555;font-size:.9em">方法：回測近2年走勢，找出個股在5/10/20/60/120MA各均線被支撐、止跌反彈次數最多者，作為「慣性支撐均線」。歷史成功率 = 反彈次數 / 觸碰次數。</p>
<table style="border-collapse:collapse;width:100%;margin-top:10px">
<thead><tr style="background:#1a5276;color:#fff">
  <th style="{th}">股票</th>
  <th style="{th}">代號</th>
  <th style="{th}">現價</th>
  <th style="{th}">支撐均線</th>
  <th style="{th}">均線價位</th>
  <th style="{th}">歷史成功率</th>
  <th style="{th}">量價關係</th>
  <th style="{th}">建議</th>
</tr></thead>
<tbody>{rows}</tbody>
</table>
<p style="color:#888;font-size:.8em;margin-top:16px">
  ※ 觸碰定義：當日低點距均線 ≤3%，且收盤在均線 ±3% 內。<br>
  ※ 反彈定義：觸碰後5個交易日內，收盤漲幅 ≥2%。<br>
  ※ 暫無符合之均線：2年內各均線有效支撐次數皆不足2次或次數相同。
</p>
</body></html>"""


def send_report_email(cfg, analysis_list, run_date_str, run_time_str):
    try:
        sender    = cfg["sender_email"]
        password  = cfg["sender_app_password"]
        receivers = _parse_recipients(cfg.get("recipient_email", "wic0935@gmail.com"))

        subject = f"[台股] 慣性支撐均線分析報告 {run_date_str}"
        body    = _build_report_html(analysis_list, run_date_str, run_time_str)

        msg = MIMEMultipart("mixed")
        msg["Subject"] = Header(subject, "utf-8")
        msg["From"]    = sender
        msg["To"]      = ", ".join(receivers)
        msg.attach(MIMEText(body, "html", "utf-8"))

        # 儲存 HTML 附件
        html_path = os.path.join(REPORT_DIR, f"ma_support_{run_date_str.replace('/','')}.html")
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(body)
        with open(html_path, "rb") as f:
            att = MIMEBase("application", "octet-stream")
            att.set_payload(f.read())
        encoders.encode_base64(att)
        att.add_header("Content-Disposition", "attachment", filename=os.path.basename(html_path))
        msg.attach(att)

        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as s:
            s.login(sender, password)
            s.send_message(msg)
        print(f"    Email 已寄出 -> {', '.join(receivers)}")
        print(f"    HTML 報告儲存於: {html_path}")
    except Exception as e:
        print(f"    [警告] Email 寄送失敗: {e}")


def main():
    now          = datetime.datetime.now()
    run_date_str = now.strftime("%Y/%m/%d")
    run_time_str = now.strftime("%H:%M")

    print(f"=== 台股慣性支撐均線分析  {run_date_str} {run_time_str} ===")

    exits = _load_5ma_exits()
    if not exits:
        print("    [略過] sim_trades.json 中尚無跌破5MA賣出記錄")
        return

    print(f"    找到 {len(exits)} 檔曾因跌破5MA賣出的股票")

    analysis_list = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = {
            pool.submit(_analyze_support_ma, p["code"], p.get("market", "上市"), p["name"]): p
            for p in exits
        }
        done = 0
        for fut in concurrent.futures.as_completed(futs):
            done += 1
            print(f"\r    分析進度: {done}/{len(exits)}", end="", flush=True)
            try:
                r = fut.result()
                if r:
                    analysis_list.append(r)
            except Exception:
                pass
    print()

    # 排序：成功率高優先
    analysis_list.sort(key=lambda x: x.get("success_rate") or 0, reverse=True)

    print(f"    完成 {len(analysis_list)} 檔分析，寄送 Email...")
    cfg = _load_config()
    if cfg:
        send_report_email(cfg, analysis_list, run_date_str, run_time_str)
    else:
        print(f"    [提示] 未找到 {EMAIL_CFG}，僅輸出結果：")
        for r in analysis_list:
            print(f"    {r['code']} {r['name']}  支撐:{r['ma_label']}  成功率:{r.get('success_rate','—')}%  建議:{r.get('suggestion','—')}")

    print(f"\n[完成] 報告資料夾: {REPORT_DIR}")


if __name__ == "__main__":
    main()
