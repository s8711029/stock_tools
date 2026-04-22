"""
tw_stock_win_analysis.py
分析模擬下單中獲利 >10% 的已出場股票，找出共通進場特徵
每兩週執行一次，結果寄送 Email（含 HTML 附件），並累積歷史資料供後續深度分析
"""

import json
import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from datetime import datetime
from collections import defaultdict

SIM_JSON      = r"C:\Users\s8711\OneDrive\桌面\stock_reports\sim_trades.json"
EMAIL_CFG     = r"C:\Users\s8711\OneDrive\桌面\stock_email_config.json"
REPORT_DIR    = r"C:\Users\s8711\OneDrive\桌面\stock_reports"
HISTORY_JSON  = r"C:\Users\s8711\OneDrive\桌面\stock_reports\win_analysis_history.json"
WIN_THRESHOLD = 10.0

# ── 資料讀取 ──────────────────────────────────────────────────────────────────
def load_data():
    with open(SIM_JSON, encoding="utf-8") as f:
        sim = json.load(f)
    closed = sim.get("closed", [])
    wins = [t for t in closed if t.get("return_pct", 0) > WIN_THRESHOLD]
    return closed, wins

# ── 信號拆解 ─────────────────────────────────────────────────────────────────
def has_signal(sig_str, keyword):
    return keyword in (sig_str or "")

def get_vol_ratio(sig_str):
    for part in (sig_str or "").split(","):
        p = part.strip()
        if "量增" in p:
            try:
                return float(p.replace("量增", "").replace("x", "").strip())
            except:
                return 2.0
    return 0.0

def get_rsi(sig_str):
    for part in (sig_str or "").split(","):
        p = part.strip()
        if "RSI=" in p:
            try:
                return int(p.split("=")[1])
            except:
                pass
    return None

# ── 分析邏輯 ─────────────────────────────────────────────────────────────────
def analyze(wins, closed):
    n = len(wins)
    total = len(closed)
    if n == 0:
        return {}

    score_buckets  = defaultdict(list)
    signal_counts  = defaultdict(int)
    signal_returns = defaultdict(list)
    vol_returns    = []
    no_vol_returns = []
    sector_map     = defaultdict(list)
    slot_map       = defaultdict(list)
    zhicheng_count = 0
    rsi_in_range   = 0

    for t in wins:
        sc  = t.get("entry_score", 0)
        sig = t.get("entry_signals", "")
        ret = t.get("return_pct", 0)
        bkt = f"{(sc // 10)*10}~{(sc // 10)*10+9}"
        score_buckets[bkt].append(ret)

        seen = set()
        for raw in sig.split(","):
            raw = raw.strip()
            key = None
            if "KD黃金交叉" in raw:   key = "KD黃金交叉"
            elif "KD低檔"    in raw:   key = "KD低檔(K<30)"
            elif "MACD柱翻正" in raw:  key = "MACD柱翻正"
            elif "MACD增強"  in raw:   key = "MACD增強"
            elif "均線多頭"  in raw:   key = "均線多頭"
            elif "站上MA20"  in raw:   key = "站上MA20"
            elif "量增"      in raw:   key = "量增≥2x"
            elif "RSI="      in raw:   key = "RSI健康(40~65)"
            elif "⚑"        in raw:   key = "⚑支撐盤整"
            if key and key not in seen:
                signal_counts[key]  += 1
                signal_returns[key].append(ret)
                seen.add(key)

        if "⚑" in sig:
            zhicheng_count += 1
        vol = get_vol_ratio(sig)
        if vol >= 2.0:
            vol_returns.append(ret)
        else:
            no_vol_returns.append(ret)
        rsi = get_rsi(sig)
        if rsi is not None and 40 <= rsi <= 65:
            rsi_in_range += 1

        sector_map[t.get("sector", "未知")].append(ret)
        slot_map[t.get("entry_slot", "?")].append(ret)

    top10 = sorted(wins, key=lambda x: x["return_pct"], reverse=True)[:10]

    avg_ret = sum(t["return_pct"] for t in wins) / n
    max_ret = max(t["return_pct"] for t in wins)
    min_ret = min(t["return_pct"] for t in wins)

    return {
        "n": n, "total": total,
        "avg_ret": avg_ret, "max_ret": max_ret, "min_ret": min_ret,
        "score_buckets":  score_buckets,
        "signal_counts":  signal_counts,
        "signal_returns": signal_returns,
        "vol_count":    len(vol_returns),
        "vol_avg":      sum(vol_returns)    / len(vol_returns)    if vol_returns    else 0,
        "no_vol_avg":   sum(no_vol_returns) / len(no_vol_returns) if no_vol_returns else 0,
        "zhicheng_count": zhicheng_count,
        "rsi_in_range":   rsi_in_range,
        "sector_map":  sector_map,
        "slot_map":    slot_map,
        "top10":       top10,
    }

# ── 歷史累積（每次執行都 append 一筆） ───────────────────────────────────────
def save_history(stats, run_date):
    history = []
    if os.path.exists(HISTORY_JSON):
        try:
            with open(HISTORY_JSON, encoding="utf-8") as f:
                history = json.load(f)
        except:
            history = []

    # 準備本次摘要（可序列化格式）
    record = {
        "run_date":       run_date,
        "n_wins":         stats["n"],
        "n_total":        stats["total"],
        "avg_ret":        round(stats["avg_ret"], 2),
        "max_ret":        round(stats["max_ret"], 2),
        "min_ret":        round(stats["min_ret"], 2),
        "win_rate_pct":   round(stats["n"] / stats["total"] * 100, 1) if stats["total"] else 0,
        "zhicheng_count": stats["zhicheng_count"],
        "vol_2x_count":   stats["vol_count"],
        "vol_2x_avg":     round(stats["vol_avg"], 2),
        "no_vol_avg":     round(stats["no_vol_avg"], 2),
        "rsi_in_range":   stats["rsi_in_range"],
        # 分數分佈：各 bucket 平均
        "score_buckets":  {k: round(sum(v)/len(v), 2) for k, v in stats["score_buckets"].items()},
        # 信號佔比
        "signal_pct":     {k: round(v/stats["n"]*100, 1) for k, v in stats["signal_counts"].items()},
        # 類股前3
        "top3_sectors":   sorted(
            [{"sector": s, "count": len(v), "avg": round(sum(v)/len(v), 2)}
             for s, v in stats["sector_map"].items()],
            key=lambda x: -x["avg"])[:3],
        # 最佳進場時段
        "best_slot":      max(stats["slot_map"].items(),
                              key=lambda x: sum(x[1])/len(x[1]))[0] if stats["slot_map"] else "",
        # top10 摘要
        "top10_summary":  [{"code": t["code"], "name": t["name"],
                            "return_pct": t["return_pct"],
                            "entry_score": t.get("entry_score"),
                            "sector": t.get("sector"),
                            "entry_signals": t.get("entry_signals")}
                           for t in stats["top10"]],
    }

    # 避免同一天重複
    history = [h for h in history if h.get("run_date") != run_date]
    history.append(record)
    history.sort(key=lambda x: x["run_date"])

    os.makedirs(REPORT_DIR, exist_ok=True)
    with open(HISTORY_JSON, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)
    print(f"[OK] History updated: {len(history)} records in {HISTORY_JSON}")
    return history

# ── HTML 生成 ─────────────────────────────────────────────────────────────────
SORT_JS = """
<script>
function sortTable(tbl, col) {
  var rows = Array.from(tbl.querySelectorAll('tbody tr'));
  var asc  = tbl.getAttribute('data-sort-asc') === '1';
  rows.sort(function(a, b) {
    var av = a.cells[col].getAttribute('data-val') || a.cells[col].innerText;
    var bv = b.cells[col].getAttribute('data-val') || b.cells[col].innerText;
    var an = parseFloat(av), bn = parseFloat(bv);
    if (!isNaN(an) && !isNaN(bn)) return asc ? an - bn : bn - an;
    return asc ? av.localeCompare(bv) : bv.localeCompare(av);
  });
  var tb = tbl.querySelector('tbody');
  rows.forEach(function(r){ tb.appendChild(r); });
  tbl.setAttribute('data-sort-asc', asc ? '0' : '1');
}
</script>"""

def _th(label, col, tbl_id):
    s = "padding:8px 12px;background:#1a237e;color:#fff;cursor:pointer;white-space:nowrap"
    return f'<th style="{s}" onclick="sortTable(document.getElementById(\'{tbl_id}\'),{col})">{label} ⇅</th>'

def build_html(closed, wins, stats, history, run_date):
    n     = stats["n"]
    total = stats["total"]
    wr    = n / total * 100 if total else 0

    # 分數表
    score_rows = ""
    for bkt in sorted(stats["score_buckets"].keys()):
        rets = stats["score_buckets"][bkt]
        avg  = sum(rets) / len(rets)
        score_rows += f"<tr><td>{bkt}</td><td>{len(rets)}</td><td style='color:{'green' if avg>0 else 'red'}'>{avg:+.1f}%</td></tr>"

    # 信號表
    sig_rows = ""
    for sig, cnt in sorted(stats["signal_counts"].items(), key=lambda x: -x[1]):
        pct = cnt / n * 100
        avg = sum(stats["signal_returns"][sig]) / cnt if cnt else 0
        sig_rows += f"<tr><td>{sig}</td><td>{cnt}/{n}</td><td>{pct:.0f}%</td><td style='color:{'green' if avg>0 else 'red'}'>{avg:+.1f}%</td></tr>"

    # 類股表
    sector_rows = ""
    for sec, rets in sorted(stats["sector_map"].items(), key=lambda x: -(sum(x[1])/len(x[1]))):
        avg = sum(rets) / len(rets)
        sector_rows += f"<tr><td>{sec}</td><td>{len(rets)}</td><td style='color:green'>{avg:+.1f}%</td></tr>"

    # 時段表
    slot_rows = ""
    for slot in sorted(stats["slot_map"].keys()):
        rets = stats["slot_map"][slot]
        avg  = sum(rets) / len(rets)
        slot_rows += f"<tr><td>{slot}</td><td>{len(rets)}</td><td style='color:{'green' if avg>0 else 'red'}'>{avg:+.1f}%</td></tr>"

    # Top10 表（含出場時間，可排序）
    top10_rows = ""
    for t in stats["top10"]:
        exit_info = f"{t.get('exit_date','')} {t.get('exit_slot','')}".strip()
        top10_rows += f"""<tr>
          <td>{t['code']} {t['name']}</td>
          <td>{t.get('sector','')}</td>
          <td style='color:green;font-weight:bold' data-val='{t["return_pct"]}'>{t['return_pct']:+.2f}%</td>
          <td data-val='{t.get("entry_score",0)}'>{t.get('entry_score','')}</td>
          <td style='font-size:.85em'>{t.get('entry_signals','')}</td>
          <td data-val='{t.get("entry_date","").replace("-","")}'>{t.get('entry_date','')} {t.get('entry_slot','')}</td>
          <td data-val='{t.get("exit_date","").replace("-","")}'>{exit_info}</td>
        </tr>"""

    # 歷史趨勢表（若有 2 筆以上）
    hist_section = ""
    if len(history) >= 2:
        hist_rows = ""
        for h in history[-10:]:
            color = "green" if h["avg_ret"] > 0 else "red"
            hist_rows += f"<tr><td>{h['run_date']}</td><td>{h['n_wins']}/{h['n_total']}</td><td>{h['win_rate_pct']}%</td><td style='color:{color}'>{h['avg_ret']:+.1f}%</td><td>{h['max_ret']:+.1f}%</td><td>{h.get('best_slot','')}</td></tr>"
        hist_section = f"""
<h2>七、歷次分析趨勢（最近 {min(len(history),10)} 次）</h2>
<table><tr><th>分析日期</th><th>大漲/總出場</th><th>大漲率</th><th>平均報酬</th><th>最大</th><th>最佳時段</th></tr>{hist_rows}</table>
<div class='tip'>📊 歷史完整數據儲存於 <code>win_analysis_history.json</code>，可匯入 Excel 或 Python 做深度分析。</div>"""

    html = f"""<!DOCTYPE html>
<html><head><meta charset='utf-8'>
{SORT_JS}
<style>
  body {{ font-family: Arial,sans-serif; font-size:14px; background:#f5f5f5; }}
  .wrap {{ max-width:960px; margin:0 auto; background:white; padding:24px; border-radius:8px; }}
  h1 {{ color:#1a237e; font-size:22px; border-bottom:3px solid #1a237e; padding-bottom:8px; }}
  h2 {{ color:#283593; font-size:16px; margin-top:24px; border-left:4px solid #42a5f5; padding-left:10px; }}
  table {{ border-collapse:collapse; width:100%; margin:10px 0; }}
  th {{ background:#1a237e; color:white; padding:8px 10px; cursor:pointer; }}
  th:hover {{ background:#283593; }}
  td {{ padding:6px 10px; border-bottom:1px solid #e0e0e0; }}
  tr:hover {{ background:#f0f4ff; }}
  .card {{ background:#e8eaf6; border-radius:6px; padding:14px 18px; margin:12px 0; }}
  .card b {{ color:#1a237e; }}
  .badge {{ display:inline-block; background:#1565c0; color:white; border-radius:4px; padding:2px 8px; font-size:12px; margin:2px; }}
  .tip {{ background:#fffde7; border-left:4px solid #f9a825; padding:10px 14px; margin:12px 0; border-radius:4px; }}
  code {{ background:#eee; padding:1px 5px; border-radius:3px; font-size:.9em; }}
  footer {{ font-size:12px; color:#888; margin-top:30px; text-align:center; }}
</style>
</head><body><div class='wrap'>
<h1>台股模擬下單 — 獲利 &gt;{WIN_THRESHOLD}% 股票深度分析</h1>
<p style='color:#666'>分析日期：{run_date} ｜ 樣本：{n} 筆（共 {total} 筆已出場，大漲佔 {wr:.1f}%）</p>

<div class='card'>
平均報酬 <b style='color:green'>+{stats["avg_ret"]:.1f}%</b> ｜
最大 <b style='color:green'>+{stats["max_ret"]:.1f}%</b> ｜
最小 <b style='color:green'>+{stats["min_ret"]:.1f}%</b> ｜
⚑支撐盤整 <b>{stats["zhicheng_count"]}</b> 筆
</div>

<h2>一、進場分數分佈</h2>
<table><tr><th>分數區間</th><th>筆數</th><th>平均報酬</th></tr>{score_rows}</table>
<div class='tip'>💡 <b>甜蜜點：65~79 分</b>。高分（&gt;85）代表市場已充分反映，後續空間有限。</div>

<h2>二、技術信號共通性</h2>
<table><tr><th>訊號</th><th>出現次數</th><th>佔比</th><th>平均報酬</th></tr>{sig_rows}</table>
<div class='tip'>
量增 ≥2x 共 {stats["vol_count"]} 筆，平均 <b style='color:green'>+{stats["vol_avg"]:.1f}%</b> ｜
無量增平均 <b style='color:green'>+{stats["no_vol_avg"]:.1f}%</b><br>
⚑支撐盤整出現 {stats["zhicheng_count"]} 筆 — 凡有此標記，大漲機率顯著提升
</div>

<h2>三、優勢類股族群</h2>
<table><tr><th>類股</th><th>大漲筆數</th><th>平均報酬</th></tr>{sector_rows}</table>

<h2>四、進場時段表現</h2>
<table><tr><th>時段</th><th>筆數</th><th>平均報酬</th></tr>{slot_rows}</table>

<h2>五、更精準進場條件（白話版）</h2>
<div class='card'>
<b>🔴 必備（全部符合）：</b><br>
<span class='badge'>站上 MA20</span>
<span class='badge'>RSI 42~65</span>
<span class='badge'>綜合分 65~79</span>
<span class='badge'>KD黃金交叉 或 MACD柱翻正</span><br><br>
<b>⭐ 強力加分（有這些機率更大）：</b><br>
<span class='badge' style='background:#e65100'>⚑ 支撐整理後突破（選股報告中「⚑整理」欄有標記）</span>
<span class='badge' style='background:#2e7d32'>量增 ≥ 2x</span>
<span class='badge' style='background:#2e7d32'>KD低檔 (K&lt;30)</span><br><br>
<b>🔵 優先類股：</b><br>
<span class='badge' style='background:#37474f'>電機機械</span>
<span class='badge' style='background:#37474f'>其他電子業</span>
<span class='badge' style='background:#37474f'>半導體業</span>
<span class='badge' style='background:#37474f'>電子零組件業</span>
<span class='badge' style='background:#37474f'>綠能環保</span>
<span class='badge' style='background:#37474f'>通信網路（高階品）</span>
</div>
<div class='tip'>
⚠️ <b>避開：</b>分數 &gt;85 ｜ RSI &gt;68 ｜ 觀光餐旅 ｜ 食品業 ｜ 10:00 時段
</div>

<h2>六、獲利前 10 名明細（點欄位標題可排序）</h2>
<table id="top10tbl" data-sort-asc="0">
<thead><tr>
  {_th("股票",0,"top10tbl")}
  {_th("類股",1,"top10tbl")}
  {_th("報酬率",2,"top10tbl")}
  {_th("進場分",3,"top10tbl")}
  {_th("進場信號",4,"top10tbl")}
  {_th("進場時間",5,"top10tbl")}
  {_th("出場時間",6,"top10tbl")}
</tr></thead>
<tbody>{top10_rows}</tbody>
</table>

{hist_section}

<footer>由 tw_stock_win_analysis.py 自動生成 ｜ 每兩週更新 ｜ 資料：sim_trades.json → win_analysis_history.json</footer>
</div></body></html>"""
    return html

# ── Email 發送（含 HTML 附件） ────────────────────────────────────────────────
def send_email(html, html_path, run_date, n_wins, total):
    with open(EMAIL_CFG, encoding="utf-8") as f:
        cfg = json.load(f)

    msg = MIMEMultipart("mixed")
    msg["Subject"] = f"[台股分析] 獲利>10%共通性報告 {run_date}（{n_wins}/{total}筆）"
    msg["From"]    = cfg["sender_email"]
    msg["To"]      = ", ".join(cfg["recipient_email"])

    # 本文（HTML）
    msg.attach(MIMEText(html, "html", "utf-8"))

    # HTML 附件（可下載後在瀏覽器開啟，含完整排序功能）
    with open(html_path, "rb") as f:
        att = MIMEApplication(f.read(), _subtype="octet-stream")
        fname = os.path.basename(html_path)
        att.add_header("Content-Disposition", "attachment",
                       filename=("utf-8", "", fname))
    msg.attach(att)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
        s.login(cfg["sender_email"], cfg["sender_app_password"])
        s.sendmail(cfg["sender_email"], cfg["recipient_email"], msg.as_bytes())
    print(f"[OK] Email sent with attachment: {fname}")

# ── 儲存報告 ──────────────────────────────────────────────────────────────────
def save_report(html, run_date):
    os.makedirs(REPORT_DIR, exist_ok=True)
    fname   = f"win_analysis_{run_date.replace('-','')}.html"
    path    = os.path.join(REPORT_DIR, fname)
    latest  = os.path.join(REPORT_DIR, "win_analysis_latest.html")
    for p in (path, latest):
        with open(p, "w", encoding="utf-8") as f:
            f.write(html)
    print(f"[OK] Report saved: {path}")
    return path

# ── 主程式 ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_date = datetime.now().strftime("%Y-%m-%d")
    print(f"[{run_date}] 開始分析獲利 >{WIN_THRESHOLD}% 出場股票...")

    closed, wins = load_data()
    print(f"  已出場：{len(closed)} 筆 ｜ 獲利>{WIN_THRESHOLD}%：{len(wins)} 筆")

    if len(wins) < 3:
        print("[WARN] 樣本不足 3 筆，跳過分析")
        exit(0)

    stats   = analyze(wins, closed)
    history = save_history(stats, run_date)
    html    = build_html(closed, wins, stats, history, run_date)
    path    = save_report(html, run_date)
    send_email(html, path, run_date, len(wins), len(closed))
    print("[DONE] 分析完成！")
