#!/usr/bin/env python3
"""
Finance Monitor - Fetch 10 financial indicators from CNBC via web_fetch.
Writes to SQLite database.
"""

import sqlite3, json, re, os, sys, argparse, pathlib, warnings
from datetime import datetime
from urllib.request import urlopen, Request
from urllib.error import URLError

# Fix Windows console encoding
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# ── Indicators Config ─────────────────────────────────────────────────────────
# CNBC URLs for each indicator
INDICATORS = [
    {"code": "US10YTIP", "name_cn": "美国10年期TIPS", "url": "https://www.cnbc.com/quotes/US10YTIP"},
    {"code": "US10Y",    "name_cn": "美国10年期国债", "url": "https://www.cnbc.com/quotes/US10Y"},
    {"code": "GC",       "name_cn": "黄金 COMEX",     "url": "https://www.cnbc.com/quotes/@GC.1"},
    {"code": "CL",       "name_cn": "WTI 原油",       "url": "https://www.cnbc.com/quotes/@CL.1"},
    {"code": "SPY",      "name_cn": "标普500 ETF",    "url": "https://www.cnbc.com/quotes/SPY"},
    {"code": "SPX",      "name_cn": "标普500指数",    "url": "https://www.cnbc.com/quotes/SPX"},
    {"code": "QQQ",      "name_cn": "纳斯达克100 ETF","url": "https://www.cnbc.com/quotes/QQQ"},
    {"code": "NDX",      "name_cn": "纳斯达克100指数", "url": "https://www.cnbc.com/quotes/NDX"},
    {"code": "DXY",      "name_cn": "美元指数 DXY",   "url": "https://www.cnbc.com/quotes/.DXY"},
    {"code": "VIX",      "name_cn": "恐慌指数 VIX",   "url": "https://www.cnbc.com/quotes/.VIX"},
]

PROTECTED = (
    "/etc/", "/usr/", "/bin/", "/sbin/", "/lib/", "/System/",
    "/Windows/System32/", "/Windows/SysWOW64/",
    "C:\\Windows\\", "C:\\Program Files\\", "C:\\Program Files (x86)\\",
)

def check_path_safe(path_str, name):
    p = pathlib.Path(path_str).resolve()
    for pat in PROTECTED:
        if pat in str(p):
            raise PermissionError(f"Refusing {name}='{path_str}' — system path detected.")

# ── HTML Fetch ────────────────────────────────────────────────────────────────
def fetch_html(url):
    req = Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml",
    })
    with urlopen(req, timeout=15) as resp:
        return resp.read().decode("utf-8", errors="replace")

# ── Parsing ──────────────────────────────────────────────────────────────────
def parse_float(s):
    s = str(s).strip().replace(",", "").replace("%", "").replace("$", "")
    try:
        return float(s)
    except:
        return None

def parse_price_and_change(text):
    """Parse '4,291.90-283.00 (-6.19%)' or '648.57-9.43 (-1.43%)' or '642.91-5.66 (-0.87%)'"""
    # Pattern: price change (changepct)
    m = re.search(r'([\d,]+\.?\d*)\s*([+-]?\d+\.?\d*)\s*\(([%+-]?\d+\.?\d*)%\)', text)
    if m:
        price = parse_float(m.group(1))
        change = parse_float(m.group(2))
        chg_pct = parse_float(m.group(3))
        return price, change, chg_pct
    # Pattern: just price (maybe with %)
    m = re.search(r'^([\d,]+\.?\d+)', text.strip().replace('%', ''))
    if m:
        return parse_float(m.group(1)), None, None
    return None, None, None

def parse_html_content(html, code):
    """Extract price data from raw CNBC HTML."""
    result = {
        "prev_close": None, "current_price": None, "after_hours": None,
        "change_pct": None, "week52_high": None, "dist_from_high_pct": None,
        "week52_low": None, "dist_from_low_pct": None,
    }

    # Plain text version of HTML for regex matching (used by multiple sections)
    all_text = re.sub(r'<[^>]+>', ' ', html)

    # Try JSON embedded in HTML (FinancialQuote JSON-LD blocks)
    # Find each <script type="application/ld+json">, extract top-level
    # JSON objects via depth-counting, check for ticker match, extract price fields
    for script_m in re.finditer(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>', html):
        script_content_start = script_m.end()
        script_end_m = re.search(r'</script>', html[script_content_start:])
        if not script_end_m:
            continue
        script_content_end = script_content_start + script_end_m.start()
        json_str = html[script_content_start:script_content_end]
        # Extract top-level JSON objects using depth-counting
        i = 0
        while i < len(json_str):
            if json_str[i] == '{':
                # Start of a top-level JSON object
                obj_start = i
                depth = 0
                obj_end = obj_start
                for j in range(obj_start, len(json_str)):
                    if json_str[j] == '{':
                        depth += 1
                    elif json_str[j] == '}':
                        depth -= 1
                        if depth == 0:
                            obj_end = j + 1
                            break
                obj = json_str[obj_start:obj_end]
                # Check if this object contains our ticker's tickerSymbol
                # Handle special cases: .DXY, .VIX have dot-prefixed tickerSymbols
                dot_prefix_codes = {'DXY', 'VIX'}
                tp_variants = [re.escape(code)]
                if code in dot_prefix_codes:
                    tp_variants.append('\\.' + re.escape(code))
                tp_variants.append('@' + re.escape(code) + r'\.[0-9]+')
                ticker_found = False
                for tp_pat in tp_variants:
                    if re.search(r'"tickerSymbol"\s*:\s*"' + tp_pat + r'"', obj):
                        ticker_found = True
                        break
                if ticker_found:
                    price_m = re.search(r'"price"\s*:\s*"([0-9.,]+%?)"', obj)
                    if price_m:
                        result["current_price"] = parse_float(price_m.group(1).rstrip('%').replace(',', ''))
                        pcp_m = re.search(r'"priceChangePercent"\s*:\s*"([+-]?[0-9.]+)"', obj)
                        pc_m = re.search(r'"priceChange"\s*:\s*"([+-]?[0-9.-]+)"', obj)
                        if pcp_m: result["change_pct"] = parse_float(pcp_m.group(1))
                        elif pc_m: result["change_pct"] = parse_float(pc_m.group(1))
                        pp_m = re.search(r'"previousPrice"\s*:\s*"([0-9.,]+)"', obj)
                        if pp_m:
                            result["prev_close"] = parse_float(pp_m.group(1).replace(',', ''))
                        else:
                            # Compute prev_close from price and priceChange
                            price_val = result.get("current_price")
                            pc_m2 = re.search(r'"priceChange"\s*:\s*"([+-]?[0-9.]+)"', obj)
                            if price_val is not None and pc_m2:
                                pc_val = parse_float(pc_m2.group(1))
                                if pc_val is not None:
                                    result["prev_close"] = round(price_val - pc_val, 4)
                        # If we have price+pcp, extract 52-week data then return
                        if result["current_price"] is not None and result["change_pct"] is not None:
                            # 52w range from HTML
                            m52 = re.search(r'52 week range\s*([\d,]+\.?\d*)\s*[-–]\s*([\d,]+\.?\d*)', all_text, re.IGNORECASE)
                            if m52:
                                result["week52_low"] = parse_float(m52.group(1))
                                result["week52_high"] = parse_float(m52.group(2))
                            return result
                i = obj_end
            else:
                i += 1

    # Try last price from QuoteStrip (set current_price)
    # Must match class="QuoteStrip-lastPrice" exactly to avoid matching CSS selectors
    # in <style> tags (where "QuoteStrip-lastPrice{...}>" would incorrectly cross into HTML)
    quote_m = re.search(r'class="QuoteStrip-lastPrice"[^>]*>\s*([\d,]+\.?\d*)', html)
    if quote_m:
        result["current_price"] = parse_float(quote_m.group(1))

    # Try after hours separately (does NOT overwrite current_price)
    # Handle newlines/child elements: label is followed by tag containing the value
    ah_m = re.search(r'After Hours:[\s\S]*?>([\d,]+\.?\d*)', html)
    if ah_m:
        result["after_hours"] = parse_float(ah_m.group(1))

    # If no current_price yet, try Last | pattern as fallback
    # IMPORTANT: sanity check — "Last | 7:40 AM EDT" must NOT give us 7.0 as price
    if result["current_price"] is None:
        last_m = re.search(r'Last\s*\|\s*[^0-9]*([\d,]+\.?\d*)', html)
        if last_m:
            val = parse_float(last_m.group(1))
            # Reject absurdly small values (likely time like "7" from "7:40 AM")
            if val is not None and val > 10:
                result["current_price"] = val

    # Try to find price-change pattern anywhere
    # "4,291.90-283.00 (-6.19%)" or "648.57-9.43 (-1.43%)"
    for m in re.finditer(r'([\d,]+\.\d+)\s*([+-][\d,]+\.\d+)\s*\(([%+-]?\d+\.\d+)%\)', all_text):
        price, change, chg_pct = parse_float(m.group(1)), parse_float(m.group(2)), parse_float(m.group(3))
        if price and chg_pct and abs(chg_pct) < 50:  # sanity check
            result["current_price"] = price
            result["change_pct"] = chg_pct
            break

    # Prev Close
    for pat in [r'Prev[iI]?[ ]?[Cc]lose\s*([\d,]+\.?\d*)', r'Previous\s*Close\s*([\d,]+\.?\d*)']:
        m = re.search(pat, all_text)
        if m:
            result["prev_close"] = parse_float(m.group(1))
            break

    # 52w range
    m = re.search(r'52 week range\s*([\d,]+\.?\d*)\s*[-–]\s*([\d,]+\.?\d*)', all_text, re.IGNORECASE)
    if m:
        result["week52_low"] = parse_float(m.group(1))
        result["week52_high"] = parse_float(m.group(2))

    return result

# ── Fetch one indicator ───────────────────────────────────────────────────────
def fetch_cnbc(indicator):
    url = indicator["url"]
    try:
        html = fetch_html(url)
    except Exception as e:
        warnings.warn(f"Failed to fetch {indicator['code']}: {e}", UserWarning)
        return None

    data = parse_html_content(html, indicator["code"])

    if data.get("current_price") is None:
        warnings.warn(f"No price extracted for {indicator['code']}", UserWarning)

    return {
        "code": indicator["code"],
        "name_cn": indicator["name_cn"],
        **data,
        "source": "cnbc",
    }

# ── DB ────────────────────────────────────────────────────────────────────────
def init_db(db_path):
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS indicators (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fetch_date TEXT NOT NULL, fetch_time TEXT NOT NULL,
        name_cn TEXT NOT NULL, code TEXT NOT NULL,
        prev_close REAL, current_price REAL, after_hours REAL,
        change_pct REAL, week52_high REAL, dist_from_high_pct REAL,
        week52_low REAL, dist_from_low_pct REAL,
        source TEXT DEFAULT 'cnbc',
        created_at TEXT DEFAULT (datetime('now', 'localtime'))
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS fetch_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fetch_date TEXT NOT NULL, fetch_time TEXT NOT NULL,
        source TEXT NOT NULL, indicators_count INTEGER,
        created_at TEXT DEFAULT (datetime('now', 'localtime'))
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_code ON indicators(code)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_fetch_time ON indicators(fetch_time)")
    conn.commit()
    return conn

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    args = parse_args()
    db_path = args.db_path
    check_path_safe(db_path, "db-path")
    log_path = args.log_path or str(pathlib.Path(db_path).resolve().parent / "fetch.log")
    check_path_safe(log_path, "log-path")

    now = datetime.now()
    fetch_date = now.strftime("%Y-%m-%d")
    fetch_time = now.strftime("%Y-%m-%d %H:%M")

    all_data = []
    for ind in INDICATORS:
        r = fetch_cnbc(ind)
        if r and r.get("current_price") is not None:
            # Calculate dist from 52w high/low
            if r.get("current_price") and r.get("week52_high"):
                r["dist_from_high_pct"] = round((r["current_price"] - r["week52_high"]) / r["week52_high"] * 100, 4)
            if r.get("current_price") and r.get("week52_low"):
                r["dist_from_low_pct"] = round((r["current_price"] - r["week52_low"]) / r["week52_low"] * 100, 4)
            all_data.append(r)

    if not all_data:
        warnings.warn("All sources failed. No data written.", UserWarning)

    conn = init_db(db_path)
    c = conn.cursor()
    for d in all_data:
        c.execute("""INSERT INTO indicators
            (fetch_date, fetch_time, name_cn, code, prev_close, current_price,
             after_hours, change_pct, week52_high, dist_from_high_pct,
             week52_low, dist_from_low_pct, source)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (fetch_date, fetch_time, d["name_cn"], d["code"],
             d["prev_close"], d["current_price"], d["after_hours"],
             d["change_pct"], d["week52_high"], d["dist_from_high_pct"],
             d["week52_low"], d["dist_from_low_pct"], d["source"]))
    c.execute("INSERT INTO fetch_log (fetch_date,fetch_time,source,indicators_count) VALUES (?,?,?,?)",
        (fetch_date, fetch_time, "cnbc", len(all_data)))
    conn.commit()
    conn.close()

    os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{fetch_time}] Fetched {len(all_data)} indicators -> {db_path}\n")

    print(f"\n✅ Data fetched | {fetch_time}")
    print(f"📂 DB: {db_path}\n")
    print(f"{'Indicator':<18} {'Code':<10} {'Price':>12} {'Change':>8}")
    print("-" * 52)
    for d in all_data:
        curr = d.get("current_price")
        curr_str = f"{curr:.4f}" if curr is not None and curr < 100 else f"{curr:.2f}" if curr is not None else "N/A"
        chg = d.get("change_pct")
        chg_str = f"{chg:+.2f}%" if chg is not None else "N/A"
        print(f"{d['name_cn']:<18} {d['code']:<10} {curr_str:>12} {chg_str:>8}")

def parse_args():
    p = argparse.ArgumentParser(description="Finance Monitor — fetch 10 indicators from CNBC")
    p.add_argument("--db-path", required=True)
    p.add_argument("--log-path", default=None)
    return p.parse_args()

if __name__ == "__main__":
    main()
