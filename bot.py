import discord
from discord.ext import commands
import requests
import zipfile
import io
import csv
import time
import random
from datetime import datetime, timedelta
import os

TOKEN = os.environ.get("TOKEN")
PREFIX = "!"

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix=PREFIX, intents=intents)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-IN,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
}

FUTURES_TYPES = {"STF", "IDF"}


def make_nse_session():
    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        session.get("https://www.nseindia.com", timeout=15)
        time.sleep(random.uniform(1.5, 2.5))
        session.get("https://www.nseindia.com/market-data/oi-spurts", timeout=15)
        time.sleep(random.uniform(1, 2))
    except Exception:
        pass
    return session


def fetch_bhav_copy(date: datetime):
    """Try to download Bhav Copy for given date. Returns (csv_content, date_label) or (None, date_label)."""
    date_str   = date.strftime("%Y%m%d")
    date_label = date.strftime("%d %b %Y (%A)")

    urls = [
        f"https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{date_str}_F_0000.csv.zip",
        f"https://www.nseindia.com/content/historical/DERIVATIVES/{date.year}/{date.strftime('%b').upper()}/fo{date.strftime('%d%b%Y').upper()}bhav.csv.zip",
    ]

    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        session.get("https://www.nseindia.com", timeout=15)
        time.sleep(1)
    except Exception:
        pass

    for url in urls:
        try:
            r = session.get(url, timeout=30)
            print(f"[bhav] {url} → {r.status_code} size={len(r.content)}")
            if r.status_code == 200 and len(r.content) > 1000:
                with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                    with z.open(z.namelist()[0]) as f:
                        csv_data = f.read().decode("utf-8", errors="replace")
                print(f"[bhav] ✅ Got data for {date_label}")
                return csv_data, date_label
        except Exception as e:
            print(f"[bhav] error: {e}")
            continue

    print(f"[bhav] ❌ No data for {date_label}")
    return None, date_label


def get_most_recent_bhav():
    """
    Find the most recent available Bhav Copy.
    Start from yesterday, go back up to 7 trading days.
    """
    day = datetime.now() - timedelta(days=1)
    # Skip weekends
    while day.weekday() in (5, 6):
        day -= timedelta(days=1)

    for _ in range(7):
        csv_data, date_label = fetch_bhav_copy(day)
        if csv_data:
            return csv_data, date_label, day
        # Go back one more trading day
        day -= timedelta(days=1)
        while day.weekday() in (5, 6):
            day -= timedelta(days=1)

    return None, None, None


def fetch_oi_spurts_symbols(session):
    """Fetch symbols from NSE OI Spurts page."""
    symbols = set()
    url = "https://www.nseindia.com/api/live-analysis-oi-spurts-underlyings"
    try:
        r = session.get(url, timeout=15)
        print(f"[oi-spurts] status={r.status_code} len={len(r.text)}")
        if r.status_code == 200:
            text = r.text.strip()
            if text and text[0] in ('[', '{'):
                data = r.json()
                records = data if isinstance(data, list) else data.get("data", [])
                for item in records:
                    sym = item.get("symbol", item.get("Symbol", "")).strip().upper()
                    if sym:
                        symbols.add(sym)
    except Exception as e:
        print(f"[oi-spurts] error: {e}")
    print(f"[oi-spurts] found {len(symbols)} symbols: {list(symbols)[:10]}")
    return symbols


def fetch_gainers_losers_symbols(session):
    """Fetch symbols from NSE Top Gainers & Losers."""
    symbols = set()
    urls = [
        "https://www.nseindia.com/api/live-analysis-variations?index=gainers&limit=50",
        "https://www.nseindia.com/api/live-analysis-variations?index=loosers&limit=50",
    ]
    for url in urls:
        try:
            r = session.get(url, timeout=15)
            print(f"[gainers/losers] {url} → {r.status_code}")
            if r.status_code == 200:
                text = r.text.strip()
                if not text or text[0] not in ('[', '{'):
                    continue
                data = r.json()
                # NSE returns nested dict with keys like NIFTY, BANKNIFTY etc
                if isinstance(data, dict):
                    for key, val in data.items():
                        if isinstance(val, list):
                            for item in val:
                                sym = item.get("symbol", item.get("Symbol", "")).strip().upper()
                                if sym:
                                    symbols.add(sym)
                elif isinstance(data, list):
                    for item in data:
                        sym = item.get("symbol", item.get("Symbol", "")).strip().upper()
                        if sym:
                            symbols.add(sym)
        except Exception as e:
            print(f"[gainers/losers] error: {e}")
    print(f"[gainers/losers] found {len(symbols)} symbols: {list(symbols)[:10]}")
    return symbols


def parse_bhav_oi(csv_content, threshold_pct, filter_symbols=None):
    reader = csv.DictReader(io.StringIO(csv_content))
    rows = [{k.strip(): v.strip() for k, v in row.items()} for row in reader]

    if not rows:
        raise Exception("CSV is empty.")

    symbol_data = {}
    for row in rows:
        inst = row.get("FinInstrmTp", row.get("Instrument", row.get("INSTRUMENT", ""))).strip()
        is_future = inst in FUTURES_TYPES or "FUT" in inst.upper()
        if not is_future:
            continue

        symbol = row.get("TckrSymb", row.get("Symbol", row.get("SYMBOL", ""))).strip().upper()
        if not symbol:
            continue

        if filter_symbols is not None and symbol not in filter_symbols:
            continue

        try:
            curr_oi = float(row.get("OpnIntrst",       row.get("OPEN_INT", "0")).replace(",", "") or 0)
            oi_chg  = float(row.get("ChngInOpnIntrst", row.get("CHG_IN_OI", "0")).replace(",", "") or 0)
            ltp     = float(row.get("ClsPric",          row.get("CLOSE", row.get("SttlmPric", "0"))).replace(",", "") or 0)
        except ValueError:
            continue

        if symbol not in symbol_data:
            symbol_data[symbol] = {"curr_oi": 0.0, "oi_chg": 0.0, "ltp": 0.0}
        symbol_data[symbol]["curr_oi"] += curr_oi
        symbol_data[symbol]["oi_chg"]  += oi_chg
        if ltp:
            symbol_data[symbol]["ltp"] = ltp

    print(f"[parse] symbols after filter: {len(symbol_data)}")

    results = []
    for sym, d in symbol_data.items():
        prev_oi = d["curr_oi"] - d["oi_chg"]
        if prev_oi <= 0:
            continue
        oi_pct = (d["oi_chg"] / prev_oi) * 100
        results.append({
            "symbol":  sym,
            "oi_pct":  round(oi_pct, 2),
            "prev_oi": int(prev_oi),
            "curr_oi": int(d["curr_oi"]),
            "ltp":     d["ltp"],
        })

    gainers = sorted([r for r in results if r["oi_pct"] >=  threshold_pct], key=lambda x: -x["oi_pct"])
    losers  = sorted([r for r in results if r["oi_pct"] <= -threshold_pct], key=lambda x:  x["oi_pct"])
    return gainers, losers


def fmt(title, rows, emoji):
    if not rows:
        return [f"**{emoji} {title}**\n*No stocks found*"]
    header = (
        f"**{emoji} {title}** ({len(rows)} stocks)\n"
        f"```\n"
        f"{'Symbol':<14} {'OI Chg%':>8}  {'Prev OI':>11}  {'Curr OI':>11}  {'LTP':>8}\n"
        f"{'─' * 60}\n"
    )
    footer = "```"
    row_lines = [
        f"{r['symbol']:<14} {r['oi_pct']:>+7.2f}%  "
        f"{r['prev_oi']:>11,}  {r['curr_oi']:>11,}  {r['ltp']:>8.2f}"
        for r in rows
    ]
    chunks = []
    current = header
    for line in row_lines:
        if len(current + line + "\n" + footer) > 1900:
            chunks.append(current + footer)
            current = "```\n" + line + "\n"
        else:
            current += line + "\n"
    chunks.append(current + footer)
    return chunks


@bot.command(name="nse")
async def nse_oi(ctx, threshold: float = 2.0):
    """
    !nse <percent>
    Shows stocks present in BOTH NSE OI Spurts AND Top Gainers/Losers,
    with OI change >= ±threshold% from Bhav Copy EOD data.
    """
    if not 0.1 <= threshold <= 100:
        await ctx.send("❌ Example: `!nse 3`")
        return

    msg = await ctx.send("⏳ Step 1/3 — Fetching NSE OI Spurts & Gainers/Losers...")

    try:
        # ── Step 1: Get live NSE symbols ──────────────────────────────────────
        session = make_nse_session()
        oi_spurts_syms      = fetch_oi_spurts_symbols(session)
        gainers_losers_syms = fetch_gainers_losers_symbols(session)

        common = oi_spurts_syms & gainers_losers_syms
        print(f"[filter] OI Spurts={len(oi_spurts_syms)}, G/L={len(gainers_losers_syms)}, Common={len(common)} → {common}")

        # ── Step 2: Download Bhav Copy ────────────────────────────────────────
        await msg.edit(content="⏳ Step 2/3 — Downloading NSE Bhav Copy...")
        csv_data, date_label, _ = get_most_recent_bhav()

        if not csv_data:
            await msg.edit(content="❌ Could not download Bhav Copy from NSE.")
            return

        # ── Step 3: Parse ─────────────────────────────────────────────────────
        await msg.edit(content=f"⏳ Step 3/3 — Parsing **{date_label}** data...")

        if not common:
            # Market closed or NSE API blocked — show all with threshold
            note = (
                f"⚠️ NSE live API returned no data (market may be closed).\n"
                f"Showing **all** stocks with OI change ≥ ±{threshold}% from Bhav Copy.\n\n"
            )
            filter_set = None
        else:
            note = f"*(Stocks in both OI Spurts & Gainers/Losers — {len(common)} matched)*\n\n"
            filter_set = common

        gainers, losers = parse_bhav_oi(csv_data, threshold, filter_symbols=filter_set)

        header_msg = f"📊 **NSE F&O | ±{threshold}% OI Change | {date_label}**\n{note}"
        gainer_chunks = fmt(f"OI Gainers (≥+{threshold}%)", gainers, "📈")
        loser_chunks  = fmt(f"OI Losers  (≤-{threshold}%)", losers,  "📉")
        all_chunks    = [header_msg] + gainer_chunks + loser_chunks

        first = True
        for chunk in all_chunks:
            if not chunk.strip():
                continue
            if first:
                await msg.edit(content=chunk)
                first = False
            else:
                await ctx.send(chunk)

    except Exception as e:
        await msg.edit(content=f"❌ **Error:** `{e}`")


@bot.command(name="debug")
async def debug_csv(ctx):
    msg = await ctx.send("⏳ Fetching debug info...")
    try:
        csv_data, date_label, _ = get_most_recent_bhav()
        rows = [{k.strip(): v.strip() for k, v in row.items()}
                for row in csv.DictReader(io.StringIO(csv_data))]
        inst_types = set(r.get("FinInstrmTp", "?") for r in rows[:500])
        out = (
            f"**Debug — {date_label}**\n```\n"
            f"Total rows: {len(rows)}\n"
            f"Instrument types: {sorted(inst_types)}\n"
            f"Columns: {list(rows[0].keys()) if rows else []}\n```"
        )
        await msg.edit(content=out[:1990])
    except Exception as e:
        await msg.edit(content=f"❌ `{e}`")


@bot.event
async def on_ready():
    print(f"✅ {bot.user} online | !nse <percent> | !debug")


bot.run(TOKEN)