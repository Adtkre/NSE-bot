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

# ─── CONFIG ───────────────────────────────────────────────────────────────────
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


def get_prev_trading_day():
    day = datetime.now() - timedelta(days=1)
    while day.weekday() in (5, 6):
        day -= timedelta(days=1)
    return day


def make_nse_session():
    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        session.get("https://www.nseindia.com", timeout=15)
        time.sleep(random.uniform(1, 2))
    except Exception:
        pass
    return session


def fetch_bhav_copy(date: datetime):
    date_str   = date.strftime("%Y%m%d")
    date_label = date.strftime("%d %b %Y (%A)")

    urls = [
        f"https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{date_str}_F_0000.csv.zip",
        f"https://www.nseindia.com/content/historical/DERIVATIVES/{date.year}/{date.strftime('%b').upper()}/fo{date.strftime('%d%b%Y').upper()}bhav.csv.zip",
    ]

    session = make_nse_session()

    for url in urls:
        try:
            r = session.get(url, timeout=30)
            if r.status_code == 200 and len(r.content) > 1000:
                with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                    with z.open(z.namelist()[0]) as f:
                        content = f.read().decode("utf-8", errors="replace")
                return content, date_label
        except Exception as e:
            print(f"[bhav] error: {e}")
            continue

    return None, date_label


def fetch_nse_gainers_losers(session):
    """
    Fetch top gainers and losers from NSE live API.
    Returns set of symbols appearing in gainers or losers.
    """
    symbols = set()
    urls = [
        "https://www.nseindia.com/api/live-analysis-variations?index=gainers&limit=50",
        "https://www.nseindia.com/api/live-analysis-variations?index=loosers&limit=50",
    ]
    for url in urls:
        try:
            r = session.get(url, timeout=15)
            if r.status_code == 200:
                data = r.json()
                records = data if isinstance(data, list) else data.get("data", data.get("NIFTY", []))
                if isinstance(records, dict):
                    # sometimes nested under market type keys
                    for v in records.values():
                        if isinstance(v, list):
                            for item in v:
                                sym = item.get("symbol", item.get("Symbol", ""))
                                if sym:
                                    symbols.add(sym.upper())
                else:
                    for item in records:
                        sym = item.get("symbol", item.get("Symbol", ""))
                        if sym:
                            symbols.add(sym.upper())
        except Exception as e:
            print(f"[gainers/losers] error: {e}")
    print(f"[gainers/losers] symbols found: {len(symbols)} → {list(symbols)[:10]}")
    return symbols


def fetch_oi_spurts(session):
    """
    Fetch OI spurts symbols from NSE live API.
    Returns set of symbols in OI spurts.
    """
    symbols = set()
    url = "https://www.nseindia.com/api/live-analysis-oi-spurts-underlyings"
    try:
        r = session.get(url, timeout=15)
        if r.status_code == 200:
            text = r.text.strip()
            if text and text[0] in ('[', '{'):
                data = r.json()
                records = data if isinstance(data, list) else data.get("data", [])
                for item in records:
                    sym = item.get("symbol", item.get("Symbol", ""))
                    if sym:
                        symbols.add(sym.upper())
    except Exception as e:
        print(f"[oi-spurts] error: {e}")
    print(f"[oi-spurts] symbols found: {len(symbols)} → {list(symbols)[:10]}")
    return symbols


def parse_bhav_oi(csv_content, threshold_pct, filter_symbols=None):
    """
    Parse Bhav Copy CSV.
    If filter_symbols is provided, only include those symbols.
    """
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

        # If filter active, skip symbols not in filter
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
    """Returns list of message chunks (each under 1990 chars)."""
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
        candidate = current + line + "\n"
        if len(candidate + footer) > 1900:
            chunks.append(current + footer)
            current = "```\n" + line + "\n"
        else:
            current = candidate
    chunks.append(current + footer)
    return chunks


@bot.command(name="nse")
async def nse_oi(ctx, threshold: float = 2.0):
    """
    !nse <percent>
    Shows stocks that appear in BOTH OI Spurts AND Top Gainers/Losers on NSE,
    filtered by OI change threshold from Bhav Copy.
    Example: !nse 3
    """
    if not 0.1 <= threshold <= 100:
        await ctx.send("❌ Example: `!nse 3` for ±3% OI change filter")
        return

    msg = await ctx.send("⏳ Fetching NSE data...")

    try:
        # Step 1: Get gainers/losers + OI spurts symbols from NSE live
        await msg.edit(content="⏳ Fetching NSE Gainers/Losers & OI Spurts...")
        session = make_nse_session()

        gainers_losers_symbols = fetch_nse_gainers_losers(session)
        oi_spurts_symbols      = fetch_oi_spurts(session)

        # Intersection — only symbols in BOTH
        common_symbols = gainers_losers_symbols & oi_spurts_symbols
        print(f"[filter] Common symbols (OI Spurts ∩ Gainers/Losers): {common_symbols}")

        # Step 2: Download Bhav Copy for OI change data
        await msg.edit(content="⏳ Downloading NSE Bhav Copy...")
        date = get_prev_trading_day()
        csv_content = date_label = None

        for _ in range(7):
            csv_content, date_label = fetch_bhav_copy(date)
            if csv_content:
                break
            date -= timedelta(days=1)
            while date.weekday() in (5, 6):
                date -= timedelta(days=1)

        if not csv_content:
            await msg.edit(content="❌ Couldn't fetch Bhav Copy.")
            return

        # Step 3: Parse with filter
        await msg.edit(content=f"⏳ Parsing **{date_label}** data...")

        # Use common symbols as filter (if we got any from live API)
        filter_set = common_symbols if common_symbols else None

        if not common_symbols:
            await msg.edit(content=(
                f"⚠️ NSE live API returned no common symbols "
                f"(market may be closed). Showing all OI ≥±{threshold}% stocks instead."
            ))
            await ctx.send("⏳ Parsing Bhav Copy without filter...")

        gainers, losers = parse_bhav_oi(csv_content, threshold, filter_symbols=filter_set)

        title = (
            f"📊 **NSE — OI Spurts ∩ Gainers/Losers | ±{threshold}% | {date_label}**\n"
            f"*(Stocks in both OI Spurts & Top Gainers/Losers)*"
        )
        gainer_chunks = fmt(f"OI Gainers (≥+{threshold}%)", gainers, "📈")
        loser_chunks  = fmt(f"OI Losers  (≤-{threshold}%)", losers,  "📉")
        all_chunks    = [title] + gainer_chunks + loser_chunks

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
    msg = await ctx.send("⏳ Fetching for debug...")
    try:
        date = get_prev_trading_day()
        for _ in range(7):
            csv_content, date_label = fetch_bhav_copy(date)
            if csv_content:
                break
            date -= timedelta(days=1)
            while date.weekday() in (5, 6):
                date -= timedelta(days=1)

        rows = [{k.strip(): v.strip() for k, v in row.items()}
                for row in csv.DictReader(io.StringIO(csv_content))]
        inst_types = set(r.get("FinInstrmTp", r.get("Instrument", "?")) for r in rows[:500])
        cols = list(rows[0].keys()) if rows else []

        out = (
            f"**Debug — {date_label}**\n"
            f"```\nTotal rows: {len(rows)}\n"
            f"Columns: {cols}\n"
            f"Instrument types: {sorted(inst_types)}\n"
            f"Sample: {dict(list(rows[0].items())[:6])}\n```"
        )
        await msg.edit(content=out[:1990])
    except Exception as e:
        await msg.edit(content=f"❌ `{e}`")


@bot.event
async def on_ready():
    print(f"✅ {bot.user} online | !nse <percent> | !debug")


bot.run(TOKEN)