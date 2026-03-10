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


def fetch_bhav_copy(date: datetime):
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
    return None, date_label


def get_most_recent_bhav():
    now = datetime.now()
    
    # NSE uploads Bhav Copy after 4:30 PM IST
    # If it's past 4:30 PM on a weekday, try today first
    if now.weekday() not in (5, 6) and now.hour >= 16:
        day = now  # try today
    else:
        day = now - timedelta(days=1)
        while day.weekday() in (5, 6):
            day -= timedelta(days=1)

    for _ in range(7):
        csv_data, date_label = fetch_bhav_copy(day)
        if csv_data:
            return csv_data, date_label
        day -= timedelta(days=1)
        while day.weekday() in (5, 6):
            day -= timedelta(days=1)
    return None, None


def parse_bhav(csv_content, oi_threshold_pct):
    """
    Intersection of OI Spurts AND Gainers/Losers — from Bhav Copy only:

    OI Spurts condition  → |OI change| >= oi_threshold%
    Gainer condition     → Price went UP   (ltp > prev_close)
    Loser  condition     → Price went DOWN (ltp < prev_close)

    Final output:
    - Gainers: OI change >= +threshold% AND price went up
    - Losers:  OI change >= +threshold% AND price went down
               OR OI change <= -threshold% (long unwinding — bearish)
    """
    reader = csv.DictReader(io.StringIO(csv_content))
    rows = [{k.strip(): v.strip() for k, v in row.items()} for row in reader]

    if not rows:
        raise Exception("CSV is empty.")

    symbol_data = {}

    for row in rows:
        inst = row.get("FinInstrmTp", row.get("Instrument", "")).strip()
        is_future = inst in FUTURES_TYPES or "FUT" in inst.upper()
        if not is_future:
            continue

        symbol = row.get("TckrSymb", row.get("Symbol", "")).strip().upper()
        if not symbol:
            continue

        try:
            curr_oi    = float(row.get("OpnIntrst",      row.get("OPEN_INT",   "0")).replace(",", "") or 0)
            oi_chg     = float(row.get("ChngInOpnIntrst", row.get("CHG_IN_OI",  "0")).replace(",", "") or 0)
            ltp        = float(row.get("ClsPric",          row.get("CLOSE",      row.get("SttlmPric",   "0"))).replace(",", "") or 0)
            prev_close = float(row.get("PrvsClsgPric",     row.get("PREV_CLOSE", "0")).replace(",", "") or 0)
        except ValueError:
            continue

        if symbol not in symbol_data:
            symbol_data[symbol] = {"curr_oi": 0.0, "oi_chg": 0.0, "ltp": 0.0, "prev_close": 0.0}

        symbol_data[symbol]["curr_oi"] += curr_oi
        symbol_data[symbol]["oi_chg"]  += oi_chg
        if ltp:        symbol_data[symbol]["ltp"]        = ltp
        if prev_close: symbol_data[symbol]["prev_close"] = prev_close

    gainers = []
    losers  = []

    for sym, d in symbol_data.items():
        prev_oi = d["curr_oi"] - d["oi_chg"]
        if prev_oi <= 0:
            continue

        oi_pct    = (d["oi_chg"] / prev_oi) * 100
        price_pct = 0.0
        if d["prev_close"] > 0:
            price_pct = ((d["ltp"] - d["prev_close"]) / d["prev_close"]) * 100

        # Only stocks with significant OI change (OI Spurts condition)
        if abs(oi_pct) < oi_threshold_pct:
            continue

        row_data = {
            "symbol":    sym,
            "oi_pct":    round(oi_pct, 2),
            "price_pct": round(price_pct, 2),
            "prev_oi":   int(prev_oi),
            "curr_oi":   int(d["curr_oi"]),
            "ltp":       d["ltp"],
        }

        # Gainer = OI buildup + price went UP (long buildup)
        if oi_pct >= oi_threshold_pct and price_pct > 0:
            gainers.append(row_data)

        # Loser = OI buildup + price went DOWN (short buildup)
        #       OR OI unwinding + price went DOWN (long exit)
        elif price_pct < 0:
            losers.append(row_data)

    gainers.sort(key=lambda x: -x["oi_pct"])
    losers.sort(key=lambda x:   x["price_pct"])
    return gainers, losers


def fmt(title, rows, emoji):
    if not rows:
        return [f"**{emoji} {title}**\n*No stocks found*"]

    header = (
        f"**{emoji} {title}** ({len(rows)} stocks)\n"
        f"```\n"
        f"{'Symbol':<14} {'OI Chg%':>8}  {'Price%':>7}  {'Prev OI':>11}  {'Curr OI':>11}  {'LTP':>8}\n"
        f"{'─' * 68}\n"
    )
    footer = "```"
    row_lines = [
        f"{r['symbol']:<14} {r['oi_pct']:>+7.2f}%  {r['price_pct']:>+6.2f}%  "
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
async def nse_oi(ctx, oi_threshold: float = 2.0):
    """
    !nse <percent>
    Shows F&O futures with OI change >= +X% (gainers) or <= -X% (losers).
    Example: !nse 3
    """
    if not 0.1 <= oi_threshold <= 100:
        await ctx.send("❌ Example: `!nse 3` or `!nse 3 1.5`")
        return

    msg = await ctx.send("⏳ Downloading NSE Bhav Copy...")

    try:
        csv_data, date_label = get_most_recent_bhav()
        if not csv_data:
            await msg.edit(content="❌ Could not download Bhav Copy from NSE.")
            return

        await msg.edit(content=f"⏳ Parsing **{date_label}** — OI ≥ ±{oi_threshold}%...")

        gainers, losers = parse_bhav(csv_data, oi_threshold)

        note = f"*(OI Spurts ∩ Gainers/Losers — OI change ≥ ±{oi_threshold}%)*"
        header_msg = f"📊 **NSE F&O | {date_label}**\n{note}\n"

        gainer_chunks = fmt(f"OI Spurts + Gainers (OI≥+{oi_threshold}% & Price↑)", gainers, "📈")
        loser_chunks  = fmt(f"OI Spurts + Losers  (OI≥±{oi_threshold}% & Price↓)", losers,  "📉")
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
    msg = await ctx.send("⏳ Debug...")
    try:
        csv_data, date_label = get_most_recent_bhav()
        rows = [{k.strip(): v.strip() for k, v in row.items()}
                for row in csv.DictReader(io.StringIO(csv_data))]
        inst_types = set(r.get("FinInstrmTp", "?") for r in rows[:500])
        # Sample a futures row
        fut_sample = next((r for r in rows if r.get("FinInstrmTp","") in FUTURES_TYPES), {})
        out = (
            f"**Debug — {date_label}**\n```\n"
            f"Total rows: {len(rows)}\n"
            f"Instrument types: {sorted(inst_types)}\n"
            f"Futures sample: {dict(list(fut_sample.items())[:8])}\n```"
        )
        await msg.edit(content=out[:1990])
    except Exception as e:
        await msg.edit(content=f"❌ `{e}`")


@bot.event
async def on_ready():
    print(f" {bot.user} online | !nse <oi%> [price%] | !debug")


bot.run(TOKEN)