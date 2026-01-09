from __future__ import annotations

import argparse
import asyncio
import hashlib
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

import pandas as pd
from playwright.async_api import async_playwright

# Optionnel (lat/lng)
try:
    from geopy.geocoders import Nominatim
except Exception:
    Nominatim = None


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


GOOGLE_FILE_RE = re.compile(r"^google.*\.xlsx$", re.IGNORECASE)


# ============================================================
# Excel loader
# ============================================================
def normalize_category(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return "Unknown"
    return s


def build_dedupe_key(df: pd.DataFrame, dedupe_key_cols: list[str]) -> pd.Series:
    """Build a stable dedupe key across the concatenated columns (normalisées)."""
    parts = []
    for c in dedupe_key_cols:
        if c not in df.columns:
            parts.append("")
        else:
            parts.append(df[c].astype(str).fillna("").str.strip().str.lower())
    combined = parts[0]
    for p in parts[1:]:
        combined = combined + " | " + p

    return combined.apply(lambda x: hashlib.md5(x.encode("utf-8")).hexdigest())


def load_all_google_excels(
    root_folder: str,
    dedupe_key_cols: Optional[list[str]] = None,
) -> pd.DataFrame:
    root = Path(root_folder)
    if not root.exists():
        raise FileNotFoundError(f"Root folder not found: {root_folder}")

    def file_index(p: Path) -> int:
        m = re.search(r"(\d+)", p.stem)
        return int(m.group(1)) if m else 0

    default_candidates = [
        "name",
        "address",
        "title",
        "lieu",
        "adresse",
    ]
    dedupe_key_cols = dedupe_key_cols or default_candidates

    all_dfs: list[pd.DataFrame] = []

    for sub in sorted([p for p in root.iterdir() if p.is_dir()]):
        files = [f for f in sub.glob("*.xlsx") if GOOGLE_FILE_RE.match(f.name)]
        if not files:
            continue

        files.sort(key=file_index)
        category = normalize_category(sub.name)

        for f in files:
            df = pd.read_excel(f, engine="openpyxl")
            df["category"] = category
            df["__source_file"] = f.name
            df["__source_folder"] = sub.name
            all_dfs.append(df)

    if not all_dfs:
        return pd.DataFrame()

    df = pd.concat(all_dfs, ignore_index=True)

    # Basic dedupe
    df["__dedupe_key"] = build_dedupe_key(df, dedupe_key_cols)
    before = len(df)
    df = df.drop_duplicates(subset=["__dedupe_key"]).drop(columns=["__dedupe_key"])
    after = len(df)
    if after != before:
        logging.info("🧹 Dedupe: %s -> %s rows", before, after)

    return df


# ============================================================
# Geocode (optional)
# ============================================================
def add_lat_lng(df: pd.DataFrame, address_col: str, cache_path: str) -> pd.DataFrame:
    if Nominatim is None:
        logging.warning("⚠️ geopy not available, skipping geocode")
        df["latitude"] = None
        df["longitude"] = None
        return df

    cache_file = Path(cache_path)
    cache_file.parent.mkdir(parents=True, exist_ok=True)

    cache = {}
    if cache_file.exists():
        try:
            cdf = pd.read_csv(cache_file)
            for _, r in cdf.iterrows():
                cache[str(r["address"])] = (r["latitude"], r["longitude"])
        except Exception as e:
            logging.warning("⚠️ Failed reading geocode cache: %s", e)

    geolocator = Nominatim(user_agent="event_geocoder")
    lats = []
    lngs = []

    for _, row in df.iterrows():
        addr = str(row.get(address_col, "") or "").strip()
        if not addr:
            lats.append(None)
            lngs.append(None)
            continue

        if addr in cache:
            lat, lng = cache[addr]
            lats.append(lat)
            lngs.append(lng)
            continue

        try:
            loc = geolocator.geocode(addr, timeout=10)
            if loc is None:
                lat, lng = None, None
            else:
                lat, lng = float(loc.latitude), float(loc.longitude)
            cache[addr] = (lat, lng)
            lats.append(lat)
            lngs.append(lng)
        except Exception:
            lats.append(None)
            lngs.append(None)

    df["latitude"] = lats
    df["longitude"] = lngs

    # Save cache
    try:
        cdf = pd.DataFrame(
            [{"address": k, "latitude": v[0], "longitude": v[1]} for k, v in cache.items()]
        )
        cdf.to_csv(cache_file, index=False)
    except Exception as e:
        logging.warning("⚠️ Failed saving geocode cache: %s", e)

    return df


# ============================================================
# YouTube
# ============================================================
YOUTUBE_SHORT_RE = re.compile(r"https?://(?:www\.)?youtube\.com/shorts/[A-Za-z0-9_\-]+", re.IGNORECASE)

def build_youtube_query(place_name: str, address: str) -> str:
    place_name = (place_name or "").strip()
    address = (address or "").strip()
    q = " ".join([place_name, address]).strip()
    q = re.sub(r"\s+", " ", q)
    return q


async def add_youtube_shorts_to_df(
    df: pd.DataFrame,
    name_col: str,
    address_col: str,
    headless: bool = True,
) -> pd.DataFrame:
    if "youtube_query" not in df.columns:
        df["youtube_query"] = None
    if "youtube_short" not in df.columns:
        df["youtube_short"] = None

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context()
        page = await context.new_page()

        for i, row in df.iterrows():
            place = str(row.get(name_col, "") or "").strip()
            addr = str(row.get(address_col, "") or "").strip()
            if not place and not addr:
                continue

            query = build_youtube_query(place, addr)
            df.at[i, "youtube_query"] = query

            url = "https://www.youtube.com/results?search_query=" + quote_plus(query) + "&sp=EgZzaG9ydHMQAQ%253D%253D"
            try:
                await page.goto(url, timeout=60000)
                await page.wait_for_timeout(1500)

                content = await page.content()
                m = YOUTUBE_SHORT_RE.search(content)
                if m:
                    df.at[i, "youtube_short"] = m.group(0)
                else:
                    df.at[i, "youtube_short"] = None
            except Exception as e:
                logging.warning("⚠️ [youtube] failed for %r: %s", query, e)
                df.at[i, "youtube_short"] = None

        await context.close()
        await browser.close()

    return df


# ============================================================
# Instagram (best effort: DuckDuckGo HTML search, no login)
# ============================================================

INSTAGRAM_RE = re.compile(
    r"https?://(?:www\.)?instagram\.com/(?:reel|reels)/[A-Za-z0-9_\-]+/?",
    re.IGNORECASE,
)

def _duckduckgo_html_search(query: str, timeout: int = 20) -> str:
    """Return raw HTML from DuckDuckGo 'html' endpoint."""
    url = "https://duckduckgo.com/html/?q=" + quote_plus(query)
    req = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        },
    )
    with urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def find_instagram_reel_link(query: str) -> Optional[str]:
    """Try to find an Instagram reel link for a given query (best effort)."""
    try:
        html = _duckduckgo_html_search(f"site:instagram.com/reel {query}")
    except (HTTPError, URLError, TimeoutError, Exception) as e:
        logging.warning("⚠️ [instagram] DuckDuckGo search failed for %r: %s", query, e)
        return None

    m = INSTAGRAM_RE.search(html)
    if not m:
        try:
            html = _duckduckgo_html_search(f"site:instagram.com/reels {query}")
        except Exception:
            return None
        m = INSTAGRAM_RE.search(html)

    if not m:
        return None

    link = m.group(0)
    if link.startswith("http://"):
        link = "https://" + link[len("http://"):]
    return link


def add_instagram_reels_to_df(df: pd.DataFrame, name_col: str, address_col: str) -> pd.DataFrame:
    """Add instagram_query + instagram_reel columns (no Playwright)."""
    if "instagram_query" not in df.columns:
        df["instagram_query"] = None
    if "instagram_reel" not in df.columns:
        df["instagram_reel"] = None

    for i, row in df.iterrows():
        place = str(row.get(name_col, "")).strip()
        addr = str(row.get(address_col, "")).strip()
        if not place and not addr:
            continue

        q = f"{place} {addr}".strip()
        df.at[i, "instagram_query"] = q
        df.at[i, "instagram_reel"] = find_instagram_reel_link(q)

    return df


# ============================================================
# TikTok
# ============================================================
TIKTOK_RE = re.compile(r"https?://(?:www\.)?tiktok\.com/@[^/]+/video/\d+", re.IGNORECASE)

def build_tiktok_query(place_name: str, address: str) -> str:
    place_name = (place_name or "").strip()
    address = (address or "").strip()
    q = " ".join([place_name, address]).strip()
    q = re.sub(r"\s+", " ", q)
    return q


async def add_tiktok_videos_to_df(
    df: pd.DataFrame,
    name_col: str,
    address_col: str,
    headless: bool = True,
    tt_state_path: str = "tt_state.json",
) -> pd.DataFrame:
    if "tiktok_query" not in df.columns:
        df["tiktok_query"] = None
    if "tiktok_video" not in df.columns:
        df["tiktok_video"] = None

    state_path = Path(tt_state_path)
    storage_state = state_path if state_path.exists() else None

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(storage_state=str(storage_state) if storage_state else None)
        page = await context.new_page()

        for i, row in df.iterrows():
            place = str(row.get(name_col, "") or "").strip()
            addr = str(row.get(address_col, "") or "").strip()
            if not place and not addr:
                continue

            query = build_tiktok_query(place, addr)
            df.at[i, "tiktok_query"] = query

            # TikTok search through Google (more stable than in-site search)
            url = "https://www.google.com/search?q=" + quote_plus(f"site:tiktok.com {query}")
            try:
                await page.goto(url, timeout=60000)
                await page.wait_for_timeout(1200)

                content = await page.content()
                m = TIKTOK_RE.search(content)
                if m:
                    df.at[i, "tiktok_video"] = m.group(0)
                else:
                    df.at[i, "tiktok_video"] = None
            except Exception as e:
                logging.warning("⚠️ [tiktok] failed for %r: %s", query, e)
                df.at[i, "tiktok_video"] = None

        await context.close()
        await browser.close()

    return df


# ============================================================
# Pipeline
# ============================================================
async def run_pipeline(
    root_folder: str,
    name_col: str,
    address_col: str,
    do_geocode: bool = True,
    geocode_cache: str = "geocode_cache.csv",
    tt_state_path: str = "tt_state.json",
    headless: bool = True,
    do_youtube: bool = True,
    do_tiktok: bool = True,
    do_instagram: bool = False,
    debug_limit_rows: int | None = None,   # ✅ debug
) -> pd.DataFrame:
    df = load_all_google_excels(root_folder=root_folder)

    # ✅ debug limit AVANT geocode + playwright
    if debug_limit_rows is not None:
        df = df.head(int(debug_limit_rows)).copy()
        logging.warning(
            "🧪 [debug] Limitation DataFrame à %s lignes (debug_limit_rows=%s)",
            len(df),
            debug_limit_rows,
        )

    # Normalize minimal columns
    if "title" not in df.columns and name_col in df.columns:
        df["title"] = df[name_col]
    if "address" not in df.columns and address_col in df.columns:
        df["address"] = df[address_col]

    if do_geocode:
        df = add_lat_lng(df, address_col=address_col, cache_path=geocode_cache)

    # Ensure output columns exist even if some enrichers are disabled
    for c in ["youtube_query", "youtube_short", "tiktok_query", "tiktok_video", "instagram_query", "instagram_reel"]:
        if c not in df.columns:
            df[c] = None

    if do_youtube:
        df = await add_youtube_shorts_to_df(df, name_col=name_col, address_col=address_col, headless=headless)
    else:
        logging.info("⏭️ [youtube] Skipped (do_youtube=False)")

    if do_tiktok:
        df = await add_tiktok_videos_to_df(
            df,
            name_col=name_col,
            address_col=address_col,
            headless=headless,
            tt_state_path=tt_state_path,
        )
    else:
        logging.info("⏭️ [tiktok] Skipped (do_tiktok=False)")

    if do_instagram:
        df = add_instagram_reels_to_df(df, name_col=name_col, address_col=address_col)
    else:
        logging.info("⏭️ [instagram] Skipped (do_instagram=False)")

    return df


def main():
    parser = argparse.ArgumentParser(
        description="Enrich Google excels with YouTube Shorts, TikTok, and (optionally) Instagram Reels."
    )
    parser.add_argument("--root", default=".", help="Root folder containing subfolders with google*.xlsx")
    parser.add_argument("--name-col", required=True, help="Column name containing the place name")
    parser.add_argument("--address-col", required=True, help="Column name containing the address")
    parser.add_argument("--headless", action="store_true", help="Run browsers headless")

    # defaults: YouTube + TikTok enabled, Instagram disabled
    parser.add_argument("--no-youtube", action="store_true", help="Disable YouTube Shorts enrichment")
    parser.add_argument("--no-tiktok", action="store_true", help="Disable TikTok enrichment")
    parser.add_argument("--instagram", action="store_true", help="Enable Instagram Reel enrichment (best effort, no login)")

    parser.add_argument("--geocode", action="store_true", help="Also compute latitude/longitude (requires geopy)")
    parser.add_argument("--geocode-cache", default="geocode_cache.csv", help="CSV cache file for geocoding")
    parser.add_argument("--tt-state", default="tt_state.json", help="TikTok storage_state file")
    parser.add_argument("--debug-limit-rows", type=int, default=None, help="Limit rows before enrichment")
    parser.add_argument("--out", default="enriched.csv", help="Output CSV path")
    args = parser.parse_args()

    df = asyncio.run(
        run_pipeline(
            root_folder=args.root,
            name_col=args.name_col,
            address_col=args.address_col,
            do_geocode=args.geocode,
            geocode_cache=args.geocode_cache,
            tt_state_path=args.tt_state,
            headless=args.headless,
            debug_limit_rows=args.debug_limit_rows,
            do_youtube=(not args.no_youtube),
            do_tiktok=(not args.no_tiktok),
            do_instagram=args.instagram,
        )
    )
    df.to_csv(args.out, index=False)
    print(f"[done] Saved: {args.out}")


if __name__ == "__main__":
    main()
