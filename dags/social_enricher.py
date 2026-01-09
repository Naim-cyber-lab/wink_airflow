from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.parse import quote_plus
from urllib.request import Request, urlopen

import pandas as pd
from playwright.async_api import async_playwright

# Optionnel (lat/lng)
try:
    from geopy.geocoders import Nominatim
    from geopy.extra.rate_limiter import RateLimiter
except Exception:
    Nominatim = None
    RateLimiter = None


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ============================================================
# Utils
# ============================================================

GOOGLE_FILE_RE = re.compile(r"^google(?: \(\d+\))?\.xlsx$", re.IGNORECASE)


def safe_str(x) -> str:
    """Retourne une string clean, ou '' si NaN/None."""
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x).strip()


def file_index(path: Path) -> int:
    """google.xlsx -> 0, google (1).xlsx -> 1, etc."""
    if path.stem.lower() == "google":
        return 0
    m = re.match(r"^google \((\d+)\)$", path.stem.lower())
    return int(m.group(1)) if m else 10**9


def normalize_category(folder_name: str) -> str:
    """bar-paris -> bar paris"""
    return folder_name.replace("-", " ").strip()


def compute_row_id(row: pd.Series, key_cols: list[str]) -> str:
    """ID stable: hash sha1 sur la concat des colonnes clé (normalisées)."""
    parts = []
    for c in key_cols:
        v = row.get(c, "")
        if pd.isna(v):
            v = ""
        v = str(v).strip().lower()
        v = re.sub(r"\s+", " ", v)
        parts.append(v)
    raw = "||".join(parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def build_query(name, address, fallback=None) -> str:
    """
    Query = nom + adresse (et fallback si le nom est vide)
    """
    name = safe_str(name)
    address = safe_str(address)
    fallback = safe_str(fallback)
    base = name if name else fallback
    return f"{base} {address}".strip()


# ============================================================
# 1) Load & dedupe google*.xlsx
# ============================================================

def load_all_google_excels(
    root_folder: str = ".",
    dedupe_key_cols: list[str] | None = None,
) -> pd.DataFrame:
    """
    Parcourt root_folder, charge google.xlsx + google (n).xlsx de chaque sous-dossier.
    Ajoute:
      - category (nom du dossier normalisé)
      - source_file
      - source_folder
      - row_id
    Déduplique sur row_id.
    """
    root = Path(root_folder).resolve()
    if not root.exists():
        raise FileNotFoundError(f"Dossier root introuvable: {root}")

    default_candidates = [
        "name", "title", "nom", "adresse", "address", "formatted_address",
        "phone", "telephone", "téléphone", "website", "site", "url"
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
            df["source_file"] = f.name
            df["source_folder"] = sub.name
            df["category"] = category
            all_dfs.append(df)

    if not all_dfs:
        raise FileNotFoundError(f"Aucun fichier google.xlsx / google (n).xlsx trouvé sous: {root}")

    df_all = pd.concat(all_dfs, ignore_index=True).dropna(how="all").reset_index(drop=True)

    col_map = {c.lower().strip(): c for c in df_all.columns}
    existing_key_cols = []
    for k in dedupe_key_cols:
        k_low = k.lower().strip()
        if k_low in col_map:
            existing_key_cols.append(col_map[k_low])

    if not existing_key_cols:
        existing_key_cols = [c for c in df_all.columns if c not in ["source_file", "source_folder", "category"]]

    df_all["row_id"] = df_all.apply(lambda r: compute_row_id(r, existing_key_cols), axis=1)

    before = len(df_all)
    df_all = df_all.drop_duplicates(subset=["row_id"], keep="first").reset_index(drop=True)
    after = len(df_all)

    logging.info("[load] Déduplication: %s -> %s lignes (supprimé %s)", before, after, before - after)
    logging.info("[load] Clés utilisées pour row_id: %s", existing_key_cols)

    return df_all


# ============================================================
# 2) Lat/Lng (optional) with cache
# ============================================================

def add_lat_lng(
    df: pd.DataFrame,
    address_col: str,
    cache_path: str = "geocode_cache.csv",
    min_delay_seconds: float = 1.1,
    timeout_seconds: int = 10,
) -> pd.DataFrame:
    """
    Ajoute latitude/longitude à partir d'une colonne d'adresse.
    Utilise un cache CSV (_addr, latitude, longitude).
    """
    if Nominatim is None or RateLimiter is None:
        raise ImportError("geopy n'est pas installé. pip install geopy")

    df = df.copy()

    if address_col not in df.columns:
        raise ValueError(f"address_col='{address_col}' introuvable dans le dataframe.")

    def normalize(a: str) -> str:
        a = safe_str(a)
        if not a or a.lower() == "nan":
            return ""
        a_low = a.lower()
        if "paris" not in a_low:
            a = f"{a}, Paris"
        if "france" not in a_low:
            a = f"{a}, France"
        return a

    df["_addr"] = df[address_col].astype(str).fillna("").map(normalize)

    cache_file = Path(cache_path)
    cache_file.parent.mkdir(parents=True, exist_ok=True)

    if cache_file.exists():
        cache_df = pd.read_csv(cache_file)
    else:
        cache_df = pd.DataFrame(columns=["_addr", "latitude", "longitude"])

    cache_map = {
        r["_addr"]: (r["latitude"], r["longitude"])
        for _, r in cache_df.dropna(subset=["_addr"]).iterrows()
    }

    geolocator = Nominatim(user_agent="wink-airflow-geocoder")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=min_delay_seconds)

    lat_list = []
    lon_list = []
    new_rows = []

    for a in df["_addr"]:
        if not a:
            lat_list.append(None)
            lon_list.append(None)
            continue

        if a in cache_map:
            lat, lon = cache_map[a]
            lat_list.append(lat)
            lon_list.append(lon)
            continue

        try:
            loc = geocode(a, timeout=timeout_seconds)
            if loc:
                lat_list.append(loc.latitude)
                lon_list.append(loc.longitude)
                new_rows.append({"_addr": a, "latitude": loc.latitude, "longitude": loc.longitude})
            else:
                lat_list.append(None)
                lon_list.append(None)
                new_rows.append({"_addr": a, "latitude": None, "longitude": None})
        except Exception:
            lat_list.append(None)
            lon_list.append(None)
            new_rows.append({"_addr": a, "latitude": None, "longitude": None})

    df["latitude"] = lat_list
    df["longitude"] = lon_list

    if new_rows:
        cache_df = pd.concat([cache_df, pd.DataFrame(new_rows)], ignore_index=True)
        cache_df = cache_df.drop_duplicates(subset=["_addr"], keep="last")
        cache_df.to_csv(cache_file, index=False)

    df = df.drop(columns=["_addr"], errors="ignore")
    return df


# ============================================================
# 3) YouTube Shorts (✅ robust + liste)
# ============================================================

YOUTUBE_SHORTS_URL_RE = re.compile(r"^https?://(www\.)?youtube\.com/shorts/[^/?#]+", re.IGNORECASE)


async def _handle_youtube_consent_best_effort(page) -> None:
    """
    Best effort : clique si un bouton cookie est présent.
    Ne doit jamais faire planter le scraping.
    """
    candidates = [
        "button:has-text('Tout accepter')",
        "button:has-text('J’accepte')",
        "button:has-text('Accepter tout')",
        "button:has-text('I agree')",
        "button:has-text('Accept all')",
        "button:has-text('Tout refuser')",
        "button:has-text('Reject all')",
    ]
    for _ in range(2):
        clicked = False
        for sel in candidates:
            try:
                btn = await page.query_selector(sel)
                if btn:
                    await btn.click()
                    await page.wait_for_timeout(1200)
                    clicked = True
                    break
            except Exception:
                pass
        if not clicked:
            break


async def find_youtube_shorts_list(
    page,
    query: str,
    *,
    max_results: int = 5,
    scroll_steps: int = 6,
) -> list[str]:
    """
    Retourne une liste de liens YouTube Shorts (max_results).
    Stratégie robuste :
    - aller sur results
    - gérer consent best effort
    - scroll un peu
    - récupérer a[href*="/shorts/"] dans le DOM
    """
    url = "https://www.youtube.com/results?search_query=" + quote_plus(query)
    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(2000)

    await _handle_youtube_consent_best_effort(page)

    for _ in range(scroll_steps):
        try:
            await page.mouse.wheel(0, 1600)
        except Exception:
            pass
        await page.wait_for_timeout(700)

    anchors = await page.query_selector_all("a[href*='/shorts/']")
    results: list[str] = []
    for a in anchors:
        href = await a.get_attribute("href")
        if not href:
            continue
        if href.startswith("/"):
            href = "https://www.youtube.com" + href
        # normalize a bit
        href = href.split("&")[0]
        if YOUTUBE_SHORTS_URL_RE.match(href) and href not in results:
            results.append(href)
        if len(results) >= max_results:
            break

    return results


async def add_youtube_shorts_to_df(
    df: pd.DataFrame,
    name_col: str,
    address_col: str,
    headless: bool = True,
    max_results: int = 5,
) -> pd.DataFrame:
    """
    Ajoute:
      - youtube_query (str)
      - youtube_short (str) => premier short (compat DB)
      - youtube_shorts (str JSON) => liste complète (max_results)
    """
    df = df.copy()
    if "youtube_query" not in df.columns:
        df["youtube_query"] = None
    df["youtube_short"] = None
    df["youtube_shorts"] = None

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 720},
            locale="fr-FR",
        )
        page = await context.new_page()

        for i, row in df.iterrows():
            name = safe_str(row.get(name_col))
            address = safe_str(row.get(address_col))
            query = build_query(name, address, fallback=safe_str(row.get("category")))
            df.at[i, "youtube_query"] = query

            try:
                shorts = await find_youtube_shorts_list(page, query, max_results=max_results)
                df.at[i, "youtube_short"] = shorts[0] if shorts else None
                df.at[i, "youtube_shorts"] = json.dumps(shorts, ensure_ascii=False) if shorts else None
            except Exception as e:
                logging.warning("[youtube] failed for query=%r err=%s", query, e)
                df.at[i, "youtube_short"] = None
                df.at[i, "youtube_shorts"] = None

        await context.close()
        await browser.close()

    return df


# ============================================================
# 4) TikTok
# ============================================================

@dataclass
class TikTokPlace:
    name: str
    address: str
    tiktok_video_url: Optional[str] = None


async def _dismiss_tiktok_popups(page) -> None:
    for _ in range(3):
        try:
            btn = await page.query_selector("button:has-text('Accept')")
            if btn:
                await btn.click()
        except Exception:
            pass
        await page.wait_for_timeout(200)


async def find_tiktok_video(page, place: TikTokPlace, max_scrolls: int = 12) -> TikTokPlace:
    query = quote_plus(f"{place.name} {place.address} paris")
    url = f"https://www.tiktok.com/search?q={query}"
    await page.goto(url, wait_until="domcontentloaded", timeout=60000)

    await _dismiss_tiktok_popups(page)
    await page.wait_for_timeout(1000)

    for _ in range(max_scrolls):
        await _dismiss_tiktok_popups(page)
        anchors = await page.query_selector_all('a[href*="/video/"]')
        for a in anchors:
            href = await a.get_attribute("href")
            if href and "/video/" in href:
                place.tiktok_video_url = href if href.startswith("http") else f"https://www.tiktok.com{href}"
                return place
        try:
            await page.mouse.wheel(0, 1600)
        except Exception:
            pass
        await page.wait_for_timeout(500)

    return place


async def add_tiktok_videos_to_df(
    df: pd.DataFrame,
    name_col: str,
    address_col: str,
    headless: bool = True,
    tt_state_path: str = "tt_state.json",
) -> pd.DataFrame:
    df = df.copy()
    df["tiktok_query"] = None
    df["tiktok_video"] = None

    storage_state = tt_state_path if tt_state_path and Path(tt_state_path).exists() else None
    if tt_state_path and storage_state is None:
        logging.warning("[tiktok] tt_state introuvable (%s) -> on continue sans storageState", tt_state_path)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)

        context_kwargs = {}
        if storage_state:
            context_kwargs["storage_state"] = storage_state

        context = await browser.new_context(**context_kwargs)
        page = await context.new_page()

        for i, row in df.iterrows():
            name = safe_str(row.get(name_col))
            address = safe_str(row.get(address_col))
            query = build_query(name, address, fallback=safe_str(row.get("category")))
            df.at[i, "tiktok_query"] = query

            try:
                updated = await find_tiktok_video(page, TikTokPlace(name=name, address=address))
                df.at[i, "tiktok_video"] = updated.tiktok_video_url
            except Exception:
                df.at[i, "tiktok_video"] = None

        await context.close()
        await browser.close()

    return df


# ============================================================
# 5) Instagram (best effort, no login) via DuckDuckGo HTML
# ============================================================

INSTAGRAM_REEL_RE = re.compile(
    r"https?://(?:www\.)?instagram\.com/(?:reel|reels)/[A-Za-z0-9_\-]+/?",
    re.IGNORECASE,
)


def _duckduckgo_html_search(query: str, timeout: int = 20) -> str:
    url = "https://duckduckgo.com/html/?q=" + quote_plus(query)
    req = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        },
    )
    with urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def find_instagram_reel_link(query: str) -> str | None:
    try:
        html = _duckduckgo_html_search(f"site:instagram.com/reel {query}")
    except (HTTPError, URLError, TimeoutError, Exception):
        return None

    m = INSTAGRAM_REEL_RE.search(html)
    if m:
        return m.group(0)

    try:
        html = _duckduckgo_html_search(f"site:instagram.com/reels {query}")
    except Exception:
        return None

    m = INSTAGRAM_REEL_RE.search(html)
    return m.group(0) if m else None


def add_instagram_reels_to_df(df: pd.DataFrame, name_col: str, address_col: str) -> pd.DataFrame:
    df = df.copy()
    df["instagram_query"] = None
    df["instagram_reel"] = None

    for i, row in df.iterrows():
        name = safe_str(row.get(name_col))
        address = safe_str(row.get(address_col))
        query = build_query(name, address, fallback=safe_str(row.get("category")))
        df.at[i, "instagram_query"] = query

        try:
            df.at[i, "instagram_reel"] = find_instagram_reel_link(query)
        except Exception:
            df.at[i, "instagram_reel"] = None

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
    debug_limit_rows: int | None = None,
    do_youtube: bool = True,
    do_tiktok: bool = True,
    do_instagram: bool = True,
) -> pd.DataFrame:
    df = load_all_google_excels(root_folder=root_folder)

    if debug_limit_rows is not None:
        df = df.head(int(debug_limit_rows)).copy()
        logging.warning("🧪 [debug] Limitation DataFrame à %s lignes (debug_limit_rows=%s)", len(df), debug_limit_rows)

    if do_geocode:
        df = add_lat_lng(df, address_col=address_col, cache_path=geocode_cache)

    # Colonnes stables
    for c in [
        "youtube_query", "youtube_short", "youtube_shorts",
        "tiktok_query", "tiktok_video",
        "instagram_query", "instagram_reel"
    ]:
        if c not in df.columns:
            df[c] = None

    if do_youtube:
        df = await add_youtube_shorts_to_df(
            df,
            name_col=name_col,
            address_col=address_col,
            headless=headless,
            max_results=5,
        )

    if do_tiktok:
        df = await add_tiktok_videos_to_df(
            df,
            name_col=name_col,
            address_col=address_col,
            headless=headless,
            tt_state_path=tt_state_path,
        )

    if do_instagram:
        df = add_instagram_reels_to_df(df, name_col=name_col, address_col=address_col)

    return df


def main():
    parser = argparse.ArgumentParser(description="Enrich Google excels with YouTube Shorts + TikTok + Instagram (best effort).")
    parser.add_argument("--root", default=".", help="Root folder containing subfolders with google*.xlsx")
    parser.add_argument("--name-col", required=True, help="Column name containing the place name")
    parser.add_argument("--address-col", required=True, help="Column name containing the address")
    parser.add_argument("--headless", action="store_true", help="Run browsers headless")
    parser.add_argument("--geocode", action="store_true", help="Also compute latitude/longitude (requires geopy)")
    parser.add_argument("--geocode-cache", default="geocode_cache.csv", help="CSV cache file for geocoding")
    parser.add_argument("--tt-state", default="tt_state.json", help="TikTok storage_state file")
    parser.add_argument("--debug-limit-rows", type=int, default=None, help="Limit rows before enrichment")
    parser.add_argument("--out", default="enriched.csv", help="Output CSV path")

    parser.add_argument("--no-youtube", action="store_true", help="Disable YouTube enrichment")
    parser.add_argument("--no-tiktok", action="store_true", help="Disable TikTok enrichment")
    parser.add_argument("--no-instagram", action="store_true", help="Disable Instagram enrichment")

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
            do_instagram=(not args.no_instagram),
        )
    )
    df.to_csv(args.out, index=False)
    print(f"[done] Saved: {args.out}")


if __name__ == "__main__":
    main()
