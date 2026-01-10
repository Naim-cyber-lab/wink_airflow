from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import re
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote_plus, unquote
from urllib.request import Request, urlopen

import pandas as pd
from playwright.async_api import async_playwright

# Optionnel (lat/lng)
try:
    from geopy.extra.rate_limiter import RateLimiter
    from geopy.geocoders import Nominatim
except Exception:
    Nominatim = None
    RateLimiter = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

GOOGLE_FILE_RE = re.compile(r"^google(?: \(\d+\))?\.xlsx$", re.IGNORECASE)

YOUTUBE_SHORTS_URL_RE = re.compile(r"^https?://(www\.)?youtube\.com/shorts/[^/?#]+", re.IGNORECASE)
TIKTOK_VIDEO_URL_RE = re.compile(r"^https?://(www\.)?tiktok\.com/@[^/]+/video/\d+", re.IGNORECASE)

# Instagram
INSTAGRAM_ANY_VIDEO_RE = re.compile(
    r"^https?://(?:www\.)?instagram\.com/(?:reel|reels|p)/[A-Za-z0-9_\-]+/?$",
    re.IGNORECASE,
)
INSTAGRAM_REEL_OR_POST_RE = re.compile(
    r"^https?://(?:www\.)?instagram\.com/(?:reel|reels|p)/[A-Za-z0-9_\-]+/?",
    re.IGNORECASE,
)


# ============================================================
# Utils
# ============================================================

def safe_str(x) -> str:
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x).strip()


def file_index(path: Path) -> int:
    if path.stem.lower() == "google":
        return 0
    m = re.match(r"^google \((\d+)\)$", path.stem.lower())
    return int(m.group(1)) if m else 10**9


def normalize_category(folder_name: str) -> str:
    return folder_name.replace("-", " ").strip()


def compute_row_id(row: pd.Series, key_cols: list[str]) -> str:
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
    name = safe_str(name)
    address = safe_str(address)
    fallback = safe_str(fallback)
    base = name if name else fallback
    return f"{base} {address}".strip()


def normalize_instagram_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    u = u.split("#")[0].split("?")[0].rstrip("/")
    if u.startswith("http://"):
        u = "https://" + u[len("http://") :]
    if u.startswith("https://instagram.com/"):
        u = "https://www.instagram.com/" + u[len("https://instagram.com/") :]
    if u.startswith("https://www.instagram.com//"):
        u = u.replace("https://www.instagram.com//", "https://www.instagram.com/")
    return u + "/"


# ============================================================
# 1) Load & dedupe google*.xlsx
# ============================================================

def load_all_google_excels(
    root_folder: str = ".",
    dedupe_key_cols: list[str] | None = None,
) -> pd.DataFrame:
    root = Path(root_folder).resolve()
    if not root.exists():
        raise FileNotFoundError(f"Dossier root introuvable: {root}")

    default_candidates = [
        "name", "title", "nom", "adresse", "address", "formatted_address",
        "phone", "telephone", "téléphone", "website", "site", "url",
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
# 3) YouTube Shorts => youtube_video (JSON list)
# ============================================================

async def _handle_youtube_consent_best_effort(page) -> None:
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


async def _find_youtube_shorts_list(page, query: str, *, max_results: int = 5) -> list[str]:
    url = "https://www.youtube.com/results?search_query=" + quote_plus(query)
    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(2000)
    await _handle_youtube_consent_best_effort(page)

    for _ in range(6):
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
    df = df.copy()
    df["youtube_query"] = None
    df["youtube_video"] = None

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(viewport={"width": 1280, "height": 720}, locale="fr-FR")
        page = await context.new_page()

        for i, row in df.iterrows():
            query = build_query(
                safe_str(row.get(name_col)),
                safe_str(row.get(address_col)),
                fallback=safe_str(row.get("category")),
            )
            df.at[i, "youtube_query"] = query

            try:
                shorts = await _find_youtube_shorts_list(page, query, max_results=max_results)
                df.at[i, "youtube_video"] = json.dumps(shorts, ensure_ascii=False) if shorts else None
            except Exception as e:
                logging.warning("[youtube] failed query=%r err=%s", query, e)
                df.at[i, "youtube_video"] = None

        await context.close()
        await browser.close()

    return df


# ============================================================
# 4) TikTok => tiktok_video (JSON list)
# ============================================================

async def _dismiss_tiktok_popups(page) -> None:
    for _ in range(3):
        try:
            btn = await page.query_selector("button:has-text('Accept')")
            if btn:
                await btn.click()
        except Exception:
            pass
        await page.wait_for_timeout(200)


async def _find_tiktok_videos_list(page, query: str, *, max_results: int = 5) -> list[str]:
    url = f"https://www.tiktok.com/search?q={quote_plus(query)}"
    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
    await _dismiss_tiktok_popups(page)
    await page.wait_for_timeout(1200)

    results: list[str] = []
    for _ in range(12):
        await _dismiss_tiktok_popups(page)
        anchors = await page.query_selector_all('a[href*="/video/"]')
        for a in anchors:
            href = await a.get_attribute("href")
            if not href:
                continue
            if href.startswith("/"):
                href = "https://www.tiktok.com" + href
            href = href.split("?")[0]
            if TIKTOK_VIDEO_URL_RE.match(href) and href not in results:
                results.append(href)
            if len(results) >= max_results:
                return results
        try:
            await page.mouse.wheel(0, 1600)
        except Exception:
            pass
        await page.wait_for_timeout(600)

    return results


async def add_tiktok_videos_to_df(
    df: pd.DataFrame,
    name_col: str,
    address_col: str,
    headless: bool = True,
    tt_state_path: str = "tt_state.json",
    max_results: int = 5,
) -> pd.DataFrame:
    df = df.copy()
    df["tiktok_query"] = None
    df["tiktok_video"] = None

    storage_state = tt_state_path if tt_state_path and Path(tt_state_path).exists() else None
    if tt_state_path and storage_state is None:
        logging.warning("[tiktok] tt_state introuvable (%s) -> sans storageState", tt_state_path)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)

        ctx_kwargs: dict = {}
        if storage_state:
            ctx_kwargs["storage_state"] = storage_state

        context = await browser.new_context(**ctx_kwargs)
        page = await context.new_page()

        for i, row in df.iterrows():
            query = build_query(
                safe_str(row.get(name_col)),
                safe_str(row.get(address_col)),
                fallback=safe_str(row.get("category")),
            )
            df.at[i, "tiktok_query"] = query

            try:
                videos = await _find_tiktok_videos_list(page, query, max_results=max_results)
                df.at[i, "tiktok_video"] = json.dumps(videos, ensure_ascii=False) if videos else None
            except Exception as e:
                logging.warning("[tiktok] failed query=%r err=%s", query, e)
                df.at[i, "tiktok_video"] = None

        await context.close()
        await browser.close()

    return df


# ============================================================
# 5) Instagram (Playwright + fallback DDG corrigé)
# ============================================================

def _duckduckgo_html_search(query: str, timeout: int = 20) -> str:
    url = "https://duckduckgo.com/html/?q=" + quote_plus(query)
    req = Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120 Safari/537.36"
            )
        },
    )
    with urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def _extract_instagram_links_from_ddg_html(html: str, max_results: int = 5) -> list[str]:
    """
    DDG /html/ renvoie souvent des liens intermédiaires:
      /l/?uddg=<URLENCODED>
    On décode uddg et on filtre instagram reel/reels/p.
    """
    links: list[str] = []

    # 1) uddg=... (cas le plus courant)
    for m in re.finditer(r"uddg=([^&\"'>\s]+)", html):
        try:
            decoded = unquote(m.group(1))
            decoded = normalize_instagram_url(decoded)
            if INSTAGRAM_ANY_VIDEO_RE.match(decoded) and decoded not in links:
                links.append(decoded)
                if len(links) >= max_results:
                    return links
        except Exception:
            continue

    # 2) parfois URL en clair
    for m in re.finditer(r"https?://(?:www\.)?instagram\.com/(?:reel|reels|p)/[A-Za-z0-9_\-]+/?", html, re.I):
        u = normalize_instagram_url(m.group(0))
        if INSTAGRAM_ANY_VIDEO_RE.match(u) and u not in links:
            links.append(u)
            if len(links) >= max_results:
                return links

    return links


def find_instagram_links_ddg(query: str, max_results: int = 5) -> list[str]:
    """
    Cherche sur DDG des pages IG vidéo liées au lieu.
    """
    queries = [
        f'site:instagram.com (reel OR reels OR p) "{query}"',
        f"site:instagram.com/reel {query}",
        f"site:instagram.com/p {query}",
    ]

    for q in queries:
        try:
            html = _duckduckgo_html_search(q)
        except (HTTPError, URLError, TimeoutError, Exception):
            continue

        links = _extract_instagram_links_from_ddg_html(html, max_results=max_results)
        if links:
            return links

    return []


async def _handle_instagram_popups_best_effort(page) -> None:
    candidates = [
        "button:has-text('Autoriser tous les cookies')",
        "button:has-text('Tout accepter')",
        "button:has-text('Accepter')",
        "button:has-text('Allow all cookies')",
        "button:has-text('Accept all')",
        "button:has-text('Not Now')",
        "button:has-text('Plus tard')",
        "button:has-text('Ignorer')",
    ]
    for _ in range(3):
        for sel in candidates:
            try:
                btn = await page.query_selector(sel)
                if btn:
                    await btn.click()
                    await page.wait_for_timeout(700)
            except Exception:
                pass
        await page.wait_for_timeout(300)


async def _find_instagram_links_list(page, query: str, *, max_results: int = 5) -> list[str]:
    await page.goto("https://www.instagram.com/", wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(1500)
    await _handle_instagram_popups_best_effort(page)

    # NB: IG change souvent ses selectors => best effort
    search_selectors = [
        "input[aria-label='Search input']",
        "input[placeholder*='Rechercher']",
        "input[placeholder*='Search']",
        "input[type='text']",
    ]
    search_input = None
    for sel in search_selectors:
        try:
            el = await page.query_selector(sel)
            if el:
                search_input = el
                break
        except Exception:
            pass

    if not search_input:
        return []

    try:
        await search_input.click()
        await page.wait_for_timeout(300)
        await search_input.fill(query[:80])
        await page.wait_for_timeout(1200)
    except Exception:
        return []

    try:
        await page.keyboard.press("Enter")
        await page.wait_for_timeout(900)
        await page.keyboard.press("Enter")
        await page.wait_for_timeout(1200)
    except Exception:
        pass

    await _handle_instagram_popups_best_effort(page)

    results: list[str] = []
    for _ in range(10):
        anchors = await page.query_selector_all("a[href]")
        for a in anchors:
            href = await a.get_attribute("href")
            if not href:
                continue
            if href.startswith("/"):
                href = "https://www.instagram.com" + href
            href = normalize_instagram_url(href)

            if INSTAGRAM_REEL_OR_POST_RE.match(href) and href not in results:
                results.append(href)

            if len(results) >= max_results:
                return results

        try:
            await page.mouse.wheel(0, 1600)
        except Exception:
            pass
        await page.wait_for_timeout(700)

    return results


async def add_instagram_videos_to_df(
    df: pd.DataFrame,
    name_col: str,
    address_col: str,
    headless: bool = True,
    ig_state_path: str = "ig_state.json",
    max_results: int = 5,
) -> pd.DataFrame:
    """
    Remplit:
      - instagram_query
      - instagram_video : JSON list de liens instagram (reel/reels/p)
    """
    df = df.copy()
    df["instagram_query"] = None
    df["instagram_video"] = None

    storage_state = ig_state_path if ig_state_path and Path(ig_state_path).exists() else None
    if ig_state_path and storage_state is None:
        logging.warning("[instagram] ig_state introuvable (%s) -> sans storageState (Playwright blocage probable)", ig_state_path)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)

        ctx_kwargs: dict = {"viewport": {"width": 1280, "height": 720}, "locale": "fr-FR"}
        if storage_state:
            ctx_kwargs["storage_state"] = storage_state

        context = await browser.new_context(**ctx_kwargs)
        page = await context.new_page()

        for i, row in df.iterrows():
            query = build_query(
                safe_str(row.get(name_col)),
                safe_str(row.get(address_col)),
                fallback=safe_str(row.get("category")),
            )
            df.at[i, "instagram_query"] = query

            try:
                # 1) Try Playwright IG
                links = await _find_instagram_links_list(page, query, max_results=max_results)
                logging.info("[instagram] query=%r found=%s", query, len(links))
                if links:
                    logging.info("[instagram] first=%s", links[0])

                # 2) Fallback DDG (corrigé)
                if not links:
                    ddg_links = find_instagram_links_ddg(query, max_results=max_results)
                    logging.info("[instagram] fallback DDG found=%s", len(ddg_links))
                    if ddg_links:
                        logging.info("[instagram] fallback first=%s", ddg_links[0])
                    links = ddg_links

                df.at[i, "instagram_video"] = json.dumps(links, ensure_ascii=False) if links else None

            except Exception as e:
                logging.warning("[instagram] failed query=%r err=%s", query, e)
                df.at[i, "instagram_video"] = None

        await context.close()
        await browser.close()

    # df.head logs
    try:
        logging.info("🔎 [instagram][head]\n%s", df[["instagram_query", "instagram_video"]].head(10).to_string(index=False))
    except Exception:
        pass

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
    ig_state_path: str = "ig_state.json",
    headless: bool = True,
    debug_limit_rows: int | None = None,
    do_youtube: bool = True,
    do_tiktok: bool = True,
    do_instagram: bool = True,
) -> pd.DataFrame:
    df = load_all_google_excels(root_folder=root_folder)

    if debug_limit_rows is not None:
        df = df.head(int(debug_limit_rows)).copy()
        logging.warning("🧪 [debug] Limitation DF à %s lignes", len(df))

    if do_geocode:
        df = add_lat_lng(df, address_col=address_col, cache_path=geocode_cache)

    # Colonnes stables
    for c in [
        "youtube_query", "youtube_video",
        "tiktok_query", "tiktok_video",
        "instagram_query", "instagram_video",
    ]:
        if c not in df.columns:
            df[c] = None

    if do_youtube:
        df = await add_youtube_shorts_to_df(df, name_col=name_col, address_col=address_col, headless=headless, max_results=5)

    if do_tiktok:
        df = await add_tiktok_videos_to_df(df, name_col=name_col, address_col=address_col, headless=headless, tt_state_path=tt_state_path, max_results=5)

    if do_instagram:
        df = await add_instagram_videos_to_df(df, name_col=name_col, address_col=address_col, headless=headless, ig_state_path=ig_state_path, max_results=5)

    return df


def main():
    parser = argparse.ArgumentParser(
        description="Enrich Google excels with YouTube Shorts(list)->youtube_video + TikTok(list)->tiktok_video + Instagram(list)->instagram_video."
    )
    parser.add_argument("--root", default=".", help="Root folder containing subfolders with google*.xlsx")
    parser.add_argument("--name-col", required=True, help="Column name containing the place name")
    parser.add_argument("--address-col", required=True, help="Column name containing the address")
    parser.add_argument("--headless", action="store_true", help="Run browsers headless")
    parser.add_argument("--geocode", action="store_true", help="Also compute latitude/longitude (requires geopy)")
    parser.add_argument("--geocode-cache", default="geocode_cache.csv", help="CSV cache file for geocoding")

    parser.add_argument("--tt-state", default="tt_state.json", help="TikTok storage_state file")
    parser.add_argument("--ig-state", default="ig_state.json", help="Instagram storage_state file")

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
            ig_state_path=args.ig_state,
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
