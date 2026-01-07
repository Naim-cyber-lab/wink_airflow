from __future__ import annotations

import argparse
import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd
from urllib.parse import quote_plus

from playwright.async_api import async_playwright

# Optionnel (lat/lng)
try:
    from geopy.geocoders import Nominatim
    from geopy.extra.rate_limiter import RateLimiter
except Exception:
    Nominatim = None
    RateLimiter = None


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
    """restaurant-paris-asiatiques -> restaurant paris asiatiques"""
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
    Exemple: "Tour Eiffel Champ de Mars, 5 Avenue Anatole France, 75007 Paris"
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
      - category
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

    # mapping columns case-insensitive
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

    print(f"[load] Déduplication: {before} -> {after} lignes (supprimé {before-after})")
    print(f"[load] Clés utilisées pour row_id: {existing_key_cols}")

    return df_all


# ============================================================
# 2) Lat/Lng (optional) with cache
# ============================================================

def choose_address_col(df: pd.DataFrame) -> str:
    """
    Heuristique simple pour choisir une colonne adresse.
    """
    patterns = [
        r"\brue\b", r"\bbd\b", r"\bboulevard\b", r"\bavenue\b", r"\bav\b",
        r"\bplace\b", r"\bquai\b", r"\ballée\b", r"\bimpasse\b",
        r"\bparis\b", r"\b750\d{2}\b"
    ]
    regex = re.compile("|".join(patterns), re.IGNORECASE)

    ignore = {"source_file", "source_folder", "category", "row_id", "latitude", "longitude"}
    best_col, best_score = None, -1

    for c in df.columns:
        if c in ignore:
            continue
        s = df[c].dropna().astype(str)
        if s.empty:
            continue
        sample = s.head(300)
        score = sample.apply(lambda x: 1 if regex.search(x) else 0).sum()
        if score > best_score:
            best_col, best_score = c, score

    if not best_col or best_score == 0:
        raise ValueError("Colonne adresse introuvable automatiquement. Passe address_col explicitement.")
    return best_col


def add_lat_lng(
    df: pd.DataFrame,
    address_col: str | None = None,
    cache_path: str = "geocode_cache.csv",
    min_delay_seconds: float = 1.1,
    timeout_seconds: int = 10,
) -> pd.DataFrame:
    """
    Ajoute latitude/longitude à partir de l'adresse (Nominatim) + cache CSV.
    """
    if Nominatim is None or RateLimiter is None:
        raise ImportError("geopy n'est pas installé. pip install geopy")

    df = df.copy()

    if address_col is None:
        address_col = choose_address_col(df)
    print("[geo] Colonne adresse utilisée:", address_col)

    addr = df[address_col].astype(str).fillna("").str.strip()

    def normalize(a: str) -> str:
        if not a or a.lower() == "nan":
            return ""
        a_low = a.lower()
        if "paris" not in a_low:
            a = f"{a}, Paris"
        if "france" not in a_low:
            a = f"{a}, France"
        return a

    df["_addr"] = addr.map(normalize)

    cache_file = Path(cache_path)
    cache: dict[str, tuple[Optional[float], Optional[float]]] = {}

    if cache_file.exists():
        old = pd.read_csv(cache_file)
        for _, r in old.iterrows():
            cache[str(r["address"])] = (r["latitude"], r["longitude"])

    geolocator = Nominatim(user_agent="paris-activities-geocoder")
    geocode = RateLimiter(
        lambda q: geolocator.geocode(q, timeout=timeout_seconds),
        min_delay_seconds=min_delay_seconds,
        swallow_exceptions=True,
    )

    unique_addrs = [a for a in df["_addr"].unique().tolist() if a]
    new_rows = []

    for a in unique_addrs:
        if a in cache:
            continue
        loc = geocode(a)
        if loc:
            cache[a] = (float(loc.latitude), float(loc.longitude))
        else:
            cache[a] = (None, None)
        new_rows.append({"address": a, "latitude": cache[a][0], "longitude": cache[a][1]})

    if new_rows:
        new_df = pd.DataFrame(new_rows)
        if cache_file.exists():
            merged = pd.concat([pd.read_csv(cache_file), new_df], ignore_index=True)
            merged = merged.drop_duplicates(subset=["address"], keep="last")
        else:
            merged = new_df
        merged.to_csv(cache_file, index=False)

    df["latitude"] = df["_addr"].map(lambda a: cache.get(a, (None, None))[0])
    df["longitude"] = df["_addr"].map(lambda a: cache.get(a, (None, None))[1])
    df.drop(columns=["_addr"], inplace=True)

    found = df["latitude"].notna().sum()
    print(f"[geo] Coords trouvées: {found} / {len(df)}")

    return df


# ============================================================
# 3) YouTube Shorts
# ============================================================

@dataclass
class Place:
    query_text: str | None = None
    name: str | None = None
    address: str | None = None
    youtube_short_url: str | None = None


async def _accept_consent_if_present(page):
    candidates = [
        'button:has-text("Tout accepter")',
        'button:has-text("Accepter tout")',
        'button:has-text("J’accepte")',
        'button:has-text("J\'accepte")',
        'button:has-text("Accept all")',
        'button:has-text("I agree")',
    ]
    for sel in candidates:
        try:
            btn = page.locator(sel)
            if await btn.count() > 0:
                await btn.first.click()
                await page.wait_for_timeout(800)
                return
        except Exception:
            pass


async def find_youtube_short(page, place: Place) -> Place:
    query = build_query(place.name, place.address, place.query_text)
    if not query:
        return place

    url = f"https://www.youtube.com/results?search_query={quote_plus(query)}"
    await page.goto(url, wait_until="domcontentloaded")
    await _accept_consent_if_present(page)

    try:
        await page.wait_for_selector("ytd-search", timeout=10_000)
    except Exception:
        # debug screenshot
        try:
            await page.screenshot(path="yt_debug.png", full_page=True)
        except Exception:
            pass
        return place

    links = page.locator('a[href*="/shorts/"]')
    for _ in range(8):
        if await links.count() > 0:
            href = await links.first.get_attribute("href")
            if href:
                place.youtube_short_url = f"https://www.youtube.com{href}"
            return place

        await page.mouse.wheel(0, 2500)
        await page.wait_for_timeout(1000)

    return place


async def add_youtube_shorts_to_df(
    df: pd.DataFrame,
    name_col: str,
    address_col: str,
    fallback_col: str | None = None,
    headless: bool = True,
) -> pd.DataFrame:
    df = df.copy()
    df["youtube_query"] = None
    df["youtube_short"] = None

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(locale="fr-FR", viewport={"width": 1280, "height": 800})
        page = await context.new_page()

        for i, row in df.iterrows():
            name = row.get(name_col)
            address = row.get(address_col)
            fallback = row.get(fallback_col) if fallback_col else None

            query = build_query(name, address, fallback)
            df.at[i, "youtube_query"] = query
            if not query:
                continue

            place = Place(
                name=safe_str(name) or None,
                address=safe_str(address) or None,
                query_text=safe_str(fallback) or None,
            )
            updated = await find_youtube_short(page, place)
            df.at[i, "youtube_short"] = updated.youtube_short_url

        await context.close()
        await browser.close()

    return df


# ============================================================
# 4) TikTok
# ============================================================

@dataclass
class TikTokPlace:
    query_text: Optional[str] = None
    name: Optional[str] = None
    address: Optional[str] = None
    tiktok_video_url: Optional[str] = None


async def _dismiss_tiktok_popups(page) -> None:
    candidates = [
        'button:has-text("Accepter")',
        'button:has-text("Tout accepter")',
        'button:has-text("Accept all")',
        'button:has-text("Accept")',
        'button:has-text("Plus tard")',
        'button:has-text("Not now")',
        'button:has-text("Fermer")',
        'button:has-text("Close")',
    ]
    for sel in candidates:
        try:
            btn = page.locator(sel)
            if await btn.count() > 0:
                await btn.first.click(timeout=1500)
                await page.wait_for_timeout(400)
        except Exception:
            pass


async def save_tiktok_state(state_path: str = "tt_state.json") -> None:
    """
    Ouvre TikTok en headful, tu te connectes 1 fois,
    puis sauvegarde cookies/session dans state_path.
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(locale="fr-FR", viewport={"width": 1280, "height": 800})
        page = await context.new_page()

        await page.goto("https://www.tiktok.com/", wait_until="domcontentloaded")
        await _dismiss_tiktok_popups(page)

        print("➡️ Connecte-toi manuellement si TikTok le demande.")
        print("➡️ Quand tu peux naviguer sans blocage, reviens ici et appuie sur Entrée...")
        input()

        await context.storage_state(path=state_path)
        print(f"✅ Session TikTok sauvegardée dans: {state_path}")

        await context.close()
        await browser.close()


async def find_tiktok_video(page, place: TikTokPlace, max_scrolls: int = 12) -> TikTokPlace:
    query = build_query(place.name, place.address, place.query_text)
    if not query:
        return place

    url = f"https://www.tiktok.com/search?q={quote_plus(query)}"
    await page.goto(url, wait_until="domcontentloaded")
    await _dismiss_tiktok_popups(page)
    await page.wait_for_timeout(1200)

    video_links = page.locator('a[href*="/video/"]')

    for _ in range(max_scrolls):
        await _dismiss_tiktok_popups(page)

        c = await video_links.count()
        if c > 0:
            href = await video_links.first.get_attribute("href")
            if href:
                place.tiktok_video_url = href if href.startswith("http") else f"https://www.tiktok.com{href}"
            return place

        await page.mouse.wheel(0, 2800)
        await page.wait_for_timeout(900)

    return place


async def add_tiktok_videos_to_df(
    df: pd.DataFrame,
    name_col: str,
    address_col: str,
    fallback_col: str | None = None,
    headless: bool = True,
    tt_state_path: str | None = "tt_state.json",
    throttle_ms: int = 700,
) -> pd.DataFrame:
    """
    Ajoute:
      - tiktok_query
      - tiktok_video
    Utilise storage_state si fourni.
    """
    df = df.copy()
    df["tiktok_query"] = None
    df["tiktok_video"] = None

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)

        context_kwargs = {
            "locale": "fr-FR",
            "viewport": {"width": 1280, "height": 800},
        }
        if tt_state_path:
            context_kwargs["storage_state"] = tt_state_path

        context = await browser.new_context(**context_kwargs)
        page = await context.new_page()

        for i, row in df.iterrows():
            name = row.get(name_col)
            address = row.get(address_col)
            fallback = row.get(fallback_col) if fallback_col else None

            query = build_query(name, address, fallback)
            df.at[i, "tiktok_query"] = query
            if not query:
                continue

            place = TikTokPlace(
                name=safe_str(name) or None,
                address=safe_str(address) or None,
                query_text=safe_str(fallback) or None,
            )

            updated = await find_tiktok_video(page, place)
            df.at[i, "tiktok_video"] = updated.tiktok_video_url

            await page.wait_for_timeout(throttle_ms)

        await context.close()
        await browser.close()

    return df


# ============================================================
# 5) Optional CLI runner
# ============================================================

async def run_pipeline(
    root_folder: str,
    name_col: str,
    address_col: str,
    do_geocode: bool = False,
    geocode_cache: str = "geocode_cache.csv",
    tt_state_path: str = "tt_state.json",
    headless: bool = False,
) -> pd.DataFrame:
    df = load_all_google_excels(root_folder=root_folder)

    if do_geocode:
        df = add_lat_lng(df, address_col=address_col, cache_path=geocode_cache)

    df = await add_youtube_shorts_to_df(
        df,
        name_col=name_col,
        address_col=address_col,
        headless=headless,
    )
    df = await add_tiktok_videos_to_df(
        df,
        name_col=name_col,
        address_col=address_col,
        headless=headless,
        tt_state_path=tt_state_path,
    )
    return df


def main():
    parser = argparse.ArgumentParser(description="Enrich Google excels with YouTube Shorts + TikTok video links.")
    parser.add_argument("--root", default=".", help="Root folder containing subfolders with google*.xlsx")
    parser.add_argument("--name-col", required=True, help="Column name containing the place name")
    parser.add_argument("--address-col", required=True, help="Column name containing the address")
    parser.add_argument("--headless", action="store_true", help="Run browsers headless")
    parser.add_argument("--geocode", action="store_true", help="Also compute latitude/longitude (requires geopy)")
    parser.add_argument("--geocode-cache", default="geocode_cache.csv", help="CSV cache file for geocoding")
    parser.add_argument("--tt-state", default="tt_state.json", help="TikTok storage_state file")
    parser.add_argument("--out", default="enriched.csv", help="Output CSV path")
    args = parser.parse_args()

    import asyncio
    df = asyncio.run(
        run_pipeline(
            root_folder=args.root,
            name_col=args.name_col,
            address_col=args.address_col,
            do_geocode=args.geocode,
            geocode_cache=args.geocode_cache,
            tt_state_path=args.tt_state,
            headless=args.headless,
        )
    )
    df.to_csv(args.out, index=False)
    print(f"[done] Saved: {args.out}")


if __name__ == "__main__":
    main()
