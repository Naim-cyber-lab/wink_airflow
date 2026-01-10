from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from datetime import timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg2
from psycopg2 import sql

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from social_enricher import run_pipeline  # type: ignore


PARIS_TZ = ZoneInfo("Europe/Paris")
DB_CONN_ID = "my_postgres"

POSTAL_RE = re.compile(r"\b(\d{5})\b")


def safe_str(x) -> str:
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x).strip()


def _norm_col(s: str) -> str:
    return re.sub(r"\s+", " ", safe_str(s).lower()).strip()


def resolve_col(df: pd.DataFrame, desired: str) -> str | None:
    if desired in df.columns:
        return desired
    desired_norm = _norm_col(desired)
    mapping = {_norm_col(c): c for c in df.columns}
    return mapping.get(desired_norm)


def extract_postal_code(address: str) -> str | None:
    if not address:
        return None
    m = POSTAL_RE.search(address)
    return m.group(1) if m else None


def _get_table_columns(cursor, table: str) -> set[str]:
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        """,
        (table,),
    )
    return {r[0] for r in cursor.fetchall()}


def _json_preview_list(json_list_str: str | None, limit: int = 10) -> list[str]:
    if not json_list_str:
        return []
    try:
        v = json.loads(json_list_str)
        if isinstance(v, list):
            out = []
            for x in v:
                if isinstance(x, str) and x.strip():
                    out.append(x.strip())
                if len(out) >= limit:
                    break
            return out
    except Exception:
        pass
    return []


def _short(s: str | None, n: int = 80) -> str:
    s = safe_str(s)
    if not s:
        return ""
    s = " ".join(s.split())
    return s if len(s) <= n else s[: n - 1] + "…"


def _media_stats(label: str, json_list_str: str | None) -> str:
    lst = _json_preview_list(json_list_str, limit=3)
    try:
        full = json.loads(json_list_str) if json_list_str else []
        count = len(full) if isinstance(full, list) else 0
    except Exception:
        count = 0
    preview = ", ".join([_short(x, 50) for x in lst]) if lst else ""
    return f"{label}={count}" + (f" | preview: {preview}" if preview else "")


def _log_row(action: str, *, titre: str, adresse: str, event_id: int | None = None, details: str = "") -> None:
    base = f"[db] {action}"
    if event_id is not None:
        base += f" id={event_id}"
    base += f" | titre='{_short(titre, 60)}' | adresse='{_short(adresse, 80)}'"
    if details:
        base += f" | {details}"
    logging.info(base)


def _count_non_empty(series: pd.Series | None) -> int:
    if series is None:
        return 0
    return int(series.fillna("").astype(str).str.strip().ne("").sum())


def _parse_price(v) -> float | None:
    """
    Convert rllt__details 2 -> float
    Accept: "€€", "10-20 €", "15", "15.5", "15,5"
    """
    s = safe_str(v)
    if not s:
        return None
    s = s.replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def _exec_insert(cursor, table: str, data: dict[str, object]):
    cols = [sql.Identifier(c) for c in data.keys()]
    placeholders = [sql.Placeholder() for _ in data.keys()]
    query = sql.SQL("INSERT INTO {t} ({cols}) VALUES ({vals}) RETURNING id").format(
        t=sql.Identifier(table),
        cols=sql.SQL(", ").join(cols),
        vals=sql.SQL(", ").join(placeholders),
    )
    cursor.execute(query, list(data.values()))
    return cursor.fetchone()[0]


def _exec_update(cursor, table: str, event_id: int, data: dict[str, object]):
    assignments = [sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder()) for k in data.keys()]
    query = sql.SQL("UPDATE {t} SET {assignments} WHERE id = %s").format(
        t=sql.Identifier(table),
        assignments=sql.SQL(", ").join(assignments),
    )
    values = list(data.values()) + [event_id]
    cursor.execute(query, values)


# ---------------------------------------------------------------------
# Task 1: enrich + export csv
# ---------------------------------------------------------------------
def enrich_and_export_csv(**context):
    params = context["params"]

    root_folder = params["root_folder"]
    name_col = params["name_col"]
    address_col = params["address_col"]

    # ✅ these 2 are your mappings; we log them EARLY (proof)
    bio_col_wanted = params.get("bio_col", "rllt__details 4")
    price_col_wanted = params.get("price_col", "rllt__details 2")

    headless = bool(params.get("headless", True))
    tt_state_path = params.get("tt_state_path")
    ig_state_path = params.get("ig_state_path")

    do_youtube = bool(params.get("do_youtube", True))
    do_tiktok = bool(params.get("do_tiktok", True))
    do_instagram = bool(params.get("do_instagram", True))
    debug_limit_rows = params.get("debug_limit_rows", 5)

    do_geocode = bool(params.get("do_geocode", True))
    geocode_cache = params.get("geocode_cache", "/opt/airflow/data/geocode_cache.csv")

    output_dir = params.get("output_dir", "/opt/airflow/data")
    os.makedirs(output_dir, exist_ok=True)
    out_csv = os.path.join(output_dir, "events_enriched.csv")

    logging.info("🚀 [enrich] Starting pipeline...")
    df = asyncio.run(
        run_pipeline(
            root_folder=root_folder,
            name_col=name_col,
            address_col=address_col,
            do_geocode=do_geocode,
            geocode_cache=geocode_cache,
            tt_state_path=tt_state_path,
            ig_state_path=ig_state_path,
            headless=headless,
            debug_limit_rows=debug_limit_rows,
            do_youtube=do_youtube,
            do_tiktok=do_tiktok,
            do_instagram=do_instagram,
        )
    )

    # Resolve bio/price columns (robust)
    bio_col = resolve_col(df, bio_col_wanted)
    price_col = resolve_col(df, price_col_wanted)
    ncol = resolve_col(df, name_col) or name_col
    acol = resolve_col(df, address_col) or address_col

    logging.info("🧪 [enrich] bio_col wanted=%r resolved=%r exists=%s", bio_col_wanted, bio_col, bool(bio_col))
    logging.info("🧪 [enrich] price_col wanted=%r resolved=%r exists=%s", price_col_wanted, price_col, bool(price_col))

    if bio_col:
        non_empty = _count_non_empty(df[bio_col])
        logging.info("🧪 [enrich] bio non-empty=%s / %s", non_empty, len(df))
        logging.info("🧪 [enrich] bio head:\n%s", df[[ncol, acol, bio_col]].head(10).to_string(index=False))
    else:
        logging.warning("🧪 [enrich] bio_col not found in df.columns -> bioEvent will NEVER be updated")

    if price_col:
        non_empty = _count_non_empty(df[price_col])
        logging.info("🧪 [enrich] price non-empty=%s / %s", non_empty, len(df))
        logging.info("🧪 [enrich] price head:\n%s", df[[ncol, acol, price_col]].head(10).to_string(index=False))

    # Ensure columns
    expected = [
        "category",
        "source_folder",
        "source_file",
        "latitude",
        "longitude",
        "youtube_query",
        "youtube_video",
        "tiktok_query",
        "tiktok_video",
        "instagram_query",
        "instagram_video",
    ]
    for col in expected:
        if col not in df.columns:
            df[col] = None

    cols_debug = [
        "category",
        "source_folder",
        ncol,
        acol,
        bio_col if bio_col else None,
        price_col if price_col else None,
        "instagram_query",
        "instagram_video",
        "tiktok_video",
        "youtube_video",
    ]
    cols_debug = [c for c in cols_debug if c and c in df.columns]
    logging.info("🔎 [enrich][head] cols=%s", cols_debug)
    logging.info("🔎 [enrich][head]\n%s", df[cols_debug].head(10).to_string(index=False))

    logging.info(
        "📊 [enrich] non-empty counts: instagram_video=%s instagram_query=%s | tiktok_video=%s | youtube_video=%s",
        _count_non_empty(df.get("instagram_video")),
        _count_non_empty(df.get("instagram_query")),
        _count_non_empty(df.get("tiktok_video")),
        _count_non_empty(df.get("youtube_video")),
    )

    df.to_csv(out_csv, index=False)
    logging.info("✅ [enrich] Exported: %s | rows=%s", out_csv, len(df))
    context["ti"].xcom_push(key="enriched_csv_path", value=out_csv)


# ---------------------------------------------------------------------
# Task 2: upsert (bioEvent + price)
# ---------------------------------------------------------------------
def upsert_events_from_csv(**context):
    params = context["params"]

    name_col_wanted = params["name_col"]
    address_col_wanted = params["address_col"]

    bio_col_wanted = params.get("bio_col", "rllt__details 4")
    price_col_wanted = params.get("price_col", "rllt__details 2")
    website_col_wanted = params.get("website_col", "MRe4xd href")

    region_default = params.get("region_default", "France")
    use_category_as_region = bool(params.get("use_category_as_region", True))

    creator_winker_id = int(params.get("creator_winker_id", 116))
    active_default = int(params.get("active_default", 0))
    max_participants = int(params.get("max_participants", 99999999))
    access_comment = bool(params.get("access_comment", True))
    validated_from_web = bool(params.get("validated_from_web", False))

    ti = context["ti"]
    csv_path = ti.xcom_pull(task_ids="enrich_and_export_csv", key="enriched_csv_path")
    if not csv_path or not os.path.exists(csv_path):
        raise FileNotFoundError(f"Enriched CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)
    logging.info("📥 [db] Loading CSV rows=%s from %s", len(df), csv_path)

    # Resolve columns robustly (THIS is the typical reason bioEvent doesn't update)
    name_col = resolve_col(df, name_col_wanted)
    address_col = resolve_col(df, address_col_wanted)
    bio_col = resolve_col(df, bio_col_wanted)
    price_col = resolve_col(df, price_col_wanted)
    website_col = resolve_col(df, website_col_wanted)

    logging.info("🧪 [db] name_col wanted=%r resolved=%r", name_col_wanted, name_col)
    logging.info("🧪 [db] address_col wanted=%r resolved=%r", address_col_wanted, address_col)
    logging.info("🧪 [db] bio_col wanted=%r resolved=%r exists=%s", bio_col_wanted, bio_col, bool(bio_col))
    logging.info("🧪 [db] price_col wanted=%r resolved=%r exists=%s", price_col_wanted, price_col, bool(price_col))

    if not name_col or not address_col:
        raise ValueError(f"CSV missing required columns: name_col={name_col_wanted}, address_col={address_col_wanted}")

    if bio_col:
        logging.info("🧪 [db] bio non-empty=%s / %s", _count_non_empty(df[bio_col]), len(df))
        logging.info("🧪 [db] bio head:\n%s", df[[name_col, address_col, bio_col]].head(10).to_string(index=False))
    else:
        logging.warning("🧪 [db] bio_col not found in CSV -> bioEvent will NEVER be updated")

    conn = BaseHook.get_connection(DB_CONN_ID)
    connection = psycopg2.connect(
        host=conn.host,
        port=conn.port,
        user=conn.login,
        password=conn.password,
        dbname=conn.schema,
    )
    cursor = connection.cursor()

    inserted = 0
    updated = 0
    noops = 0
    skipped = 0

    try:
        cols = _get_table_columns(cursor, "profil_event")
        logging.info("🧱 [db] profil_event columns detected: %s cols", len(cols))
        logging.info(
            "🧪 [db] has instagram_video? %s | has price? %s | has bioEvent? %s",
            "instagram_video" in cols,
            "price" in cols,
            "bioEvent" in cols,
        )

        for _, row in df.iterrows():
            titre = safe_str(row.get(name_col))
            adresse = safe_str(row.get(address_col))
            if not titre or not adresse:
                skipped += 1
                _log_row("SKIP", titre=titre, adresse=adresse, details="missing titre/adresse")
                continue

            category = safe_str(row.get("category")) or None
            region = (category if (use_category_as_region and category) else safe_str(region_default)) or None
            code_postal = extract_postal_code(adresse)

            website = safe_str(row.get(website_col)) or None if website_col else None

            # ✅ mappings
            bio_value = safe_str(row.get(bio_col)) if bio_col else ""
            bio_value = bio_value if bio_value.strip() else None

            price_value = _parse_price(row.get(price_col)) if price_col else None

            youtube_query = safe_str(row.get("youtube_query")) or None
            youtube_video = safe_str(row.get("youtube_video")) or None
            tiktok_query = safe_str(row.get("tiktok_query")) or None
            tiktok_video = safe_str(row.get("tiktok_video")) or None
            instagram_query = safe_str(row.get("instagram_query")) or None
            instagram_video = safe_str(row.get("instagram_video")) or None

            lat = row.get("latitude", None)
            lon = row.get("longitude", None)

            media_info = " | ".join([
                _media_stats("yt", youtube_video),
                _media_stats("tt", tiktok_video),
                _media_stats("ig", instagram_video),
            ])

            cursor.execute(
                """
                SELECT id, "bioEvent", price, website, lat, lon
                FROM profil_event
                WHERE titre = %s AND adresse = %s
                LIMIT 1
                """,
                (titre, adresse),
            )
            existing = cursor.fetchone()

            if existing:
                event_id, old_bio, old_price, old_website, old_lat, old_lon = existing

                # logs de preuve
                logging.info(
                    "[map] id=%s titre=%r | bio_value=%r(len=%s) old_bio=%r | price_value=%r old_price=%r",
                    event_id,
                    titre,
                    (bio_value[:120] + "…") if (bio_value and len(bio_value) > 120) else bio_value,
                    (len(bio_value) if bio_value else 0),
                    (str(old_bio)[:120] + "…") if (old_bio and len(str(old_bio)) > 120) else old_bio,
                    price_value,
                    old_price,
                )

                update_map: dict[str, object] = {}

                # ✅ bioEvent update: on compare après trim (évite le cas "identique mais espaces")
                if "bioEvent" in cols and bio_value:
                    old_norm = safe_str(old_bio)
                    new_norm = safe_str(bio_value)
                    if new_norm and new_norm != old_norm:
                        update_map["bioEvent"] = bio_value
                    else:
                        logging.info("[bioEvent] NO-UPDATE reason=%s", "empty" if not new_norm else "same_as_old")

                # ✅ price update
                if "price" in cols and price_value is not None:
                    try:
                        old_price_f = float(old_price) if old_price is not None else None
                    except Exception:
                        old_price_f = None
                    if old_price_f is None or float(price_value) != old_price_f:
                        update_map["price"] = float(price_value)

                if "website" in cols and (old_website is None and website is not None):
                    update_map["website"] = website

                if "lat" in cols and old_lat is None and lat is not None:
                    update_map["lat"] = float(lat)
                if "lon" in cols and old_lon is None and lon is not None:
                    update_map["lon"] = float(lon)

                if "region" in cols and region:
                    update_map["region"] = region
                if "codePostal" in cols and code_postal:
                    update_map["codePostal"] = code_postal

                if "youtube_query" in cols and youtube_query:
                    update_map["youtube_query"] = youtube_query
                if "youtube_video" in cols and youtube_video:
                    update_map["youtube_video"] = youtube_video

                if "tiktok_query" in cols and tiktok_query:
                    update_map["tiktok_query"] = tiktok_query
                if "tiktok_video" in cols and tiktok_video:
                    update_map["tiktok_video"] = tiktok_video

                if "instagram_query" in cols and instagram_query:
                    update_map["instagram_query"] = instagram_query
                if "instagram_video" in cols and instagram_video:
                    update_map["instagram_video"] = instagram_video

                if update_map:
                    _exec_update(cursor, "profil_event", int(event_id), update_map)
                    updated += 1
                    _log_row(
                        "UPDATE",
                        event_id=int(event_id),
                        titre=titre,
                        adresse=adresse,
                        details=f"cols={sorted(update_map.keys())} | {media_info}",
                    )
                else:
                    noops += 1
                    _log_row(
                        "NOOP",
                        event_id=int(event_id),
                        titre=titre,
                        adresse=adresse,
                        details=f"no column changed | {media_info}",
                    )
                continue

            # INSERT
            insert_map: dict[str, object] = {}

            if "titre" in cols:
                insert_map["titre"] = titre
            if "titre_fr" in cols:
                insert_map["titre_fr"] = titre
            if "adresse" in cols:
                insert_map["adresse"] = adresse

            if "region" in cols and region:
                insert_map["region"] = region
            if "codePostal" in cols and code_postal:
                insert_map["codePostal"] = code_postal

            # ✅ bioEvent + price
            if "bioEvent" in cols:
                insert_map["bioEvent"] = bio_value
            if "price" in cols:
                insert_map["price"] = float(price_value) if price_value is not None else None

            if "website" in cols:
                insert_map["website"] = website

            if "creatorWinker_id" in cols:
                insert_map["creatorWinker_id"] = creator_winker_id
            if "active" in cols:
                insert_map["active"] = active_default
            if "maxNumberParticipant" in cols:
                insert_map["maxNumberParticipant"] = max_participants
            if "accessComment" in cols:
                insert_map["accessComment"] = access_comment
            if "validated_from_web" in cols:
                insert_map["validated_from_web"] = validated_from_web

            if "lat" in cols and lat is not None:
                insert_map["lat"] = float(lat)
            if "lon" in cols and lon is not None:
                insert_map["lon"] = float(lon)

            if "youtube_query" in cols:
                insert_map["youtube_query"] = youtube_query
            if "youtube_video" in cols:
                insert_map["youtube_video"] = youtube_video

            if "tiktok_query" in cols:
                insert_map["tiktok_query"] = tiktok_query
            if "tiktok_video" in cols:
                insert_map["tiktok_video"] = tiktok_video

            if "instagram_query" in cols:
                insert_map["instagram_query"] = instagram_query
            if "instagram_video" in cols:
                insert_map["instagram_video"] = instagram_video

            if not insert_map:
                raise RuntimeError("Aucune colonne détectée pour insert dans profil_event (schéma inattendu).")

            new_id = _exec_insert(cursor, "profil_event", insert_map)
            inserted += 1
            _log_row(
                "INSERT",
                event_id=int(new_id),
                titre=titre,
                adresse=adresse,
                details=f"cols={sorted(insert_map.keys())} | {media_info}",
            )

        connection.commit()
        logging.info("✅ [db] Inserted=%s Updated=%s Noop=%s Skipped=%s", inserted, updated, noops, skipped)

    except Exception as e:
        connection.rollback()
        logging.error("❌ [db] Failure: %s", e)
        raise
    finally:
        cursor.close()
        connection.close()


default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="import_events_from_google_excels_social_enricher",
    start_date=days_ago(1),
    schedule_interval="0 3 * * *",
    catchup=False,
    default_args=default_args,
    tags=["event", "import", "tiktok", "youtube", "instagram", "playwright"],
    params={
        "do_youtube": Param(True, type="boolean"),
        "do_tiktok": Param(True, type="boolean"),
        "do_instagram": Param(True, type="boolean"),
        "debug_limit_rows": Param(5, type=["integer", "null"]),
    },
) as dag:
    dag.timezone = PARIS_TZ

    t1 = PythonOperator(
        task_id="enrich_and_export_csv",
        python_callable=enrich_and_export_csv,
        provide_context=True,
        params={
            "root_folder": "/opt/airflow/data",
            "name_col": "OSrXXb",
            "address_col": "rllt__details 3",
            "bio_col": "rllt__details 4",    # ✅ mapping visible dès enrich
            "price_col": "rllt__details 2",  # ✅ idem
            "headless": True,
            "tt_state_path": "/opt/airflow/data/tt_state.json",
            "ig_state_path": "/opt/airflow/data/ig_state.json",
            "do_geocode": True,
            "geocode_cache": "/opt/airflow/data/geocode_cache.csv",
            "output_dir": "/opt/airflow/data",
        },
    )

    t2 = PythonOperator(
        task_id="upsert_events_from_csv",
        python_callable=upsert_events_from_csv,
        provide_context=True,
        params={
            "name_col": "OSrXXb",
            "address_col": "rllt__details 3",
            "bio_col": "rllt__details 4",   # ✅ -> "bioEvent"
            "price_col": "rllt__details 2", # ✅ -> "price"
            "website_col": "MRe4xd href",
            "use_category_as_region": True,
            "region_default": "France",
            "creator_winker_id": 116,
            "active_default": 0,
            "max_participants": 99999999,
            "access_comment": True,
            "validated_from_web": False,
        },
    )

    t1 >> t2
