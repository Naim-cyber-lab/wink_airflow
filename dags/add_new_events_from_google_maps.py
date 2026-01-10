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

    # normalize decimal comma
    s = s.replace(",", ".")
    # extract first number
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
        name_col,
        address_col,
        "instagram_query",
        "instagram_video",
        "tiktok_video",
        "youtube_video",
    ]
    cols_debug = [c for c in cols_debug if c in df.columns]
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
# Task 2: upsert (bio + price mapping fixed)
# ---------------------------------------------------------------------
def upsert_events_from_csv(**context):
    params = context["params"]

    name_col = params["name_col"]
    address_col = params["address_col"]

    # ✅ mappings you requested
    bio_col = params.get("bio_col", "rllt__details 4")
    price_col = params.get("price_col", "rllt__details 2")

    website_col = params.get("website_col", "MRe4xd href")

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
        logging.info("🧪 [db] has instagram_video? %s | has price? %s | has bioEvent? %s",
                     "instagram_video" in cols, "price" in cols, "bioEvent" in cols)

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

            website = safe_str(row.get(website_col)) or None

            # ✅ requested mappings
            bio_value = safe_str(row.get(bio_col)) or None
            price_value = _parse_price(row.get(price_col))

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

                update_map: dict[str, object] = {}

                # ✅ bioEvent = rllt__details 4
                if "bioEvent" in cols and bio_value and bio_value != (old_bio or ""):
                    update_map["bioEvent"] = bio_value

                # ✅ price = rllt__details 2 (float)
                if "price" in cols and price_value is not None and (old_price is None or float(old_price) != float(price_value)):
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
                        details=f"cols={sorted(update_map.keys())} | price={price_value} | {media_info}",
                    )
                else:
                    noops += 1
                    _log_row(
                        "NOOP",
                        event_id=int(event_id),
                        titre=titre,
                        adresse=adresse,
                        details=f"no column changed | price={price_value} | {media_info}",
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
                details=f"cols={sorted(insert_map.keys())} | price={price_value} | {media_info}",
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
            "bio_col": "rllt__details 4",   # ✅
            "price_col": "rllt__details 2", # ✅
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
