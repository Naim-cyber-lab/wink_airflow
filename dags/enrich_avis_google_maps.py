from __future__ import annotations

import logging
import os
import time
import urllib.parse
from datetime import timedelta
from zoneinfo import ZoneInfo

import psycopg2
import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


PARIS_TZ = ZoneInfo("Europe/Paris")
DB_CONN_ID = "my_postgres"


def safe_str(x) -> str:
    return ("" if x is None else str(x)).strip()


def build_fallback_urls(*, titre: str, adresse: str, city: str) -> tuple[str, str]:
    """
    Fallback sans place_id (pas besoin d'API).
    - urlGoogleMapsAvis : Google Search "avis sur ..."
    - urlAjoutGoogleMapsAvis : Maps search (l'utilisateur pourra cliquer puis "Ajouter un avis")
    """
    query = " ".join([p for p in [titre, adresse, city] if p]).strip()

    # Ressemble à ton exemple "avis sur le 3bis"
    search_q = f"avis sur {titre}"
    if city:
        search_q += f" {city}"
    if adresse:
        search_q += f" {adresse}"

    url_avis = "https://www.google.com/search?q=" + urllib.parse.quote_plus(search_q)

    # Ouverture Google Maps sur une recherche du lieu (user clique puis voit onglet Avis)
    url_maps = "https://www.google.com/maps/search/?api=1&query=" + urllib.parse.quote_plus(query)

    return url_avis, url_maps


def fetch_place_id_from_text(query: str, api_key: str, session: requests.Session) -> str | None:
    """
    Google Places API - Find Place From Text
    https://developers.google.com/maps/documentation/places/web-service/search-find-place
    """
    endpoint = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
    params = {
        "input": query,
        "inputtype": "textquery",
        "fields": "place_id",
        "key": api_key,
    }
    try:
        r = session.get(endpoint, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        candidates = data.get("candidates") or []
        if candidates and isinstance(candidates, list):
            place_id = candidates[0].get("place_id")
            return safe_str(place_id) or None
    except Exception as e:
        logging.warning("Places API error for query=%r: %s", query, e)
    return None


def build_placeid_urls(place_id: str) -> tuple[str, str]:
    """
    Liens stables basés sur place_id
    - Page du lieu (avec avis accessibles)
    - Lien direct "ajouter un avis"
    """
    # Page Google Maps du lieu
    url_place = "https://www.google.com/maps/place/?q=place_id:" + urllib.parse.quote_plus(place_id)

    # Lien direct "Write a review"
    url_write = "https://search.google.com/local/writereview?placeid=" + urllib.parse.quote_plus(place_id)

    return url_place, url_write


def update_google_reviews_urls(**context):
    params = context["params"]

    batch_size = int(params.get("batch_size", 500))
    sleep_s = float(params.get("sleep_s", 0.0))
    only_active = params.get("only_active", None)

    api_key = os.getenv("GOOGLE_PLACES_API_KEY", "").strip()
    use_places = bool(api_key)

    conn = BaseHook.get_connection(DB_CONN_ID)
    connection = psycopg2.connect(
        host=conn.host,
        port=conn.port,
        user=conn.login,
        password=conn.password,
        dbname=conn.schema,
    )
    cursor = connection.cursor()

    session = requests.Session()

    updated = 0
    skipped = 0
    processed = 0

    try:
        where = []
        args = []

        # ✅ uniquement si urlGoogleMapsAvis est vide
        where.append('(COALESCE("urlGoogleMapsAvis", \'\') = \'\')')

        if only_active is not None:
            where.append('"active" = %s')
            args.append(int(only_active))

        where_sql = "WHERE " + " AND ".join(where)

        cursor.execute(
            f"""
            SELECT id, titre, adresse, city
            FROM profil_event
            {where_sql}
            ORDER BY id ASC
            """,
            tuple(args),
        )

        rows = cursor.fetchall()
        logging.info("Found %s events to process (use_places=%s)", len(rows), use_places)

        for (event_id, titre, adresse, city) in rows:
            processed += 1

            titre_s = safe_str(titre)
            adresse_s = safe_str(adresse)
            city_s = safe_str(city)

            if not titre_s:
                skipped += 1
                continue

            query = " ".join([p for p in [titre_s, adresse_s, city_s] if p]).strip()

            if use_places and query:
                place_id = fetch_place_id_from_text(query=query, api_key=api_key, session=session)
                if place_id:
                    url_avis, url_write = build_placeid_urls(place_id)
                else:
                    url_avis, url_write = build_fallback_urls(titre=titre_s, adresse=adresse_s, city=city_s)
            else:
                url_avis, url_write = build_fallback_urls(titre=titre_s, adresse=adresse_s, city=city_s)

            # ✅ sécurité: update seulement si toujours vide au moment de l'update
            cursor.execute(
                """
                UPDATE profil_event
                SET "urlGoogleMapsAvis" = %s,
                    "urlAjoutGoogleMapsAvis" = %s
                WHERE id = %s
                  AND COALESCE("urlGoogleMapsAvis", '') = ''
                """,
                (url_avis, url_write, int(event_id)),
            )

            if cursor.rowcount == 1:
                updated += 1
            else:
                skipped += 1

            if sleep_s > 0:
                time.sleep(sleep_s)

            if updated % batch_size == 0 and updated > 0:
                connection.commit()
                logging.info("Committed batch: updated=%s processed=%s skipped=%s", updated, processed, skipped)

        connection.commit()
        logging.info("DONE: updated=%s processed=%s skipped=%s", updated, processed, skipped)

    except Exception as e:
        connection.rollback()
        logging.error("Failure: %s", e)
        raise
    finally:
        cursor.close()
        connection.close()


default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="enrich_events_google_reviews_urls",
    start_date=days_ago(1),
    schedule_interval="15 3 * * *",
    catchup=False,
    default_args=default_args,
    tags=["event", "google", "reviews", "maps"],
    params={
        # force=True => réécrit tous les liens même si déjà remplis
        "force": Param(False, type="boolean"),
        # commit tous les N updates
        "batch_size": Param(500, type="integer"),
        # utile si GOOGLE_PLACES_API_KEY et tu veux éviter rate-limit
        "sleep_s": Param(0.0, type="number"),
        # None => tous, sinon filtre sur active (0/1/2...)
        "only_active": Param(None, type=["integer", "null"]),
    },
) as dag:
    dag.timezone = PARIS_TZ

    t1 = PythonOperator(
        task_id="update_google_reviews_urls",
        python_callable=update_google_reviews_urls,
        provide_context=True,
    )
