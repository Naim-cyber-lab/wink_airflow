import json
import logging
import re
import time
from datetime import timedelta
from urllib.parse import parse_qs, urlencode, urlparse, quote_plus
from zoneinfo import ZoneInfo

import psycopg2
import requests

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


# ================== CONFIG ==================
PARIS_TZ = ZoneInfo("Europe/Paris")
DB_CONN_ID = "my_postgres"

API_URL = "https://www.google.com/httpservice/web/PrivateLocalSearchUiDataService/GetLocalBoqProxy"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9",
    "Referer": "https://www.google.com/",
    # Cookies de consentement pour éviter la page consent.google.com
    "Cookie": (
        "CONSENT=YES+cb.20231020-07-p0.fr+FX+NW; "
        "SOCS=CAESEwgDEgk0OTc5NzMwNzIaAmZyIAEaBgiAnMezBg"
    ),
}

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


# ================== DB HELPERS ==================
def _pg_connect():
    conn = BaseHook.get_connection(DB_CONN_ID)
    return psycopg2.connect(
        host=conn.host,
        port=conn.port or 5432,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
    )


def fetch_events_to_scrape(limit: int) -> list[tuple[int, str]]:
    """
    Récupère tous les events où :
      - urlGoogleMapsAvis est renseignée (non null, non vide)
      - google_reviews est encore vide (null, '[]', ou '')
    """
    sql = """
        SELECT id, "urlGoogleMapsAvis"
        FROM profil_event
        WHERE "urlGoogleMapsAvis" IS NOT NULL
          AND "urlGoogleMapsAvis" <> ''
          AND (
              google_reviews IS NULL
              OR google_reviews::text = '[]'
              OR google_reviews::text = ''
          )
        ORDER BY id ASC
        LIMIT %s
    """
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (limit,))
            return [(int(r[0]), str(r[1])) for r in cur.fetchall()]
    finally:
        conn.close()


def update_event_reviews_in_db(event_id: int, reviews: list[dict]) -> None:
    """Sauvegarde les avis et la date de mise à jour dans profil_event."""
    sql = """
        UPDATE profil_event
        SET google_reviews = %s,
            google_reviews_updated_at = NOW()
        WHERE id = %s
    """
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (json.dumps(reviews, ensure_ascii=False), event_id))
        conn.commit()
    finally:
        conn.close()


# ================== URL HELPERS ==================
def extract_stick_and_hl(url: str) -> tuple[str | None, str]:
    """
    Extrait le paramètre `stick` (identifiant du lieu) et `hl` depuis
    n'importe quelle URL Google Search pointant vers un lieu.

    Fonctionne avec :
      - https://www.google.com/search?q=...&stick=H4sI...&tbm=lcl
      - https://www.google.com/search?q=...&stick=H4sI...&tbm=lcl#lkt=LocalPoiReviews
    """
    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=True)

    stick = params.get("stick", [None])[0]
    hl    = params.get("hl", ["fr-FR"])[0]

    return stick, hl


# ================== SCRAPER (HTTP pur, sans Playwright) ==================
def _parse_reviews_from_body(body: str) -> list[dict]:
    """
    Parse la réponse JSON de l'endpoint GetLocalBoqProxy.
    Structure : data[1][10][2] = liste des avis
    Chaque avis : [null, note(1-5), [date,...], [auteur,...], ..., texte(index 27)]
    """
    body_clean = re.sub(r"^\)\]\}'\n?", "", body)
    data = json.loads(body_clean)
    raw = data[1][10][2]
    reviews = []
    for r in raw:
        try:
            note       = r[1]
            date       = r[2][0].replace("\xa0", " ")
            author     = r[3][0]
            text       = r[27] if len(r) > 27 and isinstance(r[27], str) else ""
            visit_date = None
            for i in [32, 33, 34]:
                if len(r) > i and isinstance(r[i], str) and r[i].startswith("Visit"):
                    visit_date = r[i]
                    break
            reviews.append({
                "author":     author,
                "rating":     note,
                "date":       date,
                "text":       text,
                "visit_date": visit_date,
            })
        except Exception:
            pass
    return reviews


def _get_next_token(body: str) -> str | None:
    """Extrait le token de pagination depuis data[1][10][6]."""
    try:
        body_clean = re.sub(r"^\)\]\}'\n?", "", body)
        data = json.loads(body_clean)
        token = data[1][10][6]
        return token if isinstance(token, str) and token else None
    except Exception:
        return None


def scrape_reviews(url: str, max_reviews: int, delay: float = 1.0) -> list[dict]:
    """
    Récupère les avis Google via des appels HTTP directs à GetLocalBoqProxy.
    Pagine automatiquement grâce au next_token jusqu'à max_reviews.

    Args:
        url         : URL Google Search du lieu (avec ou sans #lkt=LocalPoiReviews)
        max_reviews : Nombre max d'avis à récupérer
        delay       : Délai entre chaque appel paginé (secondes)
    """
    stick, hl = extract_stick_and_hl(url)

    if not stick:
        raise ValueError(f"Paramètre 'stick' introuvable dans l'URL : {url}")

    all_reviews: list[dict] = []
    seen: set[tuple]        = set()
    next_token: str | None  = None
    page_num = 0

    while len(all_reviews) < max_reviews:
        page_num += 1

        # Construire les paramètres de la requête
        api_params: dict = {"hl": hl, "stick": stick}
        if next_token:
            api_params["pageToken"] = next_token

        resp = requests.get(
            API_URL,
            params=api_params,
            headers=HEADERS,
            timeout=15,
        )
        resp.raise_for_status()

        body = resp.text

        # Parser les avis de cette page
        try:
            batch = _parse_reviews_from_body(body)
        except Exception as exc:
            logging.warning("Page %d : erreur de parsing (%s)", page_num, exc)
            break

        if not batch:
            logging.info("Page %d : aucun avis, fin de pagination.", page_num)
            break

        # Dédupliquer et accumuler
        added = 0
        for r in batch:
            key = (r["author"], r["date"])
            if key not in seen:
                seen.add(key)
                all_reviews.append(r)
                added += 1

        logging.info(
            "Page %d : +%d avis (total %d)", page_num, added, len(all_reviews)
        )

        # Récupérer le token de la page suivante
        next_token = _get_next_token(body)
        if not next_token:
            logging.info("Page %d : plus de token de pagination, arrêt.", page_num)
            break

        # Pause pour ne pas surcharger Google
        if len(all_reviews) < max_reviews:
            time.sleep(delay)

    return all_reviews[:max_reviews]


# ================== AIRFLOW TASK ==================
def update_google_reviews(**_context):
    """
    Parcourt TOUS les events de profil_event où :
      - urlGoogleMapsAvis est renseignée
      - google_reviews est encore vide (jamais scrappé)

    Pour chaque event :
      1. Extrait le paramètre `stick` de l'URL (identifiant du lieu)
      2. Appelle directement l'API GetLocalBoqProxy en HTTP pur (pas de Playwright)
      3. Pagine automatiquement pour récupérer max_reviews avis
      4. Met à jour google_reviews + google_reviews_updated_at dans profil_event
    """
    limit       = int(Variable.get("GOOGLE_REVIEWS_BATCH_LIMIT",   default_var="200"))
    max_reviews = int(Variable.get("GOOGLE_REVIEWS_MAX_PER_EVENT", default_var="20"))
    delay       = float(Variable.get("GOOGLE_REVIEWS_DELAY_SEC",   default_var="1.0"))

    rows = fetch_events_to_scrape(limit=limit)

    if not rows:
        logging.info("Aucun événement à traiter (tous ont déjà des avis ou pas d'URL).")
        return

    logging.info(
        "%d événement(s) à scraper (max %d avis chacun, délai %.1fs entre pages)",
        len(rows), max_reviews, delay,
    )

    processed = skipped = failed = 0

    for event_id, url in rows:
        try:
            # Vérifier que le paramètre stick est présent dans l'URL
            stick, _ = extract_stick_and_hl(url)
            if not stick:
                skipped += 1
                logging.warning(
                    "Event %s : paramètre 'stick' absent dans l'URL (%s), ignoré.",
                    event_id, url,
                )
                continue

            reviews = scrape_reviews(url, max_reviews, delay=delay)

            if not reviews:
                # Aucun avis : on horodate pour ne pas re-tenter indéfiniment
                update_event_reviews_in_db(event_id, [])
                skipped += 1
                logging.warning(
                    "Event %s : aucun avis récupéré (url=%s)", event_id, url
                )
                continue

            update_event_reviews_in_db(event_id, reviews)
            processed += 1
            logging.info("Event %s : %d avis sauvegardés", event_id, len(reviews))

        except Exception as exc:
            failed += 1
            logging.exception("Event %s : échec scraping (%s)", event_id, exc)

    logging.info(
        "Terminé — processed=%d  skipped=%d  failed=%d",
        processed, skipped, failed,
    )


# ================== DAG ==================
with DAG(
    dag_id="profil_event_update_google_reviews",
    description=(
        "Pour chaque event ayant urlGoogleMapsAvis renseignée et google_reviews vide, "
        "appelle directement l'API Google GetLocalBoqProxy (HTTP pur, sans navigateur) "
        "et met à jour google_reviews + google_reviews_updated_at dans profil_event."
    ),
    default_args=default_args,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    schedule_interval=None,  # Passe à '@daily' pour un run automatique quotidien
    tags=["event", "google", "reviews"],
) as dag:
    dag.timezone = PARIS_TZ

    scrape_and_store_reviews = PythonOperator(
        task_id="scrape_and_store_reviews",
        python_callable=update_google_reviews,
    )

    scrape_and_store_reviews
