import json
import logging
import re
import time
from datetime import timedelta
from urllib.parse import parse_qs, urlparse

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

# Headers qui simulent un vrai XHR depuis Google Search.
# X-Same-Domain et X-Goog-Encode-Response-If-Executable sont obligatoires
# pour que l'API accepte la requête (sinon -> 400).
BASE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "fr-FR,fr;q=0.9",
    "X-Same-Domain": "1",
    "X-Goog-Encode-Response-If-Executable": "base64",
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
    Recupere les events ayant :
      - urlGoogleMapsAvis renseignee
      - le parametre `stick` ET `rldimm` dans l'URL (les deux sont requis par l'API)

    Ordre : jamais scrappe en premier, puis les plus anciens.
    """
    sql = """
        SELECT id, "urlGoogleMapsAvis"
        FROM profil_event
        WHERE "urlGoogleMapsAvis" IS NOT NULL
          AND "urlGoogleMapsAvis" <> ''
          AND "urlGoogleMapsAvis" LIKE '%%stick=%%'
          AND "urlGoogleMapsAvis" LIKE '%%rldimm=%%'
        ORDER BY
            google_reviews_updated_at ASC NULLS FIRST,
            id ASC
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
    """Sauvegarde les avis et la date de mise a jour dans profil_event."""
    sql = """
        UPDATE profil_event
        SET google_reviews            = %s,
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
def extract_url_params(url: str) -> tuple[str | None, str, str | None]:
    """
    Extrait depuis l'URL Google Search :
      - stick  : identifiant encode du lieu (obligatoire)
      - hl     : langue (defaut fr-FR)
      - rldimm : identifiant numerique du lieu OBLIGATOIRE pour que
                 GetLocalBoqProxy accepte la requete (sinon -> 400 Bad Request)

    Exemple d'URL valide :
      https://www.google.com/search?...&stick=H4sI...&rldimm=11800228875880652455&...
    """
    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=True)

    stick  = params.get("stick",  [None])[0]
    hl     = params.get("hl",     ["fr-FR"])[0]
    rldimm = params.get("rldimm", [None])[0]

    return stick, hl, rldimm


# ================== SCRAPER (HTTP pur) ==================
def _parse_reviews_from_body(body: str) -> list[dict]:
    """
    Parse la reponse JSON de l'endpoint GetLocalBoqProxy.
    Structure : data[1][10][2] = liste des avis
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
    Recupere les avis Google via des appels HTTP directs a GetLocalBoqProxy.

    Parametres envoyes a l'API :
      - hl     : langue
      - stick  : identifiant du lieu (encode base64/protobuf)
      - rldimm : identifiant numerique du lieu REQUIS pour eviter le 400

    Le Referer est positionne dynamiquement sur l'URL d'origine pour
    que Google considere la requete comme legitime.
    """
    stick, hl, rldimm = extract_url_params(url)

    if not stick:
        raise ValueError(f"Parametre 'stick' introuvable dans l'URL : {url}")
    if not rldimm:
        raise ValueError(f"Parametre 'rldimm' introuvable dans l'URL : {url}")

    # Le Referer doit pointer vers la page Google Search d'origine
    headers = {**BASE_HEADERS, "Referer": url}

    all_reviews: list[dict] = []
    seen: set[tuple]        = set()
    next_token: str | None  = None
    page_num = 0

    while len(all_reviews) < max_reviews:
        page_num += 1

        # rldimm est obligatoire des la premiere page pour valider la requete
        api_params: dict = {
            "hl":     hl,
            "stick":  stick,
            "rldimm": rldimm,
        }
        if next_token:
            api_params["pageToken"] = next_token

        resp = requests.get(API_URL, params=api_params, headers=headers, timeout=15)
        resp.raise_for_status()
        body = resp.text

        try:
            batch = _parse_reviews_from_body(body)
        except Exception as exc:
            logging.warning("Page %d : erreur de parsing (%s)", page_num, exc)
            break

        if not batch:
            logging.info("Page %d : aucun avis, fin de pagination.", page_num)
            break

        added = 0
        for r in batch:
            key = (r["author"], r["date"])
            if key not in seen:
                seen.add(key)
                all_reviews.append(r)
                added += 1

        logging.info("Page %d : +%d avis (total %d)", page_num, added, len(all_reviews))

        next_token = _get_next_token(body)
        if not next_token:
            logging.info("Page %d : plus de token de pagination, arret.", page_num)
            break

        if len(all_reviews) < max_reviews:
            time.sleep(delay)

    return all_reviews[:max_reviews]


# ================== AIRFLOW TASK ==================
def update_google_reviews(**_context):
    """
    Parcourt les events de profil_event ayant stick ET rldimm dans leur URL.

    Pour chaque event :
      1. Extrait stick + rldimm de l'URL
      2. Appelle GetLocalBoqProxy (HTTP pur) avec les deux parametres
      3. Pagine pour recuperer jusqu'a max_reviews avis
      4. Met a jour google_reviews + google_reviews_updated_at dans profil_event
    """
    limit       = int(Variable.get("GOOGLE_REVIEWS_BATCH_LIMIT",   default_var="200"))
    max_reviews = int(Variable.get("GOOGLE_REVIEWS_MAX_PER_EVENT", default_var="20"))
    delay       = float(Variable.get("GOOGLE_REVIEWS_DELAY_SEC",   default_var="1.0"))

    rows = fetch_events_to_scrape(limit=limit)

    if not rows:
        logging.info("Aucun evenement eligible (URLs sans 'stick' ou sans 'rldimm').")
        return

    logging.info(
        "%d evenement(s) a scraper (max %d avis chacun, delai %.1fs entre pages)",
        len(rows), max_reviews, delay,
    )

    processed = skipped = failed = 0

    for event_id, url in rows:
        try:
            stick, _, rldimm = extract_url_params(url)

            if not stick or not rldimm:
                skipped += 1
                logging.warning(
                    "Event %s : stick=%s rldimm=%s — parametre manquant, ignore.",
                    event_id, stick, rldimm,
                )
                continue

            reviews = scrape_reviews(url, max_reviews, delay=delay)

            # Sauvegarde meme si vide pour horodater et eviter de re-tenter en boucle
            update_event_reviews_in_db(event_id, reviews)

            if reviews:
                processed += 1
                logging.info("Event %s : %d avis sauvegardes.", event_id, len(reviews))
            else:
                skipped += 1
                logging.warning(
                    "Event %s : aucun avis recupere, table mise a jour avec [].", event_id
                )

        except Exception as exc:
            failed += 1
            logging.exception("Event %s : echec scraping (%s)", event_id, exc)

    logging.info(
        "Termine — processed=%d  skipped=%d  failed=%d",
        processed, skipped, failed,
    )


# ================== DAG ==================
with DAG(
    dag_id="profil_event_update_google_reviews",
    description=(
        "Met a jour google_reviews dans profil_event pour tous les events "
        "ayant stick + rldimm dans urlGoogleMapsAvis. "
        "Priorite aux events jamais scrappe, puis les plus anciens."
    ),
    default_args=default_args,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    schedule_interval="@daily",
    tags=["event", "google", "reviews"],
) as dag:
    dag.timezone = PARIS_TZ

    scrape_and_store_reviews = PythonOperator(
        task_id="scrape_and_store_reviews",
        python_callable=update_google_reviews,
    )

    scrape_and_store_reviews
