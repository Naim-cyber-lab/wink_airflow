import asyncio
import json
import logging
import re
from datetime import timedelta
from zoneinfo import ZoneInfo

import psycopg2

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


# ================== CONFIG ==================
PARIS_TZ = ZoneInfo("Europe/Paris")
DB_CONN_ID = "my_postgres"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
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


# ================== SCRAPER ==================
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


async def _scrape_reviews_async(url: str, max_reviews: int) -> list[dict]:
    """
    Lance Playwright, intercepte les réponses GetLocalBoqProxy
    et accumule les avis par scroll jusqu'à max_reviews.
    """
    from playwright.async_api import async_playwright

    all_reviews: list[dict] = []
    seen: set[tuple] = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context(
            locale="fr-FR",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
        )
        # Cookies de consentement pour bypasser consent.google.com
        await context.add_cookies([
            {"name": "CONSENT", "value": "YES+cb.20231020-07-p0.fr+FX+NW",
             "domain": ".google.com", "path": "/"},
            {"name": "SOCS",    "value": "CAESEwgDEgk0OTc5NzMwNzIaAmZyIAEaBgiAnMezBg",
             "domain": ".google.com", "path": "/"},
        ])

        page = await context.new_page()

        async def on_response(response):
            if (
                "GetLocalBoqProxy" not in response.url
                and "PrivateLocalSearch" not in response.url
            ):
                return
            try:
                body = await response.text()
                if "il y a" not in body and "ago" not in body:
                    return
                for r in _parse_reviews_from_body(body):
                    key = (r["author"], r["date"])
                    if key not in seen:
                        seen.add(key)
                        all_reviews.append(r)
            except Exception:
                pass

        page.on("response", on_response)

        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(3)

        stale_count = 0
        prev_count  = 0
        scroll_y    = 0

        while len(all_reviews) < max_reviews:
            scroll_y += 400
            await page.evaluate(f"window.scrollTo(0, {scroll_y})")
            await asyncio.sleep(1.5)

            if len(all_reviews) == prev_count:
                stale_count += 1
            else:
                stale_count = 0
                prev_count  = len(all_reviews)

            # Arrêt si plus rien ne charge après 5 scrolls consécutifs
            if stale_count >= 5:
                break

        await browser.close()

    return all_reviews[:max_reviews]


def scrape_reviews(url: str, max_reviews: int) -> list[dict]:
    """Wrapper synchrone pour appeler le scraper async depuis Airflow."""
    return asyncio.run(_scrape_reviews_async(url, max_reviews))


# ================== AIRFLOW TASK ==================
def update_google_reviews(**_context):
    """
    Parcourt TOUS les events de profil_event où :
      - urlGoogleMapsAvis est renseignée
      - google_reviews est encore vide (jamais scrappé)

    Pour chaque event éligible, scrape les avis Google et met à jour :
      - google_reviews            (JSON texte, liste d'avis)
      - google_reviews_updated_at (timestamp NOW())

    Si aucun avis n'est trouvé pour un event, on écrit quand même '[]'
    et on horodate pour éviter de re-tenter indéfiniment.
    """
    limit       = int(Variable.get("GOOGLE_REVIEWS_BATCH_LIMIT",   default_var="200"))
    max_reviews = int(Variable.get("GOOGLE_REVIEWS_MAX_PER_EVENT", default_var="20"))

    rows = fetch_events_to_scrape(limit=limit)

    if not rows:
        logging.info("Aucun événement à traiter (tous ont déjà des avis ou pas d'URL).")
        return

    logging.info(
        "%d événement(s) à scraper (max %d avis chacun)",
        len(rows), max_reviews,
    )

    processed = skipped = failed = 0

    for event_id, url in rows:
        try:
            reviews = scrape_reviews(url, max_reviews)

            if not reviews:
                # Aucun avis trouvé : on horodate quand même pour ne pas re-tenter
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
        "scrape les avis Google et met à jour google_reviews + google_reviews_updated_at."
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
