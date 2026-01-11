import logging
import os
import time
import urllib.parse
from datetime import timedelta
from zoneinfo import ZoneInfo

import psycopg2
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

PARIS_TZ = ZoneInfo("Europe/Paris")
DB_CONN_ID = "my_postgres"


def safe_str(x) -> str:
    return ("" if x is None else str(x)).strip()


def get_google_reviews_url_with_playwright(query: str) -> str | None:
    """
    Ouvre Google, fait une recherche et tente de cliquer sur "Avis".
    Retourne l'URL résultante.
    """
    q = query.strip()
    if not q:
        return None

    search_url = "https://www.google.com/search?q=" + urllib.parse.quote_plus(q)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            locale="fr-FR",
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1365, "height": 768},
        )
        page = context.new_page()

        try:
            page.goto(search_url, wait_until="domcontentloaded", timeout=30000)

            # Consentement cookies (selon régions)
            for selector in [
                'button:has-text("Tout accepter")',
                'button:has-text("Accepter tout")',
                'button:has-text("J’accepte")',
                'button:has-text("Accepter")',
                'text="J’accepte"',
            ]:
                try:
                    page.locator(selector).first.click(timeout=1500)
                    break
                except Exception:
                    pass

            # Attendre que la page soit vraiment chargée
            page.wait_for_timeout(1500)

            # En FR, le bouton/onglet peut être "Avis" ou "Avis Google"
            # On tente plusieurs patterns, en visant le knowledge panel / résultats locaux.
            candidates = [
                'a:has-text("Avis")',
                'span:has-text("Avis")',
                'a:has-text("Avis Google")',
                'span:has-text("Avis Google")',
                'a[aria-label*="Avis"]',
                'a[aria-label*="avis"]',
            ]

            clicked = False
            for sel in candidates:
                loc = page.locator(sel)
                try:
                    if loc.count() > 0:
                        # On clique le premier visible
                        loc.first.scroll_into_view_if_needed(timeout=2000)
                        loc.first.click(timeout=4000)
                        clicked = True
                        break
                except Exception:
                    continue

            if not clicked:
                return None

            # Laisse le temps de navigation / changement d’URL
            try:
                page.wait_for_load_state("domcontentloaded", timeout=10000)
            except PWTimeoutError:
                pass

            page.wait_for_timeout(500)
            final_url = page.url

            # IMPORTANT : cette URL peut être "longue" et variable.
            # Si tu veux quand même quelque chose de plus stable, tu peux nettoyer.
            return final_url

        finally:
            context.close()
            browser.close()


def update_events_reviews_url(**context):
    conn = BaseHook.get_connection(DB_CONN_ID)
    connection = psycopg2.connect(
        host=conn.host,
        port=conn.port,
        user=conn.login,
        password=conn.password,
        dbname=conn.schema,
    )
    cursor = connection.cursor()

    updated = 0
    skipped = 0

    try:
        # ✅ On ne prend que ceux dont urlGoogleMapsAvis est vide
        cursor.execute(
            """
            SELECT id, titre, adresse, city
            FROM profil_event
            WHERE COALESCE("urlGoogleMapsAvis",'') = ''
            ORDER BY id ASC
            """
        )
        rows = cursor.fetchall()
        logging.info("Events à enrichir (urlGoogleMapsAvis vide) : %s", len(rows))

        for event_id, titre, adresse, city in rows:
            titre_s = safe_str(titre)
            if not titre_s:
                skipped += 1
                continue

            query = " ".join([p for p in [titre_s, safe_str(adresse), safe_str(city)] if p]).strip()

            # C’est mieux d’orienter la recherche directement vers "Avis"
            query_for_reviews = f"{query} Avis"

            url = get_google_reviews_url_with_playwright(query_for_reviews)

            if not url:
                skipped += 1
                continue

            # ✅ sécurité: update seulement si toujours vide au moment de l’update
            cursor.execute(
                """
                UPDATE profil_event
                SET "urlGoogleMapsAvis" = %s
                WHERE id = %s
                  AND COALESCE("urlGoogleMapsAvis",'') = ''
                """,
                (url, int(event_id)),
            )

            if cursor.rowcount == 1:
                updated += 1
            else:
                skipped += 1

            if updated % 50 == 0:
                connection.commit()
                logging.info("Commit: updated=%s skipped=%s", updated, skipped)

            # éviter de se faire throttler (Google n’aime pas les rafales)
            time.sleep(1.0)

        connection.commit()
        logging.info("DONE updated=%s skipped=%s", updated, skipped)

    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()


default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="enrich_events_google_reviews_url_playwright",
    start_date=days_ago(1),
    schedule_interval="30 3 * * *",
    catchup=False,
    default_args=default_args,
    tags=["event", "google", "reviews", "playwright"],
) as dag:
    dag.timezone = PARIS_TZ

    PythonOperator(
        task_id="update_urlGoogleMapsAvis_if_empty",
        python_callable=update_events_reviews_url,
        provide_context=True,
    )
