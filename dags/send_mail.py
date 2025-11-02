from zoneinfo import ZoneInfo
from datetime import datetime
import logging
import traceback

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ───────────── Envoi email (Mailjet) ─────────────
def envoyer_mail(destinataire: str, sujet: str, corps: str):
    print(f"📧 Préparation de l'envoi d'un email à : {destinataire}")
    try:
        from mailjet_rest import Client
    except Exception as e:
        print("⚠️ mailjet_rest non installé :", str(e))
        traceback.print_exc()
        return

    API_KEY = Variable.get("MAILJET_API_KEY", default_var="3ce0be795c018accb4374777d5f8c1b0")
    API_SECRET = Variable.get("MAILJET_API_SECRET", default_var="24498e4564332925daa27dbab6798359")

    try:
        mailjet = Client(auth=(API_KEY, API_SECRET), version='v3.1')
        data = {
            "Messages": [{
                "From": {"Email": "nisuapp@gmail.com", "Name": "NisuApp"},
                "To": [{"Email": destinataire}],
                "Subject": sujet,
                "TextPart": corps,
            }]
        }
        result = mailjet.send.create(data=data)
        print(f"📬 Status code: {result.status_code}")
        if result.status_code == 200:
            print("✅ Mail envoyé avec succès")
        else:
            print(f"❌ Échec de l'envoi : {result.status_code} - {result.json()}")
    except Exception as e:
        print("⚠️ Exception lors de l'envoi du mail :", str(e))
        traceback.print_exc()

# ───────────── Utilitaires ─────────────
PARIS = ZoneInfo("Europe/Paris")

def _format_date_fr(dt: datetime) -> str:
    dtp = dt.astimezone(PARIS)
    jours = ["lundi","mardi","mercredi","jeudi","vendredi","samedi","dimanche"]
    mois  = ["janvier","février","mars","avril","mai","juin","juillet","août","septembre","octobre","novembre","décembre"]
    return f"{jours[dtp.weekday()]} {dtp.day} {mois[dtp.month-1]} {dtp.year} à {dtp:%H:%M}"

def _horizon_texte(now_dt: datetime, start_dt: datetime) -> str:
    delta = (start_dt - now_dt).total_seconds()
    if delta <= 0:
        return "très bientôt"
    hours = int(delta // 3600)
    days = hours // 24
    if days >= 1:
        return f"dans {days} jour{'s' if days > 1 else ''}"
    return f"dans {hours} heure{'s' if hours > 1 else ''}"

def _build_subject(title: str, start_at: datetime) -> str:
    return f"[Rappel] {title} — {start_at.astimezone(PARIS):%d/%m/%Y %H:%M}"

def _build_body(title: str, start_at: datetime) -> str:
    when_txt = _format_date_fr(start_at)
    horizon = _horizon_texte(datetime.now(PARIS), start_at)
    return (
        f"Bonjour,\n\n"
        f"Rappel : la rencontre « {title} » commence {horizon}, le {when_txt}.\n\n"
        f"Si vous ne pouvez finalement pas venir, merci de prévenir tout le monde AVANT le début "
        f"de la rencontre afin d'ajuster l'organisation.\n\n"
        f"À très vite,\nL'équipe NisuApp"
    )

# ───────────── Requête Postgres ─────────────
# Sélectionne les conversations dont la date est > maintenant et ≤ maintenant + 3 jours (fuseau Paris)
SQL_UPCOMING_LT_3D = """
WITH bounds AS (
  SELECT
    (NOW() AT TIME ZONE 'Europe/Paris') AS nowp,
    (NOW() AT TIME ZONE 'Europe/Paris') + INTERVAL '3 days' AS maxp
)
SELECT
  c.id AS conversation_id,
  c."date" AS start_at,
  COALESCE(c.title, 'Votre rencontre') AS title,
  w.email AS email
FROM profil_conversationactivity c
JOIN profil_participantconversationactivity p
  ON p."conversationActivity_id" = c.id
JOIN profil_winker w
  ON w.id = p."winker_id"
CROSS JOIN bounds b
WHERE w.email IS NOT NULL AND w.email <> ''
  AND (c."date" AT TIME ZONE 'Europe/Paris') > b.nowp
  AND (c."date" AT TIME ZONE 'Europe/Paris') <= b.maxp;
"""

def send_all_reminders(**context):
    hook = PostgresHook(postgres_conn_id="my_postgres")
    rows = hook.get_records(SQL_UPCOMING_LT_3D)

    if not rows:
        logging.info("Aucune rencontre à rappeler (prochaines < 3 jours).")
        return

    sent = 0
    for conv_id, start_at, title, email in rows:
        print(f"Préparation envoi rappel conv {conv_id} à {email}")
        try:
            subject = _build_subject(title, start_at)
            body = _build_body(title, start_at)
            logging.info(f"Envoi rappel conv {conv_id} -> {email}")
            # envoyer_mail(email, subject, body)
            envoyer_mail("naim.souni1789@outlook.fr", subject, body)
            sent += 1
        except Exception as e:
            logging.exception(f"Echec envoi à {email} (conv {conv_id}) : {e}")

    logging.info(f"{sent} e-mails envoyés.")

# ───────────── DAG ─────────────
default_args = {
    "start_date": datetime(2023, 1, 1, tzinfo=PARIS),
    "retries": 0,
}

with DAG(
    dag_id="reminders_conversations_next_lt_3_days",
    schedule="0 10 * * *",
    default_args=default_args,
    catchup=False,
    tags=["reminder", "email", "conversations"],
) as dag:

    send_reminders = PythonOperator(
        task_id="send_upcoming_conversation_reminders",
        python_callable=send_all_reminders,
    )
