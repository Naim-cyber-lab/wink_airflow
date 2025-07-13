# services/RegularActiveEventProcessor.py
from services.connection_database import DatabaseConnector

class RegularActiveEventProcessor:
    def run(self, **context):
        active_event_ids = [1, 2, 3]  # ou récupérés dynamiquement

        with DatabaseConnector('postgres') as db:
            for event_id in active_event_ids:
                db.execute("SELECT update_publication_date_event(%s);", [event_id])
