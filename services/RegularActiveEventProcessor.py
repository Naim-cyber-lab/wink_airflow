from services.connection_database import db_instance

class RegularActiveEventProcessor:
    def __init__(self):
        print("🔧 Initialisation du processeur d'événements actifs réguliers")

    def run(self, **context):
        print("🎯 Traitement régulier des vidéos actives")
        videos = self.get_new_videos()
        for video in videos:
            event = self.extract_event(video)
            self.save_to_db(event)

    def get_new_videos(self):
        return ["video1.mp4", "video2.mp4"]

    def extract_event(self, video):
        return {"video": video, "titre": "Événement associé"}

    def save_to_db(self, event):
        print("Insertion en BDD :", event)
        with db_instance.get_session() as session:
            session.execute(
                "INSERT INTO events (video, titre) VALUES (:video, :titre)",
                {"video": event["video"], "titre": event["titre"]}
            )
            session.commit()
