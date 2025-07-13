from airflow.providers.postgres.hooks.postgres import PostgresHook

class DatabaseConnector:
    def __init__(self, conn_id: str = 'postgres'):
        self.hook = PostgresHook(postgres_conn_id=conn_id)
        self.conn = None
        self.cursor = None

    def __enter__(self):
        self.conn = self.hook.get_conn()
        self.cursor = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.conn.close()

    def fetchall(self, query: str, params=None):
        self.cursor.execute(query, params or [])
        return self.cursor.fetchall()

    def fetchone(self, query: str, params=None):
        self.cursor.execute(query, params or [])
        return self.cursor.fetchone()

    def execute(self, query: str, params=None):
        self.cursor.execute(query, params or [])
        self.conn.commit()
