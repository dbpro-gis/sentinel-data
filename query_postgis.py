"""
Create some simple postgres queries using python.
"""
from typing import List
import os
import psycopg2


POSTGIS_PASSWORD = os.environ["DBPRO_DB_PASSWORD"]


class Geospatial:
    """Geospatial database inside of postgis."""

    def __init__(
            self,
            host,
            port=5432,
            user="postgres",
            password=None,
            dbname="geospatial"):
        self.conn = psycopg2.connect(
            dbname=dbname, user=user, password=password, host=host, port=port)

        self.cur = self.conn.cursor()

    def list_tables(self) -> List[str]:
        """Get a list of tables."""
        self.cur.execute(
            r""" SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'"""
        )
        table_names = [t for t, in self.cur.fetchall()]
        return table_names

    def query(self, query, columns):
        self.cur.execute(query)
        results = {col: [] for col in columns}
        for result in self.cur.fetchall():
            for i, column in enumerate(columns):
                results[column].append(result[i])
        return results

    def __enter__(self):
        print("Entering")

    def __exit__(self, *_):
        self.conn.commit()


def main():
    gs = Geospatial(
        "home.arsbrevis.de", port=31313, password=POSTGIS_PASSWORD)

    result = gs.query(
        """SELECT r_table_name, ST_AsText(ST_Transform(extent, 4326))
        FROM raster_columns""",
        ("r_table_name", "bound")
    )
    for table_name in result["r_table_name"]:
        print(table_name)
        with gs:
            gs.cur.execute(f"DROP TABLE {table_name} CASCADE")
    print(result)


if __name__ == "__main__":
    main()
