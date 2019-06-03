"""
Create some simple postgres queries using python.
"""
from typing import List
import os
import json
import datetime

import psycopg2
import pandas as pd


POSTGIS_USER = os.environ["PGUSER"]
POSTGIS_PASSWORD = os.environ["PGPASSWORD"]


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

    def close(self):
        self.cur.close()
        self.conn.close()

    def __enter__(self):
        print("Entering")

    def __exit__(self, *_):
        self.conn.commit()


def get_raster_tables(gs: Geospatial, metadata: str) -> pd.DataFrame:
    result = gs.query(
        """SELECT r_table_name, ST_AsText(ST_Transform(extent, 4326))
        FROM raster_columns""",
        ("r_table_name", "bound")
    )

    with open(metadata) as mfile:
        metas = json.load(mfile)

    tci_meta = {}
    for meta in metas:
        tci_name = meta["tciname"].rstrip(".jp2").lower()
        tci_meta[tci_name] = meta

    result["date"] = []
    result["cloudcover"] = []
    result["snowcover"] = []
    for name in result["r_table_name"]:
        datestr = name.split("_")[1]
        date = datetime.datetime.strptime(datestr, "%Y%m%dt%H%M%S")
        result["date"].append(date)
        meta = tci_meta[name]
        result["cloudcover"].append(meta["cloudcoverpercentage"])
        result["snowcover"].append(meta["snowicepercentage"])
    return pd.DataFrame.from_dict(result)


def main():
    gs = Geospatial(
        "home.arsbrevis.de", port=31313,
        password=POSTGIS_PASSWORD, user=POSTGIS_USER)

    dataset = get_raster_tables(gs, "metadata.json")
    print(dataset)

    data = gs.query(
        """SELECT rid, ST_AsPNG(rast), ST_AsText(ST_Transform(ST_Envelope(rast), 4326))
        FROM t31tgn_20180925t104021_tci_10m LIMIT 1""",
        ("rid", "png", "geom")
    )
    picmemory = data["png"][0]
    with open("test.png", "wb") as f:
        f.write(picmemory)

    gs.close()


if __name__ == "__main__":
    main()
