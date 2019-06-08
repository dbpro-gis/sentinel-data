"""
Create some simple postgres queries using python.
"""
from __future__ import annotations
from typing import List, Tuple
import os
import json
import pathlib
import datetime

import psycopg2
import pandas as pd

from rtree import index
from shapely.geometry import Polygon
from shapely.wkt import loads

# TODO: VISUALIZATION: FETCH IMAGES WITH JAVASCRIPT

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


class Corine:
    def __init__(
            self,
            gs: Geospatial = None,
            index_name: str = "corine",
            force_reload: bool = False):
        # self._idx = index.Index(index_name)
        self._idx = index.Index()
        if not pathlib.Path(f"{index_name}.idx").exists() or force_reload:
            self._load_corine(gs)

    def _load_corine(self, gs: Geospatial):
        self._corine = gs.query(
            """SELECT id, code_18, ST_AsText(ST_Transform(geom, 4326))
            FROM corinagermanydata""", ("id", "code_18", "polygon"))
        self._corine["shapes"] = []

        for i, corine_id in enumerate(self._corine["id"]):
            poly = loads(self._corine["polygon"][i])
            self._idx.insert(i, poly.bounds)
            self._corine["shapes"].append(poly)

    def close(self):
        self._idx.close()

    def intersect(self, shape: Polygon) -> List[Tuple[str, str, float]]:
        """Intersect the given shape against the corine dataset and return
        list of labels and their area ratios.
        """
        intersections = []
        shape_area = shape.area
        # GENERAL INTERSECTION WITH BOUNDS
        for is_id in self._idx.intersection(shape.bounds):
            corine_shape = self._corine["shapes"][is_id]
            is_area = shape.intersection(corine_shape).area
            if is_area > 0:
                ratio = is_area / shape_area
                intersections.append(
                    (
                        self._corine["id"][is_id],
                        self._corine["code_18"][is_id],
                        ratio
                    )
                )

        return intersections


def export_images_dataset(outdir):
    outdir = pathlib.Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    gs = Geospatial(
        "home.arsbrevis.de", port=31313,
        password=POSTGIS_PASSWORD, user=POSTGIS_USER)

    cori = Corine(gs)
    # dataset = get_raster_tables(gs, "metadata.json")

    # data = gs.query(
    #     """SELECT rid, ST_AsPNG(rast), ST_AsText(ST_Transform(ST_Envelope(rast), 4326))
    #     FROM t31tgn_20180925t104021_tci_10m LIMIT 1""",
    #     ("rid", "png", "geom")
    # )
    # picmemory = data["png"][0]
    # with open("test.png", "wb") as f:
    #     f.write(picmemory)

    cori.close()
    gs.close()


def main():
    export_images_dataset("sentinel-dataset")



if __name__ == "__main__":
    main()
