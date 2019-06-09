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

    def query_iterator(self, query, columns):
        self.cur.execute(query)
        while True:
            result = self.cur.fetchone()
            if result is None:
                break
            yield dict(zip(columns, result))

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
        if not pathlib.Path(f"{index_name}.idx").exists() or force_reload:
            self._idx = index.Index(index_name)
            self._load_corine(gs, index_name)
        else:
            with open(f"{index_name}.json", "r") as handle:
                self._corine = json.load(handle)
            self._corine["shapes"] = [loads(p) for p in self._corine["polygon"]]
            self._idx = index.Index(index_name)

    def _load_corine(self, gs: Geospatial, index_name):
        self._corine = gs.query(
            """SELECT id, code_18, ST_AsText(ST_Transform(geom, 4326))
            FROM corinagermanydata""", ("id", "code_18", "polygon"))
        with open(f"{index_name}.json", "w") as handle:
            json.dump(self._corine, handle)
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
        "127.0.0.1", port=31313,
        password=POSTGIS_PASSWORD, user=POSTGIS_USER)

    cori = Corine(gs)
    dataset = get_raster_tables(gs, "metadata.json")
    failed = []
    for _, row in dataset.iterrows():
        name = row["r_table_name"]
        print(row)
        query = gs.query_iterator(
            f"""SELECT rid, ST_AsPNG(rast), ST_AsText(ST_Transform(ST_Envelope(rast), 4326))
            FROM {name}""",
            ("rid", "png", "geom")
        )
        tile_metadata = []
        for rast in query:
            print(rast)
            tile_name = f"{name}_T{rast['rid']}"
            shape = loads(rast["geom"])
            corine_classes = cori.intersect(shape)
            if corine_classes:
                print(corine_classes)
                filepath = outdir / f"{tile_name}.png"
                if not filepath.exists():
                    with open(str(filepath), "wb") as handle:
                        handle.write(rast["png"])
                tile_metadata.append(
                    {
                        "name": tile_name,
                        "geom": rast["geom"],
                        "date": row["date"],
                        "snowcover": row["snowcover"],
                        "cloudcover": row["cloudcover"],
                        "corine_classes": corine_classes,
                        "max_class": max(corine_classes, key=lambda c: c[2])
                    }
                )
            else:
                failed.append({"name": tile_name, "geom": rast["geom"]})
            print("--------")

        with open(str(outdir / f"{name}.json"), "w") as handle:
            json.dump(tile_metadata, handle)

        with open(str(outdir / "{name}_failed.json"), "w") as handle:
            json.dump(failed, handle)

    cori.close()
    gs.close()


def main():
    export_images_dataset("sentinel-dataset")



if __name__ == "__main__":
    main()
