"""
Create some simple postgres queries using python.
"""
from __future__ import annotations
from typing import List, Tuple
import os
import json
import pathlib
import datetime
from argparse import ArgumentParser

import psycopg2
import pandas as pd

from rtree import index
import shapefile
from shapely.geometry import Polygon, MultiPolygon
from shapely.wkt import loads

POSTGIS_HOST = os.environ["PGHOST"]
POSTGIS_PORT = os.environ["PGPORT"]
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
    result["waterpercentage"] = []
    result["footprint"] = []
    for name in result["r_table_name"]:
        datestr = name.split("_")[1]
        date = datetime.datetime.strptime(datestr, "%Y%m%dt%H%M%S")
        result["date"].append(date)
        meta = tci_meta[name]
        result["cloudcover"].append(meta["cloudcoverpercentage"])
        result["snowcover"].append(meta["snowicepercentage"])
        result["waterpercentage"].append(meta["waterpercentage"])
        result["footprint"].append(meta["footprint"])
    return pd.DataFrame.from_dict(result)


class Corine:
    def __init__(
            self,
            gs: Geospatial = None,
            index_name: str = "corine",
            force_reload: bool = False,
            dbname: str = "corinagermanydata"):
        self._dbname = dbname
        if not pathlib.Path(f"{index_name}.idx").exists() or force_reload:
            self._idx = index.Index(index_name)
            self._load_corine(gs, index_name)
        else:
            with open(f"{index_name}.json", "r") as handle:
                self._corine = json.load(handle)
            self._corine["shapes"] = [
                loads(p) for p in self._corine["polygon"]]
            self._idx = index.Index(index_name)

    def _load_corine(self, gs: Geospatial, index_name):
        self._corine = gs.query(
            f"""SELECT id, code_18, ST_AsText(ST_Transform(geom, 4326))
            FROM {self._dbname}""", ("id", "code_18", "polygon"))
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


def export_images_dataset(outdir, corinedb, metafile):
    outdir = pathlib.Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    name = str(outdir.name)

    gs = Geospatial(
        POSTGIS_HOST, port=POSTGIS_PORT,
        password=POSTGIS_PASSWORD, user=POSTGIS_USER)

    cori = Corine(gs, dbname=corinedb)
    dataset = get_raster_tables(gs, metafile)
    failed = []
    filtered_out = 0

    shp = shapefile.Writer(f"{name}.shp")
    shp.field("name", "C")
    shp.field("year", "N")
    shp.field("type", "C")
    shp.field("ratio", "N")

    for _, row in dataset.iterrows():
        name = row["r_table_name"]
        if row["cloudcover"] > 1.0:
            filtered_out += 1
            continue
        if row["snowcover"] > 1.0:
            filtered_out += 1
            continue
        if row["waterpercentage"] > 80.0:
            filtered_out += 1
            continue
        query = gs.query_iterator(
            f"""SELECT rid, ST_AsPNG(rast), ST_AsText(ST_Transform(ST_Envelope(rast), 4326))
            FROM {name} WHERE
            ST_Intersects(ST_Transform(ST_Envelope(rast), 4326), ST_GeometryFromText('SRID=4326;{row['footprint']}'))
            AND
            ST_Width(rast) = 120 AND ST_Height(rast) = 120
            """,
            ("rid", "png", "geom")
        )
        tile_metadata = []
        for rast in query:
            print(rast)
            tile_name = f"{name}_T{rast['rid']}"
            shape = loads(rast["geom"])
            corine_classes = cori.intersect(shape)
            if corine_classes:
                summed_ratios = collections.defaultdict(int)
                for _, corine_class, ratio in corine_classes:
                    summed_ratios[corine_class] += ratio
                highest_key = max(summed_ratios, key=lambda c: summed_ratios[c])
                highest_class = ("", highest_key, summed_ratios[highest_key])
                print(highest_class, corine_classes)
                filedir = outdir / str(row["date"].year) / highest_class[1]
                filedir.mkdir(parents=True, exist_ok=True)
                filepath = filedir / f"{tile_name}_p{highest_class[2]:.2f}.png"
                with open(str(filepath), "wb") as handle:
                    handle.write(rast["png"])
                tile_metadata.append(
                    {
                        "name": tile_name,
                        "geom": rast["geom"],
                        "date": row["date"].isoformat(),
                        "snowcover": row["snowcover"],
                        "cloudcover": row["cloudcover"],
                        "corine_classes": corine_classes,
                        "max_class": highest_class,
                    }
                )
            else:
                highest_class = ("", "", 1.0)
                failed.append({"name": tile_name, "geom": rast["geom"]})

            if isinstance(shape, MultiPolygon):
                coords = [list(poly.exterior.coords) for poly in shape.geoms]
            else:
                coords = [list(shape.exterior.coords)]
            shp.poly(coords)
            shp.record(
                tile_name,
                row["date"].year,
                highest_class[1],
                highest_class[2],
            )
            print("--------")

        with open(str(outdir / f"{name}.json"), "w") as handle:
            json.dump(tile_metadata, handle)

        with open(str(outdir / f"{name}_failed.json"), "w") as handle:
            json.dump(failed, handle)

    shp.close()

    print(filtered_out)
    cori.close()
    gs.close()


def main(args):
    export_images_dataset(args.output, args.corine, args.metadata)


if __name__ == "__main__":
    PARSER = ArgumentParser()
    PARSER.add_argument("output", help="Output dataset directory")
    PARSER.add_argument(
        "--metadata", default="metadata.json", help="Tile metadata json")
    PARSER.add_argument(
        "--corine", default="corinagermanydata", help="Corine data table")
    main(PARSER.parse_args())
