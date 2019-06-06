"""
Create some simple postgres queries using python.
"""
from typing import List
import os
import json
import datetime

import psycopg2
import pandas as pd

from rtree import index
from shapely.wkt import loads

idx = index.Index()

# TODO: VISUALIZATION: FETCH IMAGES WITH JAVASCRIPT

#POSTGIS_USER = os.environ["postgres"]
#POSTGIS_PASSWORD = os.environ["dbprogis2019"]

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
    def __init__(self, gs):
        corine = gs.query("""SELECT code_18, ST_Transform(geom, 4326) AS polygon FROM corinagermanydata""")
        idx = index.Index()
        
        for c in corine:            
            poly = loads(c[1]).wkt
            idx.insert(c[0], poly)
            bounds = poly.bounds
   
    def intersect(self, tile):     
        # GENERAL INTERSECTION WITH BOUNDS 
        intersection_list = []
        corine_classes = list(idx.intersection(tile))
        
        # FURTHER INTERSECTION WITH POLYGONS
        for cc in corine_classes
            for c in corine: 
                if(c[0]==cc)
                    poly = loads(c[1]).wkt
                    intersectpart = tile.intersection(poly)
                    poly_area = poly.area
                    intersectpart_area = intersectpart.area
                    fraction = intersectpart_area / poly_area
                    intersection_list.append((c[0], fraction))
                    
        # RETURN LIST WITH TUPLES (CORINECLASS WITH FRACTION IN PERCENTAGE)         
        return intersection_list 
      
        
        
def main():
    gs = Geospatial(
        "home.arsbrevis.de", port=31313,
        password="dbprogis2019", user="postgres")

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
    
    
    
    # INPUT PARAMETER FOR INTERSECTION: CERTAIN TILE (BOUNDING BOX) -> OUTPUT:  LIST WITH CORINE CLASS + PERCENTAGE OF INTERSECTION
    cori = Corine(gs)
    cori.intersect(tile)


if __name__ == "__main__":
    main()
