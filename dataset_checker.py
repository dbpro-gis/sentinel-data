"""Check sentinel dataset"""
from __future__ import annotations
import collections
import pathlib
import shutil
import json
import datetime
from typing import Tuple


import shapefile
from shapely.wkt import loads


class Patch:
    def __init__(self, meta, path):
        if isinstance(meta["date"], str):
            meta["date"] = datetime.datetime.fromisoformat(meta["date"])
        self._meta = meta
        self._path = path

    @property
    def name(self):
        return self._meta["name"]

    @property
    def path(self):
        return f"{self._path}/{self.name}.png"

    @property
    def geom(self):
        return loads(self._meta["geom"])

    @property
    def label(self):
        """
        Returns:
            (id, corine_class, percentage)
        """
        return self._meta["max_class"]


class Tile:
    def __init__(self, meta, patches, path=None, name=None):
        self._path = path
        self.name = name
        self.meta = meta
        self.patches = patches

    @classmethod
    def from_path(cls, metapath, **kwargs):
        name = metapath.stem

        # Metadata format:
        # [{name, geom, date, snowcover, cloudcover, corine_classes, max_class}]
        with open(metapath) as handle:
            patch_metas = json.load(handle)

        if patch_metas:
            meta = patch_metas[0]
            if "date" in meta:
                meta["date"] = datetime.datetime.fromisoformat(meta["date"])
                patches = [Patch(m, kwargs["path"]) for m in patch_metas]
            else:
                print(meta)
                patches = []
        else:
            meta = {}
            patches = []
        return cls(meta, patches, **kwargs)

    @property
    def date(self):
        return self.meta.get("date", None)

    @property
    def snowcover(self):
        return self.meta.get("snowcover", None)

    @property
    def cloudcover(self):
        return self.meta.get("cloudcover", None)

    def filter(self, label=None, percentage=None):
        patch_metas = self.patches.copy()
        if label is not None:
            patch_metas = [p for p in patch_metas if p.label[1] == label]

        if percentage is not None:
            patch_metas = [p for p in patch_metas if p.label[2] >= percentage]
        return self.__class__(self.meta, patch_metas, name=self.name, path=self._path)

    def __len__(self):
        return len(self.patches)

    def __repr__(self):
        return f"<Tile | {len(self)} Patches>"


class Dataset:
    def __init__(self, tiles, filterargs=()):
        self.tiles = tiles
        self.filterargs = filterargs

    @classmethod
    def from_dir(cls, path):
        path = pathlib.Path(path)
        metafiles = list(path.glob("*.json"))
        tiles = [Tile.from_path(metapath=m, path=path) for m in metafiles]
        return cls(tiles)

    def filter(self, **filterarg):
        tiles = self.tiles.copy()

        remove_empty = filterarg.get("remove_empty", False)
        if remove_empty:
            tiles = [t for t in tiles if len(t) > 0 or t.date is None or not t.label]

        date = filterarg.get("min_date", None)
        if date is not None:
            date = datetime.date.fromisoformat(date)
            tiles = [t for t in tiles if t.date is not None and t.date >= date]

        date = filterarg.get("max_date", None)
        if date is not None:
            date = datetime.date.fromisoformat(date)
            tiles = [t for t in tiles if t.date is not None and t.date <= date]

        snowcover = filterarg.get("snowcover", None)
        if snowcover is not None:
            tiles = [t for t in tiles if t.snowcover is not None and t.snowcover <= snowcover]

        cloudcover = filterarg.get("cloudcover", None)
        if cloudcover is not None:
            tiles = [t for t in tiles if t.cloudcover is not None and t.cloudcover <= cloudcover]

        geom = filterarg.get("geom", None)
        if geom is not None:
            raise NotImplementedError

        return self.__class__(tiles, filterargs=[*self.filterargs, filterarg])

    def __repr__(self):
        return f"<Dataset | {len(self.tiles)}>"


from shapely.geometry import MultiPolygon


def export_data(dataset, output_path):
    output_path = pathlib.Path(output_path)
    with shapefile.Writer(output_path / "2018.shp") as shp_2018:
        shp_2018.field("name", "C")
        with shapefile.Writer(output_path / "2019.shp") as shp_2019:
            shp_2019.field("name", "C")
            for tile in dataset.tiles:
                if tile.date.year == 2018:
                    shp = shp_2018
                else:
                    shp = shp_2019
                for patch in tile.patches:
                    folder = output_path / str(tile.date.year) / str(patch.label[1])
                    folder.mkdir(parents=True, exist_ok=True)
                    outpath = folder / f"{patch.name}.png"
                    print("Save", outpath)
                    shutil.copy(patch.path, outpath)

                    polys = patch.geom
                    if isinstance(polys, MultiPolygon):
                        coords = [list(poly.exterior.coords) for poly in polys.geoms]
                    else:
                        coords = [list(polys.exterior.coords)]
                    shp.poly(coords)
                    shp.record(patch.name)


def main():
    dataset_path = pathlib.Path("sentinel-dataset")
    dataset = Dataset.from_dir(dataset_path)
    low_cloud = dataset.filter(remove_empty=True, cloudcover=0.1, snowcover=0.1)

    years = collections.Counter([t.date.year for t in low_cloud.tiles if t.date is not None])
    print(years)

    high_percentage_labels = Dataset(
        [t.filter(percentage=0.75) for t in low_cloud.tiles], low_cloud.filterargs)
    print(high_percentage_labels)

    num_patches = sum(len(t) for t in high_percentage_labels.tiles)
    count_classes = collections.Counter(
        p.label[1] for t in high_percentage_labels.tiles for p in t.patches)
    print(num_patches)
    print("\n".join(f"{k},{v}" for k, v in count_classes.items()))
    print("-----")

    export_data(high_percentage_labels, "germany-dataset")


if __name__ == "__main__":
    main()
