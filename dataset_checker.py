"""Check sentinel dataset"""
import collections
import pathlib
import json
import datetime


class Patch:
    def __init__(self, meta):
        if isinstance(meta["date"], str):
            meta["date"] = datetime.datetime.fromisoformat(meta["date"])
        self._meta = meta


class Tile:
    def __init__(self, meta, patch_metas, path=None, filterargs=None, name=None):
        self._path = path
        self.name = name
        self.meta = meta
        self.patch_metas = patch_metas
        self._filter = filterargs

    @classmethod
    def from_path(cls, metapath, **kwargs):
        name = metapath.stem

        # Metadata format:
        # [{name, geom, date, snowcover, cloudcover, corine_classes, max_class}]
        with open(metapath) as handle:
            patch_metas = json.load(handle)

        if patch_metas:
            meta = patch_metas[0]
            if "date" in self.meta:
                meta["date"] = datetime.datetime.fromisoformat(self.meta["date"])
            else:
                print(meta)
        else:
            meta = {}
        return cls(meta, patch_metas, **kwargs)

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
        patch_metas = self.patch_metas.copy()
        if label is not None:
            patch_metas = [p for p in patch_metas p["max_class"][1] == label]

        if percentage is not None:
            patch_metas = [p for p in patch_metas p["max_class"][2] >= percentage]
        return self.__class__(patch_metas)

    def __len__(self):
        return len(self.patch_metas)

    def __repr__(self):
        return f"<Tile | {len(self.patch_metas)} Patches>"


class Dataset:
    def __init__(self, tiles, filterargs=()):
        self.tiles = tiles
        self._filterargs = filterargs

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

        return self.__class__(tiles, filterargs=[*self._filterargs, filterarg])

    def __repr__(self):
        return f"<Dataset | {len(self.tiles)}>"


def main():
    dataset_path = pathlib.Path("sentinel-dataset")
    dataset = Dataset.from_dir(dataset_path)
    low_cloud = dataset.filter(remove_empty=True, cloudcover=0.1, snowcover=0.1)

    years = collections.Counter([t.date.year for t in low_cloud.tiles if t.date is not None])
    print(years)


if __name__ == "__main__":
    main()
