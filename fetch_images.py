# flake8: noqa
"""
Search for satellite images in specified geographical area.

1. Obtain a overview of all products covering area.
2. Try to obtain least amount of cloud coverage for all areas.
3. Export a list of products which we will download.
4. Initiate download.
"""
import os
import sys
import pathlib
import datetime
import collections
import xml.etree.ElementTree as ET

import requests
import requests_cache

import shapefile
from shapely.geometry import MultiPolygon
import shapely.wkt as swkt

import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt


requests_cache.install_cache("sentinel_cache")

# Bounding box is defined by lon lat lon lat, imagine 4 lines
BOUNDS_GERMANY = ["5.86442", "47.26543", "15.05078", "55.14777"]
COPERNICUS_USER = os.environ["COPERNICUS_USER"]
COPERNICUS_PASS = os.environ["COPERNICUS_PASS"]


def bounds_to_points(bound_box):
    lon1, lat1, lon2, lat2 = bound_box
    return [
        (lon1, lat1), (lon2, lat1), (lon2, lat2), (lon1, lat2), (lon1, lat1)
    ]


def create_query(terms):
    query = " AND ".join(f"{k}:{v}" for k, v in terms.items())
    return f"({query})"


def polygon_from_points(points):
    points = ",".join(" ".join(b) for b in points)
    return f"POLYGON(({points}))"


def polygon_from_bound_box(box):
    return polygon_from_points(bounds_to_points(box))


def selectsingle(items):
    items = list(items)
    assert len(items) == 1, "Select from iterable with single item"
    return items[0]


def select_prod_builder(key):
    def filterfun(items):
        return selectsingle(filter(lambda i: key in i, items))
    return filterfun


def parse_size(size):
    if size.endswith("GB"):
        size = float(size[:-3]) * 1000
    else:
        size = float(size[:-3])
    return size


class OData:
    base_url = "https://scihub.copernicus.eu/dhus/odata/v1"
    _ns = {
        "a": "http://www.w3.org/2005/Atom",
    }

    def __init__(self, user, password):
        self._user = user
        self._password = password

        self._session = requests.Session()

    def request(self, path, params=None):
        url = f"{self.base_url}/{path}"
        req = self._session.get(
            url, params=params, auth=(self._user, self._password))
        if req.status_code != 200:
            print(url)
            print(req.text)
            raise RuntimeError
        return req.text

    def request_nodes(self, path):
        text = self.request(path)
        tree = ET.fromstring(text)
        filename_nodes = tree.findall(
            ".//a:entry/a:title", namespaces=self._ns)
        filenames = [n.text for n in filename_nodes]
        return filenames

    def product_filename(self, uuid):
        path = f"Products('{uuid}')/Nodes"
        filenames = self.request_nodes(path)
        return filenames[0]

    def metadata(self):
        result = self.request("$metadata")
        print(result)

    def get_tci_image_path(self, uuid, path_elems=None):
        if path_elems is None:
            path_elems = [
                ("Products", uuid),
                ("Nodes", selectsingle),
                ("Nodes", "GRANULE"),
                ("Nodes", selectsingle),
                ("Nodes", "IMG_DATA"),
                ("Nodes", "R10m"),
                ("Nodes", select_prod_builder("TCI")),
            ]

        parsed_path_elems = []
        querypath = ""
        for name, value in path_elems:
            if not isinstance(value, str):
                filterfun = value
                filenames = self.request_nodes(querypath + "/Nodes")
                value = filterfun(filenames)

            parsed_path_elems.append((name, value))
            querypath += f"/{name}('{value}')"

        querypath += "/$value"
        return querypath

    def download(self, path, outpath):
        url = f"{self.base_url}/{path}"
        pathlib.Path(outpath).parent.mkdir(parents=True, exist_ok=True)
        filename = pathlib.Path(outpath).name
        with requests_cache.disabled():
            with requests.get(url, stream=True, auth=(self._user, self._password)) as r:
                r.raise_for_status()
                total_length = int(r.headers.get('content-length'))
                downloaded = 0
                with open(outpath, "wb") as outfile:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            outfile.write(chunk)
                            downloaded += len(chunk)
                            done = int(50 * downloaded / total_length)
                            sys.stdout.write(
                                f"\r{filename}: [{'='*done}{' '*(50-done)}]")
                            sys.stdout.flush()
        return path


ODATA = OData(COPERNICUS_USER, COPERNICUS_PASS)


class OpenSearch:

    base_url = "https://scihub.copernicus.eu/dhus"
    _ns = {
        "a": "http://www.w3.org/2005/Atom",
        "os": "http://a9.com/-/spec/opensearch/1.1/"
    }

    entry_fields = {
        "platformname": "str",
        "size": "str",
        "producttype": "str",
        "filename": "str",
        "format": "str",
        "footprint": "str",
        "uuid": "str",
        "beginposition": "date",
        "endposition": "date",
        "orbitnumber": "int",
        "relativeorbitnumber": "int",
        "cloudcoverpercentage": "double",
        "highprobacloudspercentage": "double",
        "mediumprobacloudspercentage": "double",
        "snowicepercentage": "double",
        "vegetationpercentage": "double",
        "waterpercentage": "double",
        "notvegetatedpercentage": "double",
        "unclassifiedpercentage": "double",
    }

    def __init__(self, user, password):
        self._session = requests.Session()
        self._user = user
        self._password = password

    @classmethod
    def parse_entry(cls, entry):
        meta = {}
        for field, field_type in cls.entry_fields.items():
            value = entry.find(
                f".//a:{field_type}[@name='{field}']", cls._ns).text
            if field_type == "double":
                value = float(value)
            elif field_type == "int":
                value = int(value)
            elif field_type == "date":
                value = datetime.datetime.strptime(
                    value[:-5], "%Y-%m-%dT%H:%M:%S")
            meta[field] = value
        return meta

    def _parse_xml(self, tree):
        if isinstance(tree, str):
            tree = ET.fromstring(tree)

        total_results = tree.find(".//os:totalResults", self._ns).text
        start_index = tree.find(".//os:startIndex", self._ns).text
        # title = tree.findall(".//", self._ns)
        entries = tree.findall(".//a:entry", self._ns)
        return {
            "start_index": start_index,
            "total_results": total_results,
            "entries": entries,
        }

    def search_raw(self, query, start=0, rows=100):
        url = f"{self.base_url}/search?start={start}&rows={rows}&q=" + query
        resp = self._session.get(url, auth=(self._user, self._password))
        if resp.status_code != 200:
            raise RuntimeError(resp)
        return resp.text

    def search(self, *args, **kwargs):
        return self._parse_xml(self.search_raw(*args, **kwargs))

    def search_terms(self, terms):
        query = create_query(terms)
        max_index = 0
        entries = []
        result = self.search(query)
        entries += result["entries"]
        max_index = int(result["total_results"])
        while len(entries) < max_index:
            print(f"Getting {len(entries)}")
            result = self.search(query, start=len(entries))
            entries += result["entries"]
        return entries


SEARCH = OpenSearch(COPERNICUS_USER, COPERNICUS_PASS)


def export_meta_shapes_to_shapefile(metas, outpath):
    with shapefile.Writer(outpath) as shp:
        shp.field("name", "C")
        shp.field("cloudcover", "C")
        for i, meta in enumerate(metas):
            footprint = meta["footprint"]
            polys = swkt.loads(meta["footprint"])
            if isinstance(polys, MultiPolygon):
                coords = [list(poly.exterior.coords) for poly in polys.geoms]
            else:
                coords = [list(polys.exterior.coords)]
            shp.poly(coords)
            shp.record(f"polygon{i}", meta["cloudcoverpercentage"])


def plot_footprint_coverage(poly, satellite="S2A"):
    """Show footprint of single tiles and plot some statistics.
    Geographical plots are created in qgis"""

    terms = {
        "platformname": "Sentinel-2",
        "filename": f"{satellite}_*",
        "producttype": "S2MSI2A",
        "footprint": f"\"Intersects({poly})\"",
        # "cloudcoverpercentage": "0",
    }
    entries = SEARCH.search_terms(terms)
    metas = [SEARCH.parse_entry(e) for e in entries]

    # create shapefiles
    export_meta_shapes_to_shapefile(metas, "shapefiles/{satellite.lower()}")

    # plot time
    times = collections.Counter([meta["beginposition"] for meta in metas])
    sel_time, _ = times.most_common(1)[0]
    sel_metas = [m for m in metas if m["beginposition"] == sel_time]
    footcount = collections.Counter(m["footprint"] for m in sel_metas)
    print(footcount.most_common(1))

    plt.scatter(times.keys(), times.values(), s=1)
    plt.ylabel("Number of files")
    plt.xlabel("Acquisition time")
    plt.tight_layout()
    plt.savefig(f"timeplot_{satellite.lower()}.png")


def plot_cloud_coverage(poly, plot_cloudbins=False, order_footprint=False):
    terms = {
        "platformname": "Sentinel-2",
        "producttype": "S2MSI2A",
        "footprint": f"\"Intersects({poly})\"",
        # "cloudcoverpercentage": "0",
    }

    entries = SEARCH.search_terms(terms)
    metas = [SEARCH.parse_entry(e) for e in entries]

    if plot_cloudbins:
        # binning metas on cloudcover
        binned_cloud = [
            round(m["cloudcoverpercentage"] / 10) * 10 for m in metas]
        cloud_counts = collections.Counter(binned_cloud)

        bins = list(sorted(cloud_counts.keys()))
        values = [cloud_counts[b] for b in bins]
        ypos = list(range(len(bins)))
        plt.bar(ypos, values)
        plt.xticks(ypos, labels=bins)
        plt.title("Cloud coverage in datasets intersecting with germany")
        plt.xlabel("Cloud cover percentage")
        plt.ylabel("Number of datasets")
        plt.tight_layout()
        plt.savefig(f"cloudbinned.png")

    # groupby entries with same footprint
    if order_footprint:
        same_footprint = collections.defaultdict(list)
        for meta in metas:
            same_footprint[meta["footprint"]].append(meta)

        for foot, group in same_footprint.items():
            min_cloud = min(group, key=lambda m: m["cloudcoverpercentage"])
            print(foot, min_cloud["cloudcoverpercentage"])

    for perc in [10, 20, 30, 40, 50, 60, 70, 80, 90]:
        low_cloud_meta = [
            m for m in metas if m["cloudcoverpercentage"] <= perc]
        export_meta_shapes_to_shapefile(
            low_cloud_meta, f"shapefiles/low_cloud_{perc}")


def download_data_test():
    """Test downloading TCI images using the ODATA API."""
    odata = OData(COPERNICUS_USER, COPERNICUS_PASS)
    uuid = "e96bba40-33de-491b-b123-866f4e60bbca"
    path = odata.get_tci_image_path(uuid)
    odata.download(path, f"download/{uuid}_tci.jp2")


def reduce_footprint_unique(metas):
    """Remove all footprints completely overlapping with shapes covering area
    alone.
    """
    metas = sorted(metas, key=lambda m: m["cloudcoverpercentage"])
    shapes = []
    for meta in metas:
        poly = swkt.loads(meta["footprint"])
        if isinstance(poly, MultiPolygon):
            assert len(poly.geoms) == 1
            shapes.extend(poly.geoms)
        else:
            shapes.append(poly)

    selected = []
    union_area = None
    for i, shape in enumerate(shapes):
        if union_area is None:
            union_area = shape
            selected.append(i)
        else:
            prev_size = union_area.area
            union_area = union_area.union(shape)
            if prev_size < union_area.area:
                selected.append(i)

    filtered = [metas[i] for i in selected]
    return filtered


def inside_bounds(poly_inner, poly_outer):
    bounds_inner = poly_inner.bounds
    bounds_outer = poly_outer.bounds
    return (
        bounds_inner[0] >= bounds_outer[0]
        and bounds_inner[1] >= bounds_outer[1]
        and bounds_inner[2] <= bounds_outer[2]
        and bounds_inner[3] <= bounds_outer[3]
    )


def split_bounds(bounds):
    minx, miny, maxx, maxy = bounds.bounds
    line_a = LineString(
        (
            (minx + ((maxx - minx) / 2), miny),
            ((minx + ((maxx - minx) / 2), maxy))
        )
    )
    line_b = LineString(
        (
            (minx, miny + ((maxy - miny) / 2)),
            (maxx, (miny + ((maxy - miny) / 2)))
        )
    )
    box_a, box_b = list(split(bounds, line_a).geoms)
    box_aa, box_ab = list(split(box_a, line_b).geoms)
    box_ba, box_bb = list(split(box_b, line_b).geoms)
    return box_aa, box_ab, box_ba, box_bb


def smallest_cover_set(poly):
    """Test filtering entries down to smallest cover set for germany."""
    terms = {
        "platformname": "Sentinel-2",
        "producttype": "S2MSI2A",
        "footprint": f"\"Intersects({poly})\"",
        # "cloudcoverpercentage": "0",
    }
    entries = SEARCH.search_terms(terms)
    metas = [SEARCH.parse_entry(e) for e in entries]
    print("All:", len(metas))

    filtered = [m for m in metas if m["cloudcoverpercentage"] < 10]
    print("Only <10% clouds:", len(filtered))

    same_footprint = collections.defaultdict(list)
    for meta in filtered:
        same_footprint[meta["footprint"]].append(meta)

    filtered = [
        min(v, key=lambda m: m["cloudcoverpercentage"])
        for v in same_footprint.values()
    ]
    print("Only same footprint:", len(filtered))

    filtered = reduce_footprint_unique(filtered)
    print("Reduce based on uniques:", len(filtered))

    sum_size = sum(parse_size(m["size"]) for m in filtered)
    print("Summed data size", sum_size / 1000, "GB")
    avgcover = sum(m["cloudcoverpercentage"] for m in filtered)/len(filtered)
    maxcover = max(m["cloudcoverpercentage"] for m in filtered)
    print("Avg cloudcover:", avgcover, "Max cloud: ", maxcover)

    export_meta_shapes_to_shapefile(
        filtered, f"shapefiles/unique_set")

    return filtered


def generate_download_urls(metas):
    uuids = [m["uuid"] for m in metas]


def main():
    print("Hello World")
    poly = polygon_from_bound_box(BOUNDS_GERMANY)

    ### Plot footprint coverage for both satellites
    # plot_footprint_coverage(poly, satellite="S2A")
    # plot_footprint_coverage(poly, satellite="S2B")

    ### Plot cloud cover using data from all satellites
    # plot_cloud_coverage(poly)

    ### Download satellite data
    # download_data_test()

    ### Filtering data down to smallest cover set
    metas = smallest_cover_set(poly)
    generate_download_urls(metas)


if __name__ == "__main__":
    main()
