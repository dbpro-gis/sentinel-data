# noqa: E266
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
                                f"\r{filename}: [{'=' * done}{' ' * (50-done)}]")
                            sys.stdout.flush()
        return path


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
    footcount = collections.Counter(m["footprint"] for m in metas)
    coords = []
    for footprint in footcount:
        polys = swkt.loads(footprint)
        if "MultiPolygon" in str(type(polys)):
            for poly in polys:
                coords.append(list(poly.exterior.coords))
        else:
            coords.append(list(polys.exterior.coords))

    with shapefile.Writer(outpath) as shp:
        shp.field("name", "C")
        shp.poly(coords)
        shp.record("polygon2")


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


def plot_cloud_coverage(poly):
    terms = {
        "platformname": "Sentinel-2",
        "producttype": "S2MSI2A",
        "footprint": f"\"Intersects({poly})\"",
        # "cloudcoverpercentage": "0",
    }

    entries = SEARCH.search_terms(terms)
    metas = [SEARCH.parse_entry(e) for e in entries]
    print(len(metas))


def download_data_test():
    odata = OData(COPERNICUS_USER, COPERNICUS_PASS)
    uuid = "e96bba40-33de-491b-b123-866f4e60bbca"
    path = odata.get_tci_image_path(uuid)
    odata.download(path, f"download/{uuid}_tci.jp2")


def main():
    poly = polygon_from_bound_box(BOUNDS_GERMANY)

    ### Plot footprint coverage for both satellites
    # plot_footprint_coverage(poly, satellite="S2A")
    # plot_footprint_coverage(poly, satellite="S2B")

    ### Plot cloud cover using data from all satellites
    # plot_cloud_coverage(poly)

    ### Download satellite data
    download_data_test()


if __name__ == "__main__":
    main()
