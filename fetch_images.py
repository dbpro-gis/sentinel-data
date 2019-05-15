"""
Search for satellite images in specified geographical area.

1. Obtain a overview of all products covering area.
2. Try to obtain least amount of cloud coverage for all areas.
3. Export a list of products which we will download.
4. Initiate download.
"""
import os
import requests
import requests_cache
import datetime
import xml.etree.ElementTree as ET


requests_cache.install_cache("sentinel_cache")


BOUNDS_GERMANY = ["5.86442", "47.26543", "15.05078", "55.14777"]


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
                value = datetime.datetime.strptime(value[:-5], "%Y-%m-%dT%H:%M:%S")
            meta[field] = value
        print(meta)

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


def main():
    user = os.environ["COPERNICUS_USER"]
    password = os.environ["COPERNICUS_PASS"]
    poly = polygon_from_bound_box(BOUNDS_GERMANY)
    search = OpenSearch(user, password)
    terms = {
        "platformname": "Sentinel-2",
        "filename": "S2A_*",
        "producttype": "S2MSI2A",
        "footprint": f"\"Intersects({poly})\"",
        # "cloudcoverpercentage": "0",
    }
    # search.search_terms(terms)
    res = search.search(create_query(terms), start=0, rows=1)
    for entry in res["entries"]:
        search.parse_entry(entry)


if __name__ == "__main__":
    main()
