#!/usr/bin/env python3
"""A very crude script which given initial dump of the manifest v1 produces
two versions in v2 -- where it is a hierarchy of the zarr tree with statistics
and checksum validated against what dandi archive reports for that zarr ATM (so
we could differ for newer zarrs).

TODOs
- if run into zarr checksum mismatch, check lastModified in metadata for the asset
  in that dandiset/path -- may be it was updated...
"""
import re
import sys
import json
from collections import defaultdict

import requests
from pathlib import Path

from zarr_checksum.generators import ZarrArchiveFile
from zarr_checksum import compute_zarr_checksum


def recursive_defaultdict():
    """A helper to simplify creating recursive defaultdicts for file tree"""
    return defaultdict(recursive_defaultdict)


def yield_files_entries(entries):
    """To feed zarr_checksum with the entries from the manifest v1"""
    for k, r in sorted(entries.items()):
        yield ZarrArchiveFile(
            path=Path(k),
            size=r[2],
            digest=r[3],
        )

#
# For custom JSON dumper with lists inlined for better readability and size
#
class MyJSONEncoder(json.JSONEncoder):
    """A custom encoder so that lists are flattened and do not have newlines

    ref: https://stackoverflow.com/a/39730360/1265472
    """

    def iterencode(self, o, _one_shot=False):
        list_lvl = 0
        for s in super(MyJSONEncoder, self).iterencode(o, _one_shot=_one_shot):
            if s.startswith('['):
                list_lvl += 1
                s = s.replace('\n', '')
                s = s[0] + s[1:].strip()
            elif 0 < list_lvl:
                s = re.sub('\n\s*', '', s).rstrip()
                if s and s[-1] == ',':
                    s = s[:-1] + self.item_separator
                elif s and s[-1] == ':':
                    s = s[:-1] + self.key_separator
            if s.endswith(']'):
                list_lvl -= 1
            yield s


def filter_entries(entries, indexes):
    """Filter entries to keep only selected fields"""
    for k, r in entries.items():
        if isinstance(r, list):
            entries[k] = [r[i] for i in indexes]
            if len(indexes) == 1:
                entries[k] = entries[k][0]
        else:
            entries[k] = filter_entries(r, indexes)
    return entries

def json_dumps(obj, indent=1):
    out = json.dumps(obj, cls=MyJSONEncoder, indent=indent)
    assert json.loads(out) == obj
    return out


def save(out_rec, outfile, fields=None):
    if fields:
        assert set(fields).issubset(out_rec['fields'])
        indexes = [
            out_rec['fields'].index(f)
            for f in fields
        ]
        if len(fields) > 1:
            out_rec['fields'] = fields
        else:
            # no list needed
            out_rec['fields'] = fields[0]
        out_rec['entries'] = filter_entries(out_rec['entries'], indexes)

    Path(outfile).write_text(json_dumps(out_rec, indent=1))


def fetch_dandi_zarr_checksum(zarr_id):
    url = f'https://api.dandiarchive.org/api/zarr/{zarr_id}/'
    response = requests.get(url)
    response.raise_for_status()
    return response.json().get('checksum')


if __name__ == '__main__':

    infile, outfile = sys.argv[1:3]

    with open(infile) as f:
        rec = json.load(f)

    hierarchy = recursive_defaultdict()

    for k, r in rec['entries'].items():
        h = hierarchy
        parts = k.split('/')
        for part in parts[:-1]:
            h = h[part]
        h[parts[-1]] = r

    # print(json.dumps(hierarchy, indent=2))

    # Gather statistics -- how many entries we have,
    # how deep is the hierarchy, total size, the latest lastModified
    # and compute the zarr checksum!
    lastModified, size, depth = None, 0, 0
    # TODO: remove hardcoding of the indexes
    for k, r in rec['entries'].items():
        depth = max(depth, k.count('/'))
        if not lastModified or r[1] > lastModified:
            lastModified = r[1]
        size += r[2]

    zarr_checksum = compute_zarr_checksum(yield_files_entries(rec['entries']))

    # For paranoids and while developing!
    assert zarr_checksum.count == len(rec['entries'])
    assert zarr_checksum.size == size
    # just crude for now while debugging etc
    assert zarr_checksum.digest == fetch_dandi_zarr_checksum(Path(infile).stem)

    out_rec = {
        "schemaVersion": 2,  # Let's call it 2 for now
        "fields": rec["fields"],  # nothing changed
        "statistics": {
            "entries": len(rec['entries']),
            "depth": depth,
            "totalSize": size,
            "lastModified": lastModified,
            "zarrChecksum": zarr_checksum.digest,
        },
        "entries": hierarchy
    }

    # TODO: provide at least 2 filtered views of the same data
    out_base = Path(outfile)
    out_base = out_base.parent / out_base.stem
    # 1. with all the fields
    save(out_rec, f"{out_base}.json")
    # 2. with only versionId
    save(out_rec, f"{out_base}.versionid.json", fields=['versionId'])
