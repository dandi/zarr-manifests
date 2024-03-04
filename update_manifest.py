#!/usr/bin/env python3
# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "boto3",
#     "click >= 8.0",
#     "requests ~= 2.20",
#     "zarr_checksum",
# ]
# ///

from __future__ import annotations
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
import json
import logging
from pathlib import Path
import re
from typing import Any
import boto3
from botocore import UNSIGNED
from botocore.client import Config
import click
import requests
from zarr_checksum.tree import ZarrChecksumTree

log = logging.getLogger("update_manifest")

INSTANCES = {
    # name: (bucket, API URL)
    "dandi": ("dandiarchive", "https://api.dandiarchive.org/api"),
    "dandi-staging": (
        "dandi-api-staging-dandisets",
        "https://api-staging.dandiarchive.org/api",
    ),
}


@dataclass
class ManifestBuilder:
    api_checksum: str | None
    tree: dict[str, Any] = field(init=False, default_factory=dict)
    depth: int = field(default=0, init=False)
    entries: int = field(default=0, init=False)
    total_size: int = field(default=0, init=False)
    last_modified: datetime | None = field(default=None, init=False)
    checksum_tree: ZarrChecksumTree = field(
        init=False, default_factory=ZarrChecksumTree
    )

    def add_entry(self, entry: Entry) -> None:
        d = self.tree
        *parents, name = entry.path.split("/")
        for p in parents:
            d = d.setdefault(p, {})
            assert isinstance(d, dict)
        d[name] = entry.field_list()
        depth = len(parents)
        if depth > self.depth:
            self.depth = depth
        self.entries += 1
        self.total_size += entry.size
        if self.last_modified is None or self.last_modified < entry.last_modified:
            self.last_modified = entry.last_modified
        self.checksum_tree.add_leaf(
            path=Path(entry.path), size=entry.size, digest=entry.etag
        )

    def dump(self, zarr_dir: Path) -> None:
        checksum = str(self.checksum_tree.process())
        data: dict[str, Any] = {
            "schemaVersion": 2,
            "fields": ["versionId", "lastModified", "size", "ETag"],
            "statistics": {
                "entries": self.entries,
                "depth": self.depth,
                "totalSize": self.total_size,
                "lastModified": (
                    self.last_modified.isoformat()
                    if self.last_modified is not None
                    else None
                ),
                "zarrChecksum": checksum,
            },
            "entries": self.tree,
        }
        if self.api_checksum != checksum:
            data["statistics"]["zarrChecksumMismatch"] = self.api_checksum
        zarr_dir.mkdir(parents=True, exist_ok=True)
        p = zarr_dir / f"{checksum}.json"
        log.info("Saving manifest to %s", p)
        with p.open("w") as fp:
            json.dump(data, fp, cls=MyJSONEncoder, indent=1)


@dataclass
class Entry:
    path: str
    version_id: str
    last_modified: datetime
    size: int
    etag: str

    def field_list(self) -> list[Any]:
        return [self.version_id, self.last_modified.isoformat(), self.size, self.etag]


@click.command()
@click.option(
    "-i", "--dandi-instance", type=click.Choice(list(INSTANCES)), default="dandi"
)
@click.option("--mode", type=click.Choice(["api-check", "force"]), default="api-check")
@click.option(
    "--manifests-root",
    type=click.Path(file_okay=False, dir_okay=True, path_type=Path),
    required=True,
)
@click.option("-v", "--verbose", is_flag=True)
@click.argument("zarr_id")
def main(
    zarr_id: str, manifests_root: Path, dandi_instance: str, mode: str, verbose: bool
) -> None:
    logging.basicConfig(
        format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        level=logging.INFO,
    )
    if verbose:
        log.setLevel(logging.DEBUG)
    zarr_dir = manifests_root / zarr_id[:3] / zarr_id[3:6] / zarr_id
    api_checksum = get_checksum_from_api(INSTANCES[dandi_instance][1], zarr_id)
    if mode == "force":
        run = True
    else:
        last_checksum = get_last_saved_checksum(zarr_dir)
        if last_checksum is None:
            log.info("Zarr %s does not have any manifests saved; creating new manifest", zarr_id)
            run = True
        elif api_checksum is None:
            log.info("API checksum for Zarr %s is reported to be `null` or Zarr is unknown to API; not doing anything", zarr_id)
            run = False
        elif last_checksum == api_checksum:
            log.info(
                "API checksum for Zarr %s (%s) equals checksum in latest manifest;"
                " not doing anything",
                zarr_id,
                api_checksum,
            )
            run = False
        else:
            log.info(
                "API checksum for Zarr %s (%s) differs from checksum in latest"
                " manifest (%s); creating new manifest",
                zarr_id,
                api_checksum,
                last_checksum,
            )
            run = True
    if not run:
        return
    builder = ManifestBuilder(api_checksum=api_checksum)
    bucket = INSTANCES[dandi_instance][0]
    prefix = f"zarr/{zarr_id}/"
    for entry in iter_zarr_entries(bucket, prefix):
        log.debug("Found entry: %s", entry.path)
        builder.add_entry(entry)
    builder.dump(zarr_dir)


def get_checksum_from_api(api_url: str, zarr_id: str) -> str | None:
    r = requests.get(f"{api_url}/zarr/{zarr_id}")
    if r.status_code == 404:
        return None
    r.raise_for_status()
    checksum = r.json().get("checksum")
    assert checksum is None or isinstance(checksum, str)
    return checksum


def get_last_saved_checksum(zarr_dir: Path) -> str | None:
    if not zarr_dir.is_dir():
        return None
    latest_modification: datetime | None = None
    latest_checksum: str | None = None
    for p in zarr_dir.iterdir():
        if p.suffixes == [".json"]:
            with p.open() as fp:
                try:
                    stats = json.load(fp)["statistics"]
                except Exception as exc:
                    log.error("Failed to load statistics from %s. Not considering", p, exception=exc)
                    continue
                if stats["lastModified"] is not None:
                    last_modified = datetime.fromisoformat(stats["lastModified"])
                    if (
                        latest_modification is None
                        or latest_modification < last_modified
                    ):
                        latest_modification = last_modified
                        latest_checksum = p.stem
    return latest_checksum


def iter_zarr_entries(bucket: str, prefix: str) -> Iterator[Entry]:
    # `prefix` must end with a slash
    s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    paginator = s3_client.get_paginator("list_object_versions")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for version in page.get("Versions", []):
            if version["IsLatest"]:
                yield Entry(
                    path=version["Key"].removeprefix(prefix),
                    version_id=version["VersionId"],
                    last_modified=version["LastModified"],
                    size=version["Size"],
                    etag=version["ETag"].strip('"'),
                )


# For custom JSON dumper with lists inlined for better readability and size
class MyJSONEncoder(json.JSONEncoder):
    """
    A custom encoder so that lists are flattened and do not have newlines

    ref: https://stackoverflow.com/a/39730360/1265472
    """

    def iterencode(self, o: Any, _one_shot: bool = False) -> Iterator[str]:
        list_lvl = 0
        for s in super().iterencode(o, _one_shot=_one_shot):
            if s.startswith("["):
                list_lvl += 1
                s = s.replace("\n", "")
                s = s[0] + s[1:].strip()
            elif 0 < list_lvl:
                s = re.sub(r"\n\s*", "", s).rstrip()
                if s.endswith(","):
                    s = s[:-1] + self.item_separator
                elif s.endswith(":"):
                    s = s[:-1] + self.key_separator
            if s.endswith("]"):
                list_lvl -= 1
            yield s


if __name__ == "__main__":
    main()
