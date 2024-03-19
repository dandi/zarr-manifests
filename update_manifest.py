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
        if p.exists() or p.is_symlink():
            log.info("Rewriting already existing %s", p)
            p.unlink()
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


@dataclass
class ManifestUpdater:
    manifests_root: Path
    dandi_instance: str
    mode: str

    @property
    def bucket(self) -> str:
        return INSTANCES[self.dandi_instance][0]

    @property
    def api_url(self) -> str:
        return INSTANCES[self.dandi_instance][1]

    def get_zarr_dir(self, zarr_id: str) -> Path:
        return self.manifests_root / zarr_id[:3] / zarr_id[3:6] / zarr_id

    def are_updating(
        self, zarr_id: str, api_checksum: str | None, zarr_dir: Path
    ) -> bool:
        if self.mode == "force":
            return True
        last_checksum = get_last_saved_checksum(zarr_dir)
        if last_checksum is None:
            log.info(
                "Zarr %s does not have any manifests saved; creating new manifest",
                zarr_id,
            )
            return True
        elif api_checksum is None:
            log.info(
                "API checksum for Zarr %s is reported to be `null` or Zarr is"
                " unknown to API; not doing anything",
                zarr_id,
            )
            return False
        elif last_checksum == api_checksum:
            log.info(
                "API checksum for Zarr %s (%s) equals checksum in latest manifest;"
                " not doing anything",
                zarr_id,
                api_checksum,
            )
            return False
        else:
            log.info(
                "API checksum for Zarr %s (%s) differs from checksum in latest"
                " manifest (%s); creating new manifest",
                zarr_id,
                api_checksum,
                last_checksum,
            )
            return True

    def update_zarr_with_checksum(self, zarr_id: str, api_checksum: str | None) -> None:
        zarr_dir = self.get_zarr_dir(zarr_id)
        if self.are_updating(zarr_id, api_checksum, zarr_dir):
            builder = ManifestBuilder(api_checksum=api_checksum)
            prefix = f"zarr/{zarr_id}/"
            for entry in iter_zarr_entries(self.bucket, prefix):
                log.debug("Found entry: %s", entry.path)
                builder.add_entry(entry)
            builder.dump(zarr_dir)

    def update_zarr(self, zarr_id: str) -> None:
        api_checksum = get_checksum_from_api(self.api_url, zarr_id)
        self.update_zarr_with_checksum(zarr_id, api_checksum)

    def update_all_zarrs(self) -> None:
        for zarr_id, api_checksum in iter_api_zarrs(self.api_url):
            self.update_zarr_with_checksum(zarr_id, api_checksum)


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
@click.argument("zarr_id", required=False)
def main(
    zarr_id: str | None,
    manifests_root: Path,
    dandi_instance: str,
    mode: str,
    verbose: bool,
) -> None:
    logging.basicConfig(
        format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        level=logging.INFO,
    )
    if verbose:
        log.setLevel(logging.DEBUG)
    updater = ManifestUpdater(
        manifests_root=manifests_root, dandi_instance=dandi_instance, mode=mode
    )
    if zarr_id is None:
        updater.update_all_zarrs()
    else:
        updater.update_zarr(zarr_id)


def iter_api_zarrs(api_url: str) -> Iterator[tuple[str, str | None]]:
    with requests.Session() as s:
        url: str | None = f"{api_url}/zarr/"
        while url is not None:
            r = s.get(url)
            r.raise_for_status()
            data = r.json()
            for zobj in data["results"]:
                zarr_id = zobj["zarr_id"]
                checksum = zobj["checksum"]
                log.info(
                    "Found Zarr %s (checksum = %s) in API listing", zarr_id, checksum
                )
                yield (zarr_id, checksum)
            url = data["next"]


def get_checksum_from_api(api_url: str, zarr_id: str) -> str | None:
    r = requests.get(f"{api_url}/zarr/{zarr_id}/")
    if r.status_code == 404:
        return None
    r.raise_for_status()
    checksum = r.json().get("checksum")
    assert checksum is None or isinstance(checksum, str)
    return checksum


def get_last_saved_checksum(zarr_dir: Path) -> str | None:
    if not zarr_dir.is_dir():
        return None
    candidates = [p for p in zarr_dir.iterdir() if p.suffixes == [".json"]]
    if not candidates:
        return None
    elif len(candidates) == 1:
        return candidates[0].stem
    else:
        latest_modification: datetime | None = None
        latest_checksum: str | None = None
        for p in candidates:
            with p.open() as fp:
                try:
                    stats = json.load(fp)["statistics"]
                except Exception:
                    log.exception(
                        "Failed to load statistics from %s. Not considering",
                        p,
                    )
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
