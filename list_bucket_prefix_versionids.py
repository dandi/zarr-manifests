#!/usr/bin/env python3
import json
import sys
import boto3
from botocore import UNSIGNED
from botocore.client import Config


# # 'list_object_versions' would list all versions of all objects in the bucket
# # whenever for manifest we need only the latest version of each object
# #
# def list_versions_all(bucket_name, prefix):
#     s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))
#     version_list = []
#
#     paginator = s3_client.get_paginator('list_object_versions')
#
#     for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
#         if 'Versions' in page:
#             for version in page['Versions']:
#                 key = version['Key']
#                 version_id = version['VersionId']
#                 version_list.append((key, version_id))
#
#     return version_list

#
#
# def list_versions_head(bucket_name, prefix):
#     """Version does not list all versions but it then needs separate head_object
#     for each key to get the versionId -- takes more time overall
#     """
#     s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))
#     version_list = []
#
#     paginator = s3_client.get_paginator('list_objects_v2')
#
#     for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
#         if 'Contents' in page:
#             for obj in page['Contents']:
#                 key = obj['Key']
#                 # Fetch the version ID of the latest version
#                 obj_head = s3_client.head_object(Bucket=bucket_name, Key=key)
#                 version_id = obj_head.get('VersionId', 'null')
#                 version_list.append((key[len(prefix):].lstrip('/'), version_id))
#
#     return version_list


# TODO: add specification of before_last_modified for zarrs based on the datetime
#   we might know for that zarr from zarr_finalize... or may be we would just
#   generate such manifests for each "draft" version of zarr upon zarr_finalize
#   so release would consist of merely copying it... actually we should probably
#   add a notion of the "zarr version" based on its checksum or date and then
#   dandiset version should reference corresponding manifest.
def list_versions(bucket_name, prefix, before_last_modified=None):
    s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    version_list = {}
    paginator = s3_client.get_paginator('list_object_versions')

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Versions' in page:
            for version in page['Versions']:
                last_modified = version['LastModified']
                if before_last_modified and last_modified > before_last_modified:
                    continue
                key = version['Key'][len(prefix):].lstrip('/')
                version_id = version['VersionId']

                if key not in version_list or version_list[key][1] < last_modified:
                    version_list[key] = (version_id, last_modified, version['Size'], version['ETag'].strip('"'))

    return version_list


def print_jsonl(versions, out=sys.stdout):
    # I want custom formatting that we would have one entry per line
    s = """{
 schemaVersion: 1,
 fields: ['versionId', 'lastModified', 'size', 'ETag'],
 entries: {"""
    delim = ''
    for k, r in versions.items():
        # we need to convert last_modified to just iso_time
        assert len(r) >= 3
        r = (r[0], r[1].isoformat()) + r[2:]
        s += f'{delim}\n "{k}": {json.dumps(r)}'
        delim = ','
    s = s.rstrip(',') + "\n}}\n"
    out.write(s)


# too expensive if many keys and not many versions -- will be separate HEAD
# per each key.
# ❯ z=7723d02f-1f71-4553-a7b0-47bda1ae8b42; of=$z.manifest.json; time python list_bucket_prefix_versionids.py s3://dandiarchive/zarr/$z >| $of; ls -l $of
# 5.11s user 0.22s system 3% cpu 2:19.17 total
# -rw------- 1 yoh yoh 71885 Jan 26 15:51 7723d02f-1f71-4553-a7b0-47bda1ae8b42.manifest.json
#list_versions = list_versions_head

# ❯ z=7723d02f-1f71-4553-a7b0-47bda1ae8b42; of=$z.manifest.json; time python list_bucket_prefix_versionids.py s3://dandiarchive/zarr/$z >| $of; ls -l $of
# 1.43s user 0.09s system 8% cpu 18.225 total
# list_versions = list_versions_cmp

# ok -- now with storing size and mtime for each we get
# ❯ z=7723d02f-1f71-4553-a7b0-47bda1ae8b42; of=$z.manifest.json; time python list_bucket_prefix_versionids.py s3://dandiarchive/zarr/$z >| $of; ls -l $of
# python list_bucket_prefix_versionids.py s3://dandiarchive/zarr/$z >| $of  1.39s user 0.02s system 11% cpu 12.221 total
# -rw------- 1 yoh yoh 841835 Jan 26 16:03 7723d02f-1f71-4553-a7b0-47bda1ae8b42.manifest.json
#

# Example Usage
arg = sys.argv[1]
arg = arg.lstrip('s3://')
bucket_name, prefix = arg.split('/', 1)

versions = list_versions(bucket_name, prefix)
print_jsonl(versions)
