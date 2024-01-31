Just a place for experimentation -- don't judge!

The idea for manifests layout is to be consistent with our /blobs/ on S3 where we provide
a few levels of "declutter" subfolders from the first letters of the zarr UUID, e.g.

    128/4a1/1284a14f-fe4f-4dc3-b10d-48e5db8bf18d

but unlike in case of blobs where the target is a blob file, in our case it is a folder under
which we collect manifests as identified by the corresponding zarr checksum: e.g.

    128/4a1/1284a14f-fe4f-4dc3-b10d-48e5db8bf18d/6ddc4625befef8d6f9796835648162be-509--710206390.json

which provides a full listing for the zarr, including the ETags per file, and sizes.  But for the zarr 
access, I think only versionid is needed, so we have also a

    128/4a1/1284a14f-fe4f-4dc3-b10d-48e5db8bf18d/6ddc4625befef8d6f9796835648162be-509--710206390.versionid.json

which has only versionId per each sub path.

Internally both files follow the same schema, and which fields are listed are listed in the header in the field 
fields which is either a list when multiple or a string when single value (saving space!), e.g.

    yoh@typhon:~/proj/dandi/zarr-manifests$ head -n 25 zarr-manifests-v2-sorted/128/4a1/1284a14f-fe4f-4dc3-b10d-48e5db8bf18d/6ddc4625befef8d6f9796835648162be-509--710206390.json 
    {
     "schemaVersion": 2,
     "fields": ["versionId","lastModified","size","ETag"],
     "statistics": {
      "entries": 509,
      "depth": 5,
      "totalSize": 710206390,
      "lastModified": "2022-06-27T23:09:39+00:00",
      "zarrChecksum": "6ddc4625befef8d6f9796835648162be-509--710206390"
     },
     "entries": {
      ".zattrs": ["VwOSu7IVLAQcQHcqOesmlrEDm2sL_Tfs","2022-06-27T23:07:47+00:00",8312,"cb32b88f6488d55818aba94746bcc19a"],
      ".zgroup": ["7obAY5BUNOdI1Uch3RoI4oHuGXhW4h0R","2022-06-27T23:07:47+00:00",24,"e20297935e73dd0154104d4ea53040ab"],
      ".zmetadata": ["Vfe0W0v4zkydmzyXkUMjm2Xr7.rIvfZQ","2022-06-27T23:07:47+00:00",15191,"4f505878fbb943a9793516cf084e07ad"],
      "0": {
       ".zarray": ["Ou6TnKwWPmEJrL.0utCWLPxgfr_lA0I1","2022-06-27T23:07:48+00:00",446,"5477ec3da352681e5ba6f6ea550ef740"],
       "0": {
        "0": {
         "13": {
          "8": {
           "100": ["lqNZ6OQ6lKd2QRW8ekWOiVfdZhiicWsh","2022-06-27T23:09:11+00:00",1793451,"7b5af4c6c28047c83dd86e4814bc0272"],
           "101": ["_i9cZBerb4mB9D8IFbPHo8nrefWcbq0p","2022-06-27T23:09:28+00:00",1799564,"50b6cfb69609319da9bf900a21d5f25c"],
           "103": ["DeraqBNPhVssSggCCTPjmgbu6XxOOupZ","2022-06-27T23:09:22+00:00",1811922,"c88b90dd6d4995fbb32b9a131d1b4ba0"],
           "104": ["J6ejQa4K9cEAjNbkKkZip8.zdK5XFI37","2022-06-27T23:09:20+00:00",1819642,"18d9f3602cb03bfaee7a6f4845db4f90"],
           "106": ["2yX_EGgu8ZxYExf.V.c_kc.jf4ijT8An","2022-06-27T23:09:33+00:00",1837884,"3006f1300734e195b505692e9c301235"],

for the full one so we have everything desired e.g. for webdav to provide listing and access, and compute/validate the zarrChecksum.
And the .versionid.json one looks like

    yoh@typhon:~/proj/dandi/zarr-manifests$ head -n 25 zarr-manifests-v2-sorted/128/4a1/1284a14f-fe4f-4dc3-b10d-48e5db8bf18d/6ddc4625befef8d6f9796835648162be-509--710206390.versionid.json 
    {
     "schemaVersion": 2,
     "fields": "versionId",
     "statistics": {
      "entries": 509,
      "depth": 5,
      "totalSize": 710206390,
      "lastModified": "2022-06-27T23:09:39+00:00",
      "zarrChecksum": "6ddc4625befef8d6f9796835648162be-509--710206390"
     },
     "entries": {
      ".zattrs": "VwOSu7IVLAQcQHcqOesmlrEDm2sL_Tfs",
      ".zgroup": "7obAY5BUNOdI1Uch3RoI4oHuGXhW4h0R",
      ".zmetadata": "Vfe0W0v4zkydmzyXkUMjm2Xr7.rIvfZQ",
      "0": {
       ".zarray": "Ou6TnKwWPmEJrL.0utCWLPxgfr_lA0I1",
       "0": {
        "0": {
         "13": {
          "8": {
           "100": "lqNZ6OQ6lKd2QRW8ekWOiVfdZhiicWsh",
           "101": "_i9cZBerb4mB9D8IFbPHo8nrefWcbq0p",
           "103": "DeraqBNPhVssSggCCTPjmgbu6XxOOupZ",
           "104": "J6ejQa4K9cEAjNbkKkZip8.zdK5XFI37",
           "106": "2yX_EGgu8ZxYExf.V.c_kc.jf4ijT8An",

which results in over twice smaller file but capable only to provide access to current version of zarr.
