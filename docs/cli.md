# CLI

- `ninelives ls`: list function cache entries.
- `ninelives inspect KEY`: validate and display entry metadata.
- `ninelives clear --older-than 30d`: remove old cache entries.
- `ninelives gc --older-than 30d --max-size 1GB`: remove old entries and trim the remaining cache to a size limit.
- `ninelives gc --tag model-weights`: remove entries registered with that decorator tag.
- `ninelives verify`: validate all cache entry headers and checksums.
- `python -m ninelives`: module entrypoint (same CLI is exposed as console script).
