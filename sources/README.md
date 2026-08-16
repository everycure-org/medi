# Manually-acquired source files

`manual-sources.zip` holds the two source files MeDIC cannot fetch for itself. Everything else the
pipeline needs is downloaded at ingest time; these two have no automated path to them, and without
them a clean checkout cannot rebuild China or Russia at all.

**The archive is not in this repo.** Neither file is redistributable — the CDE approvals table and
the GRLS register are both published without an open licence (see [`LICENSING.md`](../LICENSING.md)),
so MeDIC does not ship them. The archive is hosted out-of-band and downloaded on demand.

| Member | Source | Feeds | Why it is not fetched |
|---|---|---|---|
| `cder_drugs_final_all.csv` | CDE / NMPA approvals table, <https://www.cde.org.cn> | `medic.ingest.china` | Scraped out-of-band; the paginated table has no bulk export or API |
| `grls.zip` | GRLS, the Russian State Register of Medicines, <https://grls.rosminzdrav.ru> | `medic.ingest.russia` | IP-blocked for anonymous non-Russian sessions — search endpoints return an empty form shell rather than rows |

## Using it

Add the archive URL to the gitignored `.env` — ask a maintainer for it:

```bash
MEDIC_MANUAL_SOURCES_URL="<direct-download url>"
```

Then:

```bash
just restore-manual-sources
```

That downloads the archive to `background/manual-sources.zip` and unpacks both members into
`background/`, which is where the ingesters look (`background/cder_drugs_final_all.csv`,
`background/grls.zip`). `background/` is gitignored scratch. The download is skipped if the archive
is already there, and existing extracted files are left alone unless you pass `force=true`.

**The URL is deliberately not committed.** A public "anyone with the link" URL sitting in a public
repo would redistribute the CDE and GRLS data just as effectively as committing the zip did — the
point of moving it out was to stop doing that, not to add a hop. `just`'s dotenv file is
`config.public.mk`, which is tracked, so the recipe reads `.env` directly instead. You can also
export the variable for a single run.

The URL must be a **direct download**. A Dropbox "Copy link" gives you `…?rlkey=<key>&dl=0`, which
serves an HTML page; change it to `dl=1`. The recipe checks that what it downloaded is actually a
zip and fails with that explanation rather than letting `unzip` produce a cryptic error.

The ingesters fail loudly with the same instruction when a file is missing, so a clean run tells you
what to do rather than silently skipping a source.

## Refreshing a source

Replace the file in `background/`, then repack and re-fingerprint:

```bash
just refresh-manual-sources
```

That rebuilds `background/manual-sources.zip` from the current `background/` copies. Upload it to
the host behind `MEDIC_MANUAL_SOURCES_URL`, then re-run `just ingest-china` / `just ingest-russia`
so `data/source_manifest.json` records the sha256 and row counts of the new files — that manifest is
how a build records **which** input produced it.

Commit the manifest on its own. The archive is never committed.

## Provenance of the current copies

Fingerprints recorded in `data/source_manifest.json`:

- `cder_drugs_final_all.csv` — sha256 `980262a8a7adc7ec…`, 1,521 rows, retrieved 2026-07-25
- `grls.zip` — sha256 `a34040a4b6c433d0…`, 5,885 rows, retrieved 2026-07-24

`just check-manual-sources` verifies whatever you have restored locally against these. On a machine
with nothing restored — CI, a fresh clone — it reports the files as skipped and still checks that the
manifest itself is complete.
