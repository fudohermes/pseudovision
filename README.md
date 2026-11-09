# Pseudovision

Pseudovision (PV) is an IPTV playout engine written in Clojure. It builds
virtual TV channels from a Jellyfin media library: you attach a **Schedule** (a
sequence of **Slots**) to a **Channel**, and the engine compiles a concrete
**Playout** timeline of **Events** that IPTV clients play over HLS, with an M3U
playlist and XMLTV guide.

It started as a from-scratch rewrite of [ErsatzTV](https://ersatztv.org)'s
scheduling domain, using about 35 PostgreSQL tables instead of 130+.

PV is the hub of a small ecosystem:

| Repo | Role |
|---|---|
| **pseudovision** (this repo) | Media catalog (scanned from Jellyfin), channels, schedules, playout, streaming |
| [tunarr-scheduler](https://github.com/fudoniten/tunarr-scheduler) | Control plane: curates tags and plans each channel's schedule, then writes it to PV |
| [tunabrain](https://github.com/fudoniten/tunabrain) | Stateless LLM gateway; every AI call goes through it |
| [grout](https://github.com/fudoniten/grout) | Store for filler, bumpers, and orphan long-form clips; PV pulls filler from it |
| [marquee](https://github.com/fudoniten/marquee) | Web UI over all of the above |
| [pseudovision-cli](https://github.com/fudoniten/pseudovision-cli) | `pvcli`, a command-line client for PV, TS, and Grout |

Deployed at `https://pseudovision.kube.sea.fudo.link`.

Ongoing work for this repo **and the ecosystem as a whole** lives in
[PLAN.md](PLAN.md). Conventions and pitfalls for contributors are in
[AGENTS.md](AGENTS.md). The live-streaming design is in
[docs/streaming-design.md](docs/streaming-design.md).

---

## Running locally

Needs PostgreSQL 14+, Java 21+, and `ffmpeg`/`ffprobe`
(`nix develop .#pseudovision` provides all of them).

```sh
createdb pseudovision
clojure -X:migrate                     # apply migrations
clojure -M:run                         # start on :8080 (resources/config.edn)
clojure -M:run -c /path/to/config.edn  # with a custom config
clojure -M:repl                        # nREPL on :7888
clojure -X:migrate :action rollback    # roll back one migration
```

### Configuration

| Variable | Default | Description |
|---|---|---|
| `PSEUDOVISION_HTTP_PORT` | `8080` | HTTP port |
| `PSEUDOVISION_LOG_LEVEL` | `info` | trace/debug/info/warn/error |
| `PSEUDOVISION_DB_URL` | `jdbc:postgresql://localhost:5432/pseudovision` | JDBC URL |
| `PSEUDOVISION_DB_USER` / `PSEUDOVISION_DB_PASS` | `pseudovision` | DB credentials |
| `FFMPEG_PATH` / `FFPROBE_PATH` | `/usr/bin/ffmpeg`, `/usr/bin/ffprobe` | ffmpeg binaries |
| `PSEUDOVISION_STREAMS_DIR` | `/tmp/pseudovision/streams` | Live HLS segments |
| `PSEUDOVISION_SCRATCH_DIR` | `/tmp/pseudovision/scratch` | Scratch space |
| `GROUT_URL` | unset (Grout disabled) | Grout base URL |
| `GROUT_MEDIA_DIR` | `/data/media/grout` | Grout's media mount, which PV must share |
| `TZ` | `UTC` | Wall-clock zone for scheduling and EPG; keep it the same as TS's |

## Testing

```sh
clojure -M:test                                    # unit tests (Kaocha)
clojure -M:test --focus pseudovision.scheduling.packing-test
nix run .#integration-test                         # filler integration test against an ephemeral PostgreSQL
nix flake check                                    # unit tests, clj-kondo, and (on Linux) the NixOS integration test
```

---

## Core concepts

```
Channel
  └── Playout           (one per channel; the live timeline)
        └── Event[]     (concrete scheduled items with timestamps)

Schedule
  └── Slot[]            (ordered list of "what to play, how much")
        └── collection_id | media_item_id
```

### Media

PV scans media sources (Jellyfin) into libraries of `media_items`, each with
`metadata` and `metadata_tags`. Tags are plain strings; TS pushes its
dimensions as prefixed tags (`channel:hua`, `audience:kids`, `genre:comedy`).
Tags that describe a show live on the show, not on its episodes.

### Collections

A collection is a named source of items for a slot or filler preset:

- `manual`: an explicit list of items.
- `smart`: a query evaluated at build time. Keys (kebab-case): `show-id` (a
  series in season/episode order), `category` (+ optional `channel-tag`; all
  episodes and movies with that genre, scoped to one channel), `media-type`,
  `include-tags`/`exclude-tags` with `match` `all`|`any` (the item's own tags
  only), and `order-by` (`id`, `title`, `year`, `random`).
- `playlist`, `multi`, `trakt`: ordered lists, unions, and Trakt.tv lists.
- `rerun` is a stub and always resolves empty.

See `pseudovision.db.collections/resolve-collection` for the canonical rules.

### Schedules and slots

A **Schedule** is a reusable template with an ordered list of **Slots**. Each
slot has:

- **anchor**: `fixed` (starts at a wall-clock time) or `sequential` (follows
  the previous slot)
- **fill_mode**: `once` (one item), `count` (N items), `block` (fill a fixed
  duration), or `flood` (fill until the next fixed slot)
- **collection_id** or **media_item_id**; a slot with neither plays anything
  tagged for the channel
- **playback_order**: `chronological`, `shuffle`, `random`, and so on
- per-slot filler and watermark overrides

### Playouts and events

A **Playout** is a channel's compiled timeline. `PUT /api/channels/:id/playout`
attaches a schedule; `POST` rebuilds it as an async job. Rebuilds run in one of
two modes:

- **Reset** (`?from=now`) wipes upcoming generated events and restarts the
  schedule and every collection from the top.
- **Extend** (`?from=horizon`, and the bulk `POST /api/playouts/ensure`) resumes
  from the saved cursor, keeps near-term events, and continues each show's
  rotation. This is the one that's safe to run daily.

**Manual events** (`is_manual = true`) are injected through the API, and
rebuilds never touch them.

Rebuilds are tracked as **jobs** (`GET /api/jobs`, `GET /api/jobs/:job-id`)
with the same wire shape as TS's jobs, so Marquee renders both. Jobs live in
memory and don't survive a restart.

### Filler

Filler presets fill gaps around and between items. Each has a **role** (`pre`,
`mid`, `post`, `tail`, `fallback`), a **category** (`commercial`, `bumper`,
`short`, …), a **mode** (`duration`, `count`, `random_count`,
`pad_to_minute`), and a content source: a collection, a single item, or
**Grout**.

### Grout

Grout is off unless `GROUT_URL` is set, and always best-effort: a Grout outage
leaves a gap unfilled rather than failing a build or stream.

- **Filler.** A preset with a non-empty `grout_tags` is Grout-backed. When the
  build fills a gap with it, PV queries Grout for clips matching the channel
  (the channel name, kebab-cased; Grout also returns generic clips), the tags,
  and the gap length. Each clip is ingested once as a local-path `media_item`
  (`remote_key = grout:<id>`) and packed like any other filler.
- **Long-form content.** `POST /api/sync/grout` upserts every Grout `program`
  item as a `program`-kind media item with metadata and tags (plus a
  `channel:<slug>` tag), so it shows up in the catalog aggregate and is
  schedulable. Grout owns this metadata; each sync overwrites PV's copy.
- **Playback** reads the clip's `path` directly off the shared
  `GROUT_MEDIA_DIR` mount.

---

## API

The OpenAPI 3 spec is served at `/openapi.json` (interactive at
`/swagger-ui/`), generated from the route table in `http/core.clj` and the Malli
schemas in `http/schemas.clj`. It's authoritative; this is a map.

| Area | Paths |
|---|---|
| Channels | `/api/channels`, `/api/channels/:id` |
| Schedules & slots | `/api/schedules`, `/api/schedules/:id/slots`, `/api/schedules/:id/slot-order` |
| Playout | `/api/channels/:id/playout` (GET; PUT attaches a schedule; POST rebuilds; DELETE clears), `/api/channels/:id/playout/events` (list, inject, bulk delete), `/api/playouts/ensure` |
| Media | `/api/media/sources`, `/api/media/libraries`, `/api/media/items/:id` (+ `/children`, `/stream`, `/playback-url`), `/api/media/collections`, `/api/media-items/:id/tags`, `/api/tags` |
| Filler & ffmpeg | `/api/filler-presets`, `/api/ffmpeg/profiles` |
| Catalog (for TS) | `GET /api/catalog/aggregate` (`?channel=` or `?tag=`), `POST /api/sync/grout` |
| Jobs & metrics | `/api/jobs`, `/api/metrics/channels`, `/api/metrics/media-items` |
| Output | `/iptv/channels.m3u`, `/xmltv` (or `/epg.xml`), `/stream/:uuid`, `/logos/:uuid` |
| Ops | `/health`, `/api/version`, `/api/debug/stream/:uuid` |
| Dev only | `/api/test/*` creates throwaway test channels and collections |

To rotate a Jellyfin API key, `PUT /api/media/sources/:id` with
`{"api-key": "<new key>"}`. It's merged into the source's existing connection
config, so the connections stay as they are. A scan with a bad key fails with
"Jellyfin rejected the API key", and `POST /api/media/scan-all` fails its job
when any library fails (the per-library summary stays in the job's `result`).

Responses are kebab-case JSON. Routes migrated to Malli coercion return a
structured `400` on malformed input instead of a `500`.

To add the channels to Jellyfin, use Live TV → Tuner Devices → **M3U Tuner**
with `/iptv/channels.m3u`, and TV Guide Data Providers → **XMLTV** with
`/xmltv`.

---

## Project layout

```
src/pseudovision/
├── main.clj, system.clj, config.clj   entry point, Integrant system, config
├── db/                                next.jdbc + HoneySQL queries per domain
├── http/                              reitit router, middleware, Malli schemas,
│   └── api/                           one handler namespace per resource
├── scheduling/                        build engine (core), cursor, enumerators,
│                                      packing, filler
├── streaming/                         channel stream manager, HLS playlist,
│                                      segment store, source resolution
├── ffmpeg/                            profiles (hwaccel, decode modes) and HLS commands
├── media/                             Jellyfin client, scanner, Grout client and source
├── jobs/runner.clj                    in-memory async job runner
└── dev/test_channel.clj               helpers behind /api/test/*
```

## Database

Migrations live in `resources/migrations/`; the base schema is
`20260225-001-initial-schema.up.sql`. Compared with ErsatzTV, media items,
metadata, and collections are each one table (TPH, JSONB config), the playout
cursor is a JSONB column, and an ffmpeg profile is one JSONB `config`.

## Docker

```sh
docker build -t pseudovision .
docker run -p 8080:8080 \
  -e PSEUDOVISION_DB_URL=jdbc:postgresql://host.docker.internal:5432/pseudovision \
  pseudovision
```

Production images are built with Nix (`nix run .#deployContainers`).
