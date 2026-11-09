# AGENTS.md — Pseudovision

> Working notes for AI agents and humans changing this repo. Start with
> [README.md](README.md) (what PV is, how to run and test it, core concepts)
> and [PLAN.md](PLAN.md) (what's being worked on here and across the
> ecosystem). The live-streaming design is
> [docs/streaming-design.md](docs/streaming-design.md).

## Who calls PV

- **Tunarr Scheduler** is the main programmatic writer: it pushes channels and
  tags, syncs each channel's frozen grid onto schedules and slots, extends
  playout horizons, and reads `GET /api/catalog/aggregate`.
- **Marquee** reads almost everything and triggers rebuilds; **pvcli** reads.
- **Grout** is called *by* PV (filler at build time, long-form via
  `POST /api/sync/grout`); it never calls PV.
- **Tunabrain** never talks to PV.

## Conventions

- **Kebab-case JSON both ways.** The Muuntaja middleware kebab-cases every
  request and response key (`has-more`, `duration-ms`). Clients that read
  snake_case keys silently get `nil`.
- **The route table and Malli schemas are the API docs.** Add routes in
  `http/core.clj` with schemas in `http/schemas.clj` so they show up in
  `/openapi.json`; don't document endpoints by hand elsewhere.
- **The native engine is the only timeline writer** (decided 2026-10-13).
  The DailySlot ingest is gone; scheduling and override work belongs in the
  schedule/slot engine and TS's `native_schedule.clj`.
- **Never return or log secrets.** Media-source `connection-config` holds the
  Jellyfin API key; responses and logs must omit it, and playback URLs go
  through PV's own `/api/media/items/:id/stream` proxy.

## Common pitfalls

1. **`text = bigint` errors** usually mean a path `:id` was parsed with a
   `try`/`catch → -1` fallback and passed to a query that expects a number.
   Prefer routes with Malli coercion.
2. **Channel ids:** `channels.id` is a BIGINT primary key, `channels.uuid` is
   the external id used in `/stream/:uuid`, M3U, and XMLTV, and `channels.slug`
   is the curated name used in `channel:<slug>` tags. Don't mix them up.
3. **Show tags live on the show.** `channel:*`, `audience:*`, `genre:*` and the
   like are on the series item, not its episodes; filter by the top-level item.
4. **Slots with no collection or item** play everything tagged
   `channel:<slug>`. A channel with no `slug` makes the build throw on purpose
   rather than emit zero events.
5. **Reset vs. extend.** `POST /api/channels/:id/playout?from=now` restarts every
   show from episode one; only `?from=horizon` and `/api/playouts/ensure` are
   safe to run on a schedule.
6. **HLS segment length is set by the GOP, not `hls_time`.** FFmpeg cuts on
   keyframes, so the effective segment is `min(hls_time, GOP / fps)`. Profiles
   default `video.gop` to `fps × segment duration`; an override larger than
   that lengthens segments and causes stutter (NVENC's own default of 250
   frames did exactly this).
7. **The playlist manager owns the HLS sequence numbers.** Media-sequence must
   stay monotonic and discontinuity-sequence must increment when a
   discontinuity segment leaves the window; see the correctness rules in
   `docs/streaming-design.md` §9 before touching `streaming/playlist.clj`.
8. **Streaming needs real hardware to test.** The automated tests have no GPU
   or live FFmpeg; NVENC/VAAPI paths are only covered by command-generation
   tests.
