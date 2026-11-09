# Pseudovision plan

Last reviewed: 2026-10-13

This is the one place for ongoing work on Pseudovision: what's being worked on,
what's next, known issues, and the decisions that still shape the code. Delete
items once they're done (git history keeps them).

Pseudovision's plan also carries the **ecosystem plan**: priorities that span
several repos. Each other repo's `PLAN.md` has the same sections and points
back here.

## Ecosystem

Six repos make up the system: pseudovision (playout), tunarr-scheduler
(control plane), tunabrain (LLM gateway), grout (filler store), marquee (UI),
and pseudovision-cli. The current cleanup runs in four steps; each repo's plan
lists its own share of each step.

1. **Fix contained bugs.** Done (TS pagination, TS flake check, PV's
   unfinished HDHomeRun emulation removed in favor of M3U + XMLTV).
2. **Stop leaking secrets.** Done (Jellyfin API key out of PV responses and
   logs; Marquee's stack traces behind `ENABLE_UI_DEBUG_TRACES`).
3. **One timeline writer, then clear out dead code.** PV's native engine
   becomes the only writer. Monthly overrides become date-bounded manual
   events, and the DailySlot push is deleted from TS and PV. Then delete dead
   code (TS strategies and `llm.clj`, the TS TTS stub, duplicate import paths,
   one-time migrations, superseded endpoints, Tunabrain's strategy and
   `channel-mapping` endpoints, unused PV tables and helpers), gate PV's
   `/api/test/*` routes behind a dev flag, and wire up endpoints that are
   built but never called (`POST /api/sync/grout`,
   `POST /api/bumpers/generate`, Grout → `/enrich/long-form`).
4. **CI.** Add GitHub CI running `nix flake check` to PV, TS, Grout,
   Tunabrain, and Marquee (pvcli already has it).

**Later, ecosystem-wide:** auth on every public endpoint once development
winds down (Marquee and pvcli already send a bearer token); one channel
registry, in PV; one wire casing (kebab-case) across all APIs; PV reading
Grout clips over HTTP instead of a shared mount; merging pvcli and
`grout-cli`.

**Ecosystem decisions**

- 2026-10-13: PV's native engine is the only timeline writer.
- 2026-10-13: Services run at `*.kube.sea.fudo.link`, local-only behind NAT.
  Real auth waits until development winds down.
- 2026-10-13: M3U + XMLTV is how Jellyfin consumes PV; no HDHomeRun emulation.
- All AI calls go through Tunabrain. No other service holds model keys.
- Tunabrain's Pydantic models define the scheduling contracts
  (`tunabrain/docs/scheduling-grid-spec.md`).

## Now

### One timeline writer (ecosystem step 3)

- [ ] Support TS applying monthly overrides as date-bounded manual events for
      each override window (manual events already survive rebuilds; check that
      the engine resumes cleanly after an override window ends).

### Dead code (ecosystem step 3)

- [ ] Gate `/api/test/*` and `dev/test_channel.clj` behind a config flag; they
      can create a collection containing every media item.
- [ ] Drop unused tables and columns: `transcode_jobs`, `channels.gpu_pool`,
      `playout_history` (and its helpers), `playout_gaps`/`replace-gaps!`, and
      the never-used ErsatzTV-port tables (`graphics_elements`,
      `slot_graphics_elements`, `event_graphics_elements`, `language_codes`,
      `library_folders`, `media_chapters`, `metadata_artwork`,
      `metadata_external_ids`, `metadata_people`).
- [ ] Implement or delete `POST /api/catalog/count`; it ignores its filters
      and returns the total.
- [ ] Remove unused config and helpers: `scheduling.rebuild-interval-minutes`,
      `filler/pad-to-boundary`, unused `util/time.clj` functions,
      `streaming.source/get-jellyfin-stream-url`, and the unused schemas
      (`MediaItemKind`, `RebuildResult`, `StreamDebug`).

### Wiring and CI

- [ ] Run `POST /api/sync/grout` on a schedule (a CronJob), and have it prune
      programs deleted from Grout.
- [ ] Add GitHub CI that runs `nix flake check` (ecosystem step 4).

## Next

### Streaming (see [docs/streaming-design.md](docs/streaming-design.md))

- [ ] Hardware smoke tests: the live overlap handoff (Phase 2), `:auto` decode
      and NVENC paths (Phase 3-lite), and `pad_vaapi` letterboxing (Phase 3).
- [ ] GPU padding on NVENC (CUDA has no pad filter).
- [ ] Phase 4: a per-encoder watchdog that restarts a stalled encoder or
      splices in the fallback slate without tearing down the channel, and
      metrics that survive handoffs (`media_item_views` opened and closed at
      each splice).

### Playout

- [ ] De-duplicate concurrent rebuilds of the same channel (return the
      in-flight job instead of starting a second).
- [ ] Clips that Grout supersedes or deletes stay referenceable as filler
      until cleaned up; add a cleanup pass.

## Later

- EPG: honor `show_in_epg` and `guide_start_at`/`guide_finish_at` in XMLTV, and
  collapse multi-part events by `guide_group`.
- Watermarks: the schema has `watermark_id` but nothing applies an overlay.
- Direct streaming (no transcode) for compatible sources, and per-channel
  audio-language and subtitle preferences.
- Durable job history (jobs are in-memory today).
- Distributed transcoding (streaming design Phase 5), deferred until seamless
  transitions are proven.
- `rerun` collections (the resolver is a stub).

## Known issues

- Every endpoint is unauthenticated, including writes and deletes. Acceptable
  while `*.sea.fudo.link` is local-only behind NAT.
- The Grout media source is created lazily on first ingest without a unique
  constraint, so concurrent first builds could create duplicates.
- Gap filler with no preset (`grout-channel-fallback`) asks Grout for any
  non-bumper clip that fits, so long-form `program` items can land in a tail
  gap as filler. Decide whether the fallback should ask for `kind=filler` only.

## Decisions

- 2026-10-13: The native schedule engine is the only timeline writer; the
  DailySlot ingest goes away.
- 2026-10-13: HDHomeRun emulation was removed; Jellyfin uses the M3U tuner and
  XMLTV.
- Live streaming uses per-event encoders with a PV-owned playlist and overlap
  handoff (design Option B), not a single continuous encoder.
- Segment storage is local disk behind a `SegmentStore` protocol, so shared
  storage can drop in later.
