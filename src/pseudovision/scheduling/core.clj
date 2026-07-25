(ns pseudovision.scheduling.core
  "Playout build engine.

   Entry points:
     (build! db opts playout)     — core engine. Resume/extend by default
                                    (preserving the near term); pass
                                    :reset-cursor? true for a full rebuild.
     (rebuild-from-now! …)        — reset: wipe the future and regenerate from now
     (rebuild-horizon! …)         — extend the timeline forward to a horizon
     (ensure-horizon! …)          — daily top-up for one channel (extend)
     (ensure-all-horizons! …)     — daily top-up across every schedule-backed channel

   The engine walks the schedule's slots in order, advancing a cursor
   through the content collections and emitting playout_events rows.

   Slot fill modes:
     :once  — emit one item then advance to next slot
     :count — emit exactly N items then advance
     :block — fill a fixed duration, pad tail with filler, then advance
     :flood — fill until the next fixed-anchor slot, then advance

   Slot anchors:
     :fixed      — slot starts at a specific wall-clock time of day
     :sequential — slot starts immediately when the previous one ends"
  (:require [next.jdbc                   :as jdbc]
            [honey.sql                 :as sql]
            [honey.sql.helpers         :as h]
            [pseudovision.db.core      :as db-core]
            [pseudovision.db.channels  :as channels-db]
            [pseudovision.db.filler    :as filler-db]
            [pseudovision.db.media     :as media-db]
            [pseudovision.db.playouts  :as playout-db]
            [pseudovision.db.schedules :as schedules-db]
            [pseudovision.db.collections :as col-db]
            [pseudovision.media.grout        :as grout]
            [pseudovision.media.grout-source :as grout-source]
            [pseudovision.scheduling.cursor      :as cursor]
            [pseudovision.scheduling.enumerators :as enum]
            [pseudovision.scheduling.filler      :as filler]
            [pseudovision.scheduling.packing     :as packing]
            [pseudovision.util.sql  :as sql-util]
            [pseudovision.util.time :as t]
            [taoensso.timbre        :as log])
  (:import [java.time Duration Instant]))

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn- collection-key
  "Returns a stable string key identifying the content source for a slot.
   Used to key enumerator states in the cursor so each slot's position is
   tracked independently.  Channel-catalog slots (no collection-id, no
   media-item-id) get a key scoped to the channel so two different channels
   that both fall back to the channel-catalog path maintain separate
   enumerator positions."
  [slot channel]
  (cond
    (:schedule-slots/collection-id slot)
    (str "collection:" (:schedule-slots/collection-id slot))
    (:schedule-slots/media-item-id slot)
    (str "item:" (:schedule-slots/media-item-id slot))
    :else
    (str "channel-catalog:" (or (:channels/id channel) "unknown"))))

(defn- get-item-tags
  "Get all tags for a media item."
  [db media-item-id]
  (let [tags (db-core/query db (-> (h/select :mt.name)
                                   (h/from [:metadata-tags :mt])
                                   (h/join [:metadata :m] [:= :m.id :mt.metadata-id])
                                   (h/where [:= :m.media-item-id media-item-id])
                                   sql/format))]
    (set (map :metadata-tags/name tags))))

(defn- matches-tag-filters?
  "Returns true if item matches the required/excluded tag filters.
   - Must have ALL required tags (AND logic)
   - Must have NONE of the excluded tags (NOT logic)"
  [db item required-tags excluded-tags]
  (if (or (seq required-tags) (seq excluded-tags))
    (let [item-tags (get-item-tags db (:media-items/id item))]
      (and
       ;; Must have all required tags
       (every? #(contains? item-tags %) required-tags)
       ;; Must not have any excluded tags
       (not-any? #(contains? item-tags %) excluded-tags)))
    ;; If no tag filters, item matches
    true))

(defn- playable?
  "True when an item has a known, positive duration.

   Media that has not been probed yet carries a zero duration
   (media_versions.duration defaults to INTERVAL '0'), and a slot whose item
   has no media_versions row at all resolves to a nil duration. Either way the
   item has no playable length: emitting it would not advance the build's
   wall-clock cursor, and because the collection enumerators loop forever the
   block/flood fill loops would never terminate (a silent infinite loop). Such
   items are therefore excluded from scheduling — mirroring the filler
   bin-packer, which already skips non-positive-duration items."
  [item]
  (when-let [^Duration d (:media-versions/duration item)]
    (pos? (.getSeconds d))))

(defn- load-items
  "Returns the ordered seq of playable media items for a slot.
   Filters by required/excluded tags if specified, and drops items with no
   usable (positive) duration so they can't stall the build.

   Resolution paths, in order:
     1. `:schedule-slots/collection-id` — the slot is pinned to a specific
        collection, resolved through `col-db/resolve-collection`.
     2. `:schedule-slots/media-item-id` — the slot is pinned to one media
        item, fetched directly.
     3. Channel-catalog fallback (the new path) — when a slot has neither a
        collection-id nor a media-item-id, the schedule is implicitly asking
        for anything tagged for this channel.  The slot is resolved against
        the channel's `channel:<slug>` dimension tag through
        `media-db/resolve-playable-by-channel-tag`.  The lookup uses
        `:channels/slug`, NOT `:channels/name`, because the production
        `metadata_tags` namespace stores curated lowercase slugs (e.g.
        `channel:spectrum` for the channel whose display name is
        'Sitcom Spectrum') — the display-name form returned zero events
        on every rebuild for the four (auto) channels (verified live
        2026-07-17; see the #145 regression notes).  If `:channels/slug`
        is nil, this branch THROWS an `ex-info` carrying `:channel-id`,
        `:channel-name`, and a `:fix` hint rather than silently returning
        an empty vector.  Loud error is the contract — the pre-fix code's
        silent zero-event build was the bug we just fixed; if the data
        layer breaks again, the deploy log shows it immediately.  This is
        the contract for (auto) schedules whose slots carry no explicit
        content source — the pre-fix `:else []` branch silently returned
        an empty vector and every such slot produced zero events (see
        pitfall-51 in the pseudovision-ecosystem-development skill).  A
        slot whose `required-tags` filter strips every catalog item is
        still legitimately empty and produces zero events — the fallback
        only adds the resolution path, not content."
  [db slot channel]
  (let [required-tags (or (:schedule-slots/required-tags slot) [])
        excluded-tags (or (:schedule-slots/excluded-tags slot) [])
        slot-id       (:schedule-slots/id slot)
        items (cond
                (:schedule-slots/collection-id slot)
                (let [collection-id (:schedule-slots/collection-id slot)
                      coll (media-db/get-collection db collection-id)]
                  (log/info "Loading collection for slot"
                           {:slot-id slot-id
                            :collection-id collection-id
                            :collection-name (:collections/name coll)
                            :collection-kind (:collections/kind coll)})
                  (col-db/resolve-collection db coll))

                (:schedule-slots/media-item-id slot)
                (let [media-id (:schedule-slots/media-item-id slot)
                      item (media-db/get-media-item db media-id)]
                  (log/info "Loading single media item for slot"
                           {:slot-id slot-id
                            :media-id media-id
                            :media-name (when item (:media-items/name item))})
                  [item])

                :else
                ;; Channel-catalog fallback — see `load-items` docstring.
                ;; Reads the *slug* field (NOT the display name) because the
                ;; production tags are stored as curated slugs (e.g.
                ;; `channel:spectrum` for "Sitcom Spectrum"); using the
                ;; display name returned 0 items on every rebuild for the
                ;; four (auto) channels (verified live 2026-07-17 from the
                ;; PV pod logs).  There is intentionally no fallback to the
                ;; display name — if a channel has no slug, this branch
                ;; throws loud so the operator fixes the data instead of
                ;; letting the build silently emit zero events again.
                ;;
                ;; Test fixtures and dev scripts sometimes pass `{}` or
                ;; `nil` as the channel; those are "no channel in scope"
                ;; cases, not the data-layer bug PR #145 was created to
                ;; catch.  Both are treated as "no fallback available"
                ;; and the function returns [] silently — the pre-fix
                ;; contract for those callers.
                (cond
                  ;; No channel object: there is no slug to look up.
                  (or (nil? channel) (nil? (:channels/id channel)))
                  (do (log/warn "Slot has no content source and no channel; emitting nothing"
                                {:slot-id slot-id})
                      [])

                  ;; Channel object provided but lacks the curated slug:
                  ;; this is the data-layer bug; refuse to silently emit
                  ;; zero events and throw so deploy logs make it obvious.
                  (nil? (:channels/slug channel))
                  (do (log/error "Slot is on an auto schedule but its channel has no slug — refusing to silently emit zero events. Set channels.slug for this channel and rerun the rebuild."
                                {:slot-id slot-id
                                 :channel-id (:channels/id channel)
                                 :channel-name (:channels/name channel)})
                      (throw (ex-info "channel has no slug; cannot run channel-catalog fallback"
                                      {:slot-id slot-id
                                       :channel-id (:channels/id channel)
                                       :channel-name (:channels/name channel)
                                       :fix "SET channels.slug = '<curated-slug>' WHERE id = <channel-id>"})))

                  :else
                  (let [slug       (:channels/slug channel)
                        channel-tag (str "channel:" slug)]
                    (log/info "Loading channel-catalog items for slot"
                              {:slot-id slot-id
                               :channel-id (:channels/id channel)
                               :channel-slug slug
                               :channel-tag channel-tag})
                    (media-db/resolve-playable-by-channel-tag db channel-tag))))
        ;; Apply tag filters if specified
        tagged (if (or (seq required-tags) (seq excluded-tags))
                 (do
                   (log/info "Filtering items by tags"
                            {:slot-id slot-id
                             :required required-tags
                             :excluded excluded-tags
                             :before-count (count items)})
                   (let [filtered (filterv #(matches-tag-filters? db % required-tags excluded-tags) items)]
                     (log/info "Items after tag filtering"
                              {:slot-id slot-id
                               :after-count (count filtered)
                               :filtered-out (- (count items) (count filtered))})
                     filtered))
                 (vec items))
        playable (filterv playable? tagged)]
    (when (< (count playable) (count tagged))
      (log/warn "Skipping unplayable items with zero/unknown duration"
                {:slot-id slot-id
                 :dropped (- (count tagged) (count playable))}))
    (log/info "Loaded items for slot (final)"
             {:slot-id slot-id
              :count (count playable)})
    playable))

(defn- item-duration
  "Returns the item's playback duration, or zero if the item has not been probed."
  [item]
  (or (some-> (:media-versions/duration item))
      (Duration/ofSeconds 0)))

(defn- events-end
  "Returns the finish time of the last event in `events`, or `default` when
   `events` is empty.  Used to advance the cursor past injected filler."
  [events default]
  (if (seq events)
    (:finish-at (last events))
    default))

(defn- point-filler-window
  "For an inline (pre/mid/post) filler insert starting at `from`, returns the
   instant the fill should run until.  Unlike gap filler (tail/fallback), the
   length of an inline insert is dictated by the preset, not by an external
   deadline:
     - duration mode      → from + preset duration
     - pad_to_minute mode → next n-minute boundary
     - count modes        → no intrinsic deadline; fill-gap stops after N items
   `ceil` (may be nil) caps the window so the insert can't overrun the
   surrounding block."
  [preset from ceil]
  (let [target (case (:filler-presets/mode preset)
                 "duration"      (if-let [d (:filler-presets/duration preset)]
                                   (t/add-duration from d)
                                   (or ceil from))
                 "pad_to_minute" (if-let [n (:filler-presets/pad-to-nearest-minute preset)]
                                   (t/ceil-to-n-minutes from n)
                                   (or ceil from))
                 ;; count / random_count: fill-gap counts items and ignores the
                 ;; boundary, so any in-range value works.
                 (or ceil from))]
    (if (and ceil (.isAfter target ceil)) ceil target)))

(defn- lay-out-filler
  "Turns an ordered seq of filler items into back-to-back event maps starting at
   `from`.  Stamps guide-group / slot-id so the rows share the content event
   column set (guide_group is NOT NULL)."
  [from items role playout-id guide slot-id]
  (loop [t from, items (seq items), events []]
    (if (empty? items)
      events
      (let [it  (first items)
            dur (or (:media-versions/duration it) (Duration/ofSeconds 0))
            fin (t/add-duration t dur)]
        (recur fin (rest items)
               (conj events {:playout-id    playout-id
                             :media-item-id (:media-items/id it)
                             :kind          (sql-util/->pg-enum "event_kind" (name role))
                             :start-at      t
                             :finish-at     fin
                             :guide-group   guide
                             :slot-id       slot-id
                             :is-manual     false}))))))

(defn- pack-filler
  "Variety-oriented fill of [from, until] using the bin-packer.  Selection is
   biased away from items that aired recently anywhere (global recency, carried
   in opts as :filler-airings-atom + :recency-window) and seeded per-gap so
   different breaks differ while rebuilds stay stable.  Records the chosen
   airings back into the recency atom so later gaps in this build space away
   from them too.  Returns [events cursor] (the packer keeps no enumerator
   state, so the cursor is returned unchanged)."
  [cursor slot role from until items playout-id opts]
  (let [airings-atom (:filler-airings-atom opts)
        window       (:recency-window opts)
        recency      (if (and airings-atom window)
                       (packing/airing-penalties @airings-atom from window)
                       {})
        seed         (bit-xor (long (get opts :seed 0)) (.getEpochSecond ^Instant from))
        target       (t/duration-between from until)
        picked       (packing/pack target items
                                   :seed seed
                                   :recency recency
                                   :tolerance (:filler-tolerance opts))
        events       (lay-out-filler from picked role playout-id
                                     (:next-guide-group cursor)
                                     (:schedule-slots/id slot))]
    (when (and airings-atom window)
      (swap! airings-atom
             (fn [m] (reduce (fn [acc ev]
                               (update acc (:media-item-id ev) (fnil conj []) (:finish-at ev)))
                             m events))))
    [events cursor]))

(defn- grout-channel-fallback
  "Last-resort gap fill for a `role` that has NO filler preset configured on the
   slot or channel.  Rather than leave dead air, pull channel-mapped clips
   straight from Grout — any media Grout has tagged for this channel that fits
   the gap [from, to] — and pack the window with them.

   This is the \"filler mapped to channel\" contract: Grout already scopes filler
   by channel, so a PV channel doesn't need a per-channel filler collection or a
   `fallback_filler_id` wired up for its gaps to fill.  A synthetic empty preset
   (no grout-tags) makes the query channel + gap-duration only, i.e. \"any clip
   for this channel that fits.\"

   Best-effort: returns [[] cursor] (a genuine gap, exactly as before) when Grout
   is disabled/unreachable, the window is empty, or Grout returns nothing."
  [db cursor slot channel role from to playout-id opts]
  (let [grout (:grout opts)]
    (if (and to
             (grout/enabled? grout)
             (.isBefore ^Instant from ^Instant to))
      (let [items (filterv playable?
                           (grout-source/grout-filler-items db grout channel from to {}))]
        (if (seq items)
          (do (log/info "Filling gap with channel-mapped Grout filler (no preset configured)"
                        {:slot-id (:schedule-slots/id slot)
                         :channel (:channels/name channel)
                         :role    role
                         :candidates (count items)})
              (pack-filler cursor slot role from to items playout-id opts))
          [[] cursor]))
      [[] cursor])))

(defn- apply-filler
  "Resolves the filler preset for `role`, loads its items, emits filler events,
   and returns [filler-events updated-cursor].  When no preset is configured for
   the role on this slot/channel, gap-filling roles (:tail, :fallback, :post)
   fall back to channel-mapped Grout filler (see grout-channel-fallback); other
   roles, or a disabled/empty Grout, yield [[] cursor] (a genuine gap).

   The fill window depends on the role:
     - gap roles (:tail, :fallback) fill the interval [from, to] exactly.
     - inline roles (:pre, :mid, :post) fill a preset-sized chunk starting at
       `from`, with `to` acting as an upper bound (it may be nil for slots that
       have no natural deadline, e.g. count-mode slots).

   For duration-mode presets, when packing is enabled (opts :pack-filler?, set
   by build!), the gap is bin-packed for variety; otherwise the original
   cursor-managed enumerator fill is used (count mode always uses it).

   Small tail gaps (≤ 15 seconds) are preferentially filled with auto-generated
   bumper items sourced from Grout, when a suitable one exists for the
   channel."
  [db cursor slot channel role from to playout-id opts]
  (or
   ;; --- Bumper priority for small tail gaps ---
   ;; `or`'d with the regular-filler branch below so a real [events cursor]
   ;; short-circuits it; a nil (no small gap, or Grout had nothing in range)
   ;; falls through to regular filler.
   (when (and (= role :tail) to)
     (when-let [bumper-events (filler/fill-gap-with-bumper
                               db (:grout opts) channel from to playout-id)]
       (let [guide   (:next-guide-group cursor)
             slot-id (:schedule-slots/id slot)
             events  (mapv #(assoc % :guide-group guide :slot-id slot-id)
                           bumper-events)]
         (log/debug "Injected bumper for small gap"
                    {:channel (:channels/name channel)
                     :gap-secs (t/duration->seconds (t/duration-between from to))
                     :count (count events)})
         [events cursor])))
   ;; --- Regular filler ---
   (if-let [preset-ref (filler/resolve-filler-preset role slot channel)]
     (if-let [preset (filler-db/get-filler-preset db (:id preset-ref))]
       ;; Drop unplayable (zero/unknown duration) filler items up front so the
       ;; enumerator and fill-gap below share the same item vector and the
       ;; duration-mode fill loop can't spin on a zero-length item.
       (let [until (if (#{:pre :mid :post} role)
                     (point-filler-window preset from to)
                     to)
             ;; A preset carrying grout-tags draws its candidates from Grout
             ;; (queried by channel + tags + the gap duration), ingested on the
             ;; fly as local-path media items; otherwise use its local collection
             ;; / media item.  Both paths yield the same candidate shape, so the
             ;; packer / enumerator below are unchanged.
             items (filterv playable?
                            (if (seq (:filler-presets/grout-tags preset))
                              (grout-source/grout-filler-items db (:grout opts) channel from until preset)
                              (filler-db/load-filler-items db preset)))]
         (if (and (:pack-filler? opts)
                  (= "duration" (:filler-presets/mode preset)))
           (pack-filler cursor slot role from until items playout-id opts)
           (let [ckey    (str "filler:" (name role) ":" (:filler-presets/id preset))
                 e       (cursor/get-enumerator cursor ckey items :random {:seed (get opts :seed 0)})
                 result  (filler/fill-gap from until preset items e playout-id)
                 ;; fill-gap emits the bare content columns; stamp the same
                 ;; guide-group / slot-id that content events carry so every row
                 ;; bulk-inserted in build! has an identical column set.
                 guide   (:next-guide-group cursor)
                 slot-id (:schedule-slots/id slot)
                 events  (mapv #(assoc % :guide-group guide :slot-id slot-id)
                               (:events result))
                 cursor' (cursor/save-enumerator cursor ckey (:enumerator result))]
             [events cursor'])))
       ;; preset-ref resolved but the row vanished — treat as unconfigured.
       (grout-channel-fallback db cursor slot channel role from to playout-id opts))
     ;; No preset configured for this role at slot or channel level: for the
     ;; gap-filling roles, fall back to channel-mapped Grout filler so the gap
     ;; doesn't become dead air.  Inline roles (:pre, :mid) keep the old "nothing"
     ;; behaviour — they decorate content, they don't pad dead air.
     (if (#{:tail :fallback :post} role)
       (grout-channel-fallback db cursor slot channel role from to playout-id opts)
       [[] cursor]))))

(defn- next-fixed-start
  "Returns the next wall-clock Instant when `slot` would fire on or after `after`.
   Respects the slot's days_of_week bitmask: skips days whose bit is not set.
   A nil or 0 mask means every day (preserves original behaviour)."
  [slot after zone-id]
  (let [tod      (:schedule-slots/start-time slot)   ; Duration = offset from midnight
        dow-mask (:schedule-slots/days-of-week slot)] ; nil → every day
    (t/next-dow-occurrence tod dow-mask after zone-id)))

;; ---------------------------------------------------------------------------
;; Event emission for each fill mode
;; ---------------------------------------------------------------------------

(defn emit-once
  "Emit one item, return [events cursor]."
  [db cursor slot channel playout-id opts]
  (let [items   (load-items db slot channel)
        ckey    (collection-key slot channel)
        order   (keyword (or (:schedule-slots/playback-order slot) "chronological"))
        enum-opts {:seed (get opts :seed 0)
                   :batch-size (or (:schedule-slots/marathon-batch-size slot) 5)}]
    (if (empty? items)
      (do (log/warn "emit-once: no items for slot; skipping"
                    {:slot-id (:schedule-slots/id slot)})
          [[] cursor])
      (let [e         (cursor/get-enumerator cursor ckey items order enum-opts)
            [item e'] (enum/next-item e)
            dur       (item-duration item)
            from      (:next-start cursor)
            to        (t/add-duration from dur)
            event     {:playout-id    playout-id
                       :media-item-id (:media-items/id item)
                       :kind          (sql-util/->pg-enum "event_kind" "content")
                       :start-at      from
                       :finish-at     to
                       :guide-group   (:next-guide-group cursor)
                       :slot-id       (:schedule-slots/id slot)
                       :is-manual     false}
            cursor'   (-> cursor
                          (assoc :next-start to)
                          (cursor/save-enumerator ckey e')
                          (cursor/bump-guide-group))]
        (log/info "emit-once: emitted item"
                 {:slot-id (:schedule-slots/id slot)
                  :media-name (:media-items/name item)
                  :duration (str dur)
                  :start-at (str from)
                  :finish-at (str to)})
        [[event] cursor']))))

(defn emit-count
  "Emit exactly item-count items, return [events cursor].
   Injects pre-roll filler before the first item, mid-roll filler between
   items, and post-roll filler after the last item, when configured.  A count
   slot has no time deadline, so inline filler is sized purely by its preset."
  [db cursor slot channel playout-id opts]
  (let [items  (load-items db slot channel)
        ckey   (collection-key slot channel)
        order  (keyword (or (:schedule-slots/playback-order slot) "chronological"))
        enum-opts {:seed (get opts :seed 0)
                   :batch-size (or (:schedule-slots/marathon-batch-size slot) 5)}
        n      (or (:schedule-slots/item-count slot) 1)
        guide  (:next-guide-group cursor)]
    (if (empty? items)
      (do (log/warn "emit-count: no items for slot; skipping"
                    {:slot-id (:schedule-slots/id slot)})
          [[] cursor])
      (let [start              (:next-start cursor)
            [pre-events cur0]  (apply-filler db cursor slot channel
                                             :pre start nil playout-id opts)
            pre-end            (events-end pre-events start)]
        (loop [i      0
               from   pre-end
               cur    cur0
               e      (cursor/get-enumerator cur0 ckey items order enum-opts)
               events (vec pre-events)]
          (if (>= i n)
            (let [base               (cursor/save-enumerator cur ckey e)
                  [post-events cur1] (apply-filler db base slot channel
                                                   :post from nil playout-id opts)
                  cursor'            (-> cur1
                                         (assoc :next-start (events-end post-events from))
                                         (cursor/bump-guide-group))]
              [(into events post-events) cursor'])
            (let [[item e'] (enum/next-item e)
                  dur       (item-duration item)
                  to        (t/add-duration from dur)
                  content   {:playout-id    playout-id
                             :media-item-id (:media-items/id item)
                             :kind          (sql-util/->pg-enum "event_kind" "content")
                             :start-at      from
                             :finish-at     to
                             :guide-group   guide
                             :slot-id       (:schedule-slots/id slot)
                             :is-manual     false}
                  ;; mid-roll filler sits between items, never after the last.
                  [mid-events curm] (if (< (inc i) n)
                                      (apply-filler db cur slot channel
                                                    :mid to nil playout-id opts)
                                      [[] cur])]
              (recur (inc i) (events-end mid-events to) curm e'
                     (into events (into [content] mid-events))))))))))
(defn emit-block
  "Fill a fixed-duration block.  Injects pre-roll filler before the first item,
   mid-roll filler between items, post-roll filler after the last item, and
   pads any remaining time with tail filler.  Returns [events cursor]."
  [db cursor slot channel playout-id opts]
  (let [items      (load-items db slot channel)
        ckey       (collection-key slot channel)
        order      (keyword (or (:schedule-slots/playback-order slot) "chronological"))
        enum-opts  {:seed (get opts :seed 0)
                    :batch-size (or (:schedule-slots/marathon-batch-size slot) 5)}
        block-dur  (:schedule-slots/block-duration slot)
        from       (:next-start cursor)
        block-end  (t/add-duration from block-dur)
        guide      (:next-guide-group cursor)
        ;; Pre-roll filler fires once, before the first content item, bounded
        ;; by the block end so it can't consume the whole block.
        [pre-events cur0] (apply-filler db cursor slot channel
                                        :pre from block-end playout-id opts)]
    (loop [cursor-time (events-end pre-events from)
           cur         cur0
           e           (cursor/get-enumerator cur0 ckey items order enum-opts)
           skipped     0
           events      (vec pre-events)]
      (let [remaining (t/duration-between cursor-time block-end)]
        (cond
          ;; Block finished
          (or (.isNegative remaining) (.isZero remaining))
          (let [c' (-> cur
                       (assoc :next-start block-end)
                       (cursor/save-enumerator ckey e)
                       (cursor/bump-guide-group))]
            (log/info "emit-block: block filled"
                     {:slot-id (:schedule-slots/id slot)
                      :block-duration (str block-dur)
                      :total-events (count events)
                      :fill-mode "completed"})
            [events c'])

          ;; Either no content at all, or a full pass over the pool found
          ;; nothing short enough to still fit the block. How the remainder is
          ;; handled depends on tail_mode.
          (or (empty? (:items e))
              (>= skipped (count (:items e))))
          (let [tail-mode (:schedule-slots/tail-mode slot "none")]
            (cond
              ;; tail_mode none, with content still waiting: trim the next item
              ;; to fill the block exactly, then replay it in full at the next
              ;; block start (enumerator saved unadvanced).
              (and (= "none" tail-mode) (seq (:items e)))
              (let [[item _] (enum/next-item e)
                    trimmed  {:playout-id    playout-id
                              :media-item-id (:media-items/id item)
                              :kind          (sql-util/->pg-enum "event_kind" "content")
                              :start-at      cursor-time
                              :finish-at     block-end
                              :guide-group   guide
                              :slot-id       (:schedule-slots/id slot)
                              :is-manual     false}
                    c' (-> (cursor/save-enumerator cur ckey e)
                           (assoc :next-start block-end)
                           (cursor/bump-guide-group))]
                [(conj events trimmed) c'])

              ;; tail_mode offline: leave the remainder dark (no filler).
              (= "offline" tail-mode)
              (let [c' (-> (cursor/save-enumerator cur ckey e)
                           (assoc :next-start block-end)
                           (cursor/bump-guide-group))]
                [events c'])

              ;; tail_mode filler (or none with nothing left to trim): post-roll
              ;; then pad the rest with tail filler.
              :else
              (let [base-cursor (cursor/save-enumerator cur ckey e)
                    [post-events cur1] (apply-filler db base-cursor slot channel
                                                     :post cursor-time block-end
                                                     playout-id opts)
                    post-end (events-end post-events cursor-time)
                    [tail-events cur2] (apply-filler db cur1 slot channel
                                                     :tail post-end block-end
                                                     playout-id opts)
                    c' (-> cur2
                           (assoc :next-start block-end)
                           (cursor/bump-guide-group))]
                [(into events (into (vec post-events) tail-events)) c'])))

          :else
          (let [[item e'] (enum/next-item e)
                dur       (item-duration item)
                to        (t/add-duration cursor-time dur)]
            (if (.isAfter to block-end)
              ;; Too long for what's left of the block. Skip past it and keep
              ;; packing shorter content — abandoning the block on the first
              ;; overflow stranded the shorter episodes still in the pool (the
              ;; "one episode then dead air" symptom). The enumerator advances
              ;; (e') so the oversized item comes around on a later block;
              ;; `skipped` bounds the scan to one pool cycle. The final remainder
              ;; is handled per tail_mode in the branch above.
              (recur cursor-time cur e' (inc skipped) events)
              ;; Item fits — emit it, then inject mid-roll filler before the
              ;; next item (only when more content remains).
              (let [content {:playout-id    playout-id
                             :media-item-id (:media-items/id item)
                             :kind          (sql-util/->pg-enum "event_kind" "content")
                             :start-at      cursor-time
                             :finish-at     to
                             :guide-group   guide
                             :slot-id       (:schedule-slots/id slot)
                             :is-manual     false}
                    [mid-events curm] (if (seq (:items e'))
                                        (apply-filler db cur slot channel
                                                      :mid to block-end playout-id opts)
                                        [[] cur])]
                (recur (events-end mid-events to) curm e' 0
                       (into events (into [content] mid-events)))))))))))

(defn emit-flood
  "Fill from now until the next fixed-anchor slot.
   Returns [events cursor]."
  [db cursor slot channel playout-id {:keys [flood-end] :as opts}]
  ;; Flood fills from :next-start up to flood-end (the next fixed-anchor time).
  (let [items  (load-items db slot channel)
        ckey   (collection-key slot channel)
        order  (keyword (or (:schedule-slots/playback-order slot) "chronological"))
        enum-opts {:seed (get opts :seed 0)
                   :batch-size (or (:schedule-slots/marathon-batch-size slot) 5)}
        guide  (:next-guide-group cursor)
        end    (or flood-end
                   (t/add-duration (:next-start cursor) (t/hours->duration 2)))]
    (loop [cursor-time (:next-start cursor)
           e           (cursor/get-enumerator cursor ckey items order enum-opts)
           skipped     0
           events      []]
      (let [remaining (t/duration-between cursor-time end)]
        (cond
          (or (.isNegative remaining) (.isZero remaining))
          (let [c' (-> cursor
                       (assoc :next-start end)
                       (cursor/save-enumerator ckey e)
                       (cursor/bump-guide-group))]
            (log/info "emit-flood: horizon reached"
                     {:slot-id (:schedule-slots/id slot)
                      :total-events (count events)
                      :fill-reason "time-exhausted"
                      :flood-duration (str (t/duration-between (:next-start cursor) end))})
            [events c'])

          ;; Either there is no content at all, or a full pass over the pool
          ;; found nothing short enough to still fit the shrinking window. Pad
          ;; the remainder [cursor-time, end) with fallback filler.
          (or (empty? (:items e))
              (>= skipped (count (:items e))))
          (let [base-cursor (cursor/save-enumerator cursor ckey e)
                [fill-events cursor'] (apply-filler db base-cursor slot channel
                                                    :fallback cursor-time end
                                                    playout-id opts)
                c' (-> cursor'
                       (assoc :next-start end)
                       (cursor/bump-guide-group))]
            (log/info "emit-flood: content packed, remainder padded"
                     {:slot-id (:schedule-slots/id slot)
                      :total-events (count (into events fill-events))
                      :fill-reason (if (empty? (:items e)) "items-exhausted" "no-item-fits")
                      :content-events (count events)
                      :filler-events (count fill-events)})
            [(into events fill-events) c'])

          :else
          (let [[item e'] (enum/next-item e)
                dur       (item-duration item)
                to        (t/add-duration cursor-time dur)]
            (if (.isAfter to end)
              ;; This item is too long for what's left of the window. Rather
              ;; than abandoning the rest of the slot to filler — which stranded
              ;; every shorter item still waiting in the pool and produced the
              ;; "one episode then dead air" symptom — skip past it and keep
              ;; packing. The enumerator advances (e') so the oversized item
              ;; comes around again on a later pass; `skipped` bounds the scan to
              ;; one full pool cycle so a window that nothing fits can't spin.
              (recur cursor-time e' (inc skipped) events)
              (recur to e' 0
                     (conj events {:playout-id    playout-id
                                   :media-item-id (:media-items/id item)
                                   :kind          (sql-util/->pg-enum "event_kind" "content")
                                   :start-at      cursor-time
                                   :finish-at     to
                                   :guide-group   guide
                                   :slot-id       (:schedule-slots/id slot)
                                   :is-manual     false})))))))))

;; ---------------------------------------------------------------------------
;; Slot dispatch
;; ---------------------------------------------------------------------------

(defn- next-slot-start
  "If the next slot is fixed-anchor, returns its next fire time;
   otherwise returns nil."
  [slots slot-idx now zone-id]
  (let [next-idx (inc slot-idx)
        next     (when (< next-idx (count slots)) (nth slots next-idx))]
    (when (= "fixed" (some-> next :schedule-slots/anchor))
      (next-fixed-start next now zone-id))))

(defn- process-slot
  "Processes one slot, advancing cursor and collecting events.
   Returns [events cursor].

   If the slot has a days_of_week mask and the current cursor time falls on a
   non-firing day, no events are emitted and next-start is fast-forwarded to
   the slot's next valid occurrence.  The enumerator position is left unchanged
   so sequential shows (e.g. Mad Men MWF) resume from exactly where they left
   off on the next airing day."
  [db cursor slot channel playout-id slots opts]
  (let [zone-id  (get opts :zone-id (t/default-zone-id))
        dow-mask (:schedule-slots/days-of-week slot)
        now      (:next-start cursor)]
    (log/info "Processing slot"
             {:slot-index (:schedule-slots/slot-index slot)
              :slot-id (:schedule-slots/id slot)
              :fill-mode (:schedule-slots/fill-mode slot)
              :anchor (:schedule-slots/anchor slot)
              :cursor-time (str now)})
    (if (and (= "fixed" (:schedule-slots/anchor slot))
             (not (t/fires-on-day? dow-mask now zone-id)))
      ;; This slot doesn't air today -- advance to its next valid fire time.
      (let [next-fire (next-fixed-start slot now zone-id)]
        (log/info "Slot skipped (days_of_week)"
                 {:slot-index (:schedule-slots/slot-index slot)
                  :slot-id (:schedule-slots/id slot)
                  :next-fire (str next-fire)})
        [[] (assoc cursor :next-start next-fire)])
      ;; Normal path -- slot fires today (or is sequential).
      (let [fill (keyword (or (:schedule-slots/fill-mode slot) "once"))
            [events cursor'] (case fill
                               :once  (emit-once  db cursor slot channel playout-id opts)
                               :count (emit-count db cursor slot channel playout-id opts)
                               :block (emit-block db cursor slot channel playout-id opts)
                               :flood (let [flood-end (next-slot-start
                                                       slots
                                                       (:schedule-slots/slot-index slot)
                                                       now
                                                       zone-id)]
                                        (emit-flood db cursor slot channel playout-id
                                                    (assoc opts :flood-end flood-end)))
                               (do (log/warn "Unknown fill mode, skipping slot" {:fill fill})
                                   [[] cursor]))]
        (log/info "Slot processed"
                 {:slot-index (:schedule-slots/slot-index slot)
                  :slot-id (:schedule-slots/id slot)
                  :fill-mode fill
                  :events-generated (count events)
                  :new-cursor-time (str (:next-start cursor'))})
        [events cursor']))))

;; ---------------------------------------------------------------------------
;; Public API
;; ---------------------------------------------------------------------------

(defn build!
  "Builds the playout, replacing non-manual events from the rebuild point to the
   horizon.  Runs inside a transaction; saves the updated cursor on success.

   Two modes:

   • Resume / extend (default) — pick up from the playout's saved cursor and
     extend the timeline forward to the horizon, WITHOUT disturbing the events
     already scheduled before the cursor.  The delete boundary is the cursor's
     own edge (where the previous build stopped), not `now`, so the near-term
     timeline — and every show's rotation position — is preserved.  This is the
     path the daily top-up uses (see ensure-horizon!): each show keeps advancing
     from where it left off.  A cursor that has fallen into the past (the
     channel ran dry) is fast-forwarded to `now`, so the resumed build fills
     from now rather than replaying stale past times; its enumerator states are
     kept so rotation still continues.  Without this fast-forward the
     delete-then-resume left a gap between now and the old horizon.

   • Reset (opts :reset-cursor? true) — discard the saved cursor and generate a
     fresh timeline from now: the schedule restarts at its first slot and every
     collection from the top.  To avoid cutting off the program currently on
     screen, the fresh timeline (and the delete boundary) begins at the finish
     of the in-progress event rather than exactly now."
  [db opts playout]
  (let [channel-id  (:playouts/channel-id playout)
        schedule-id (:playouts/schedule-id playout)
        playout-id  (:playouts/id playout)
        seed        (:playouts/seed playout 0)
        channel     (when channel-id
                      (channels-db/get-channel db channel-id))
        schedule    (when schedule-id
                      (schedules-db/get-schedule db schedule-id))
        slots       (when schedule-id
                      (schedules-db/list-slots db schedule-id))
        now         (t/now)
        reset?      (boolean (:reset-cursor? opts))
        saved-cursor (cursor/<-json (:playouts/cursor playout))
        ;; On a resume build the timeline is extended forward from where the last
        ;; build stopped — the saved cursor's edge — rather than from now, so the
        ;; already-generated near-term events (and rotation state) survive. A
        ;; cursor that has slipped into the past (dry channel) is clamped up to
        ;; now so we fill from now instead of the stale past edge.
        resume-edge (let [e (:next-start saved-cursor)]
                      (if (and e (.isAfter ^Instant e now)) e now))
        ;; When resetting, start the fresh timeline after any event currently on
        ;; air so the rebuild doesn't overlap (or interrupt) the program playing
        ;; right now; fall back to `now` when nothing is in progress.
        rebuild-from (if reset?
                       (or (some-> (playout-db/get-current-event db playout-id now)
                                   :playout-events/finish-at)
                           now)
                       resume-edge)
        horizon     (t/add-duration now (t/hours->duration
                                         (get opts :lookahead-hours 72)))
        opts'       (assoc opts :seed seed)]
    (log/info "Build starting"
             {:playout-id playout-id
              :channel-id channel-id
              :schedule-id schedule-id
              :schedule-name (:schedules/name schedule)
              :slot-count (count slots)
              :reset-cursor? reset?
              :rebuild-from (str rebuild-from)
              :lookahead-hours (get opts :lookahead-hours 72)
              :horizon (str horizon)})
    (if (or (nil? schedule) (empty? slots))
      (do (log/warn "No schedule or slots; nothing to build"
                    {:playout-id playout-id})
          :no-schedule)
      (let [initial-cur  (if reset?
                           (cursor/init rebuild-from)
                           ;; Resume from the saved cursor, but fast-forward its
                           ;; wall-clock edge up to `resume-edge` (= now for a dry
                           ;; channel) so a stale past position doesn't replay old
                           ;; times. Enumerator states are kept, so rotation
                           ;; continues from where it left off.
                           (if saved-cursor
                             (assoc saved-cursor :next-start resume-edge)
                             (cursor/init now)))
            window       (get opts :recency-window (t/hours->duration 4))]
        (jdbc/with-transaction [tx db]
          ;; Remove auto-generated events from the rebuild point forward. On a
          ;; reset that's the in-progress event's finish (preserving what's on
          ;; air); on a resume it's the saved cursor's edge (clamped up to now for
          ;; a dry channel), so the already-built near-term timeline survives.
          (playout-db/delete-non-manual-events-after! tx playout-id rebuild-from)

          ;; Seed global filler recency from what is already scheduled on every
          ;; OTHER channel across the build window. This playout's own future
          ;; filler was just deleted above, so it won't double-count; in-build
          ;; picks are appended to this atom as we go (see pack-filler).
          (let [airings (playout-db/recent-filler-airings
                         tx (t/sub-duration now window) horizon)
                opts'   (assoc opts' :pack-filler? (get opts :pack-filler? true)
                                     :recency-window window
                                     :filler-airings-atom (atom airings))]
          (loop [cursor       initial-cur
                 slot-idx     0
                 cycle-anchor nil
                 events       []]
            (let [slot  (nth slots slot-idx nil)
                  start (:next-start cursor)]
              (cond
                (or (nil? slot) (.isAfter start horizon))
                ;; Done; flush events and save cursor
                (do
                  (playout-db/bulk-insert-events! tx events)
                  (playout-db/update-playout! tx playout-id
                                              {:cursor         (cursor/->json cursor)
                                               :last-built-at  now
                                               :build-success  true
                                               :build-message  nil})
                  (log/info "Build complete" {:playout-id playout-id
                                              :events     (count events)})
                  (count events))

                ;; Safety net: if a full pass over every slot left the cursor
                ;; exactly where it started, the schedule can't make forward
                ;; progress (e.g. every slot resolves to no schedulable
                ;; content). Without this, build! would loop forever and the
                ;; HTTP request would hang with no response. Abort and record
                ;; the failure instead.
                (and (zero? slot-idx)
                     (some? cycle-anchor)
                     (not (.isAfter start cycle-anchor)))
                (do
                  (log/warn "Build stalled: timeline did not advance over a full slot cycle; aborting"
                            {:playout-id playout-id :at (str start) :slots (count slots)})
                  (playout-db/bulk-insert-events! tx events)
                  (playout-db/update-playout! tx playout-id
                                              {:cursor         (cursor/->json cursor)
                                               :last-built-at  now
                                               :build-success  false
                                               :build-message  "Build stalled: no schedulable content advanced the timeline"})
                  (count events))

                ;; Process this slot
                :else
                (let [[cursor''' next-slot-idx next-cycle-anchor next-events]
                      (try
                        (let [[new-events cursor'] (process-slot
                                                     tx cursor slot channel
                                                     playout-id slots opts')
                              fill-mode   (keyword (or (:schedule-slots/fill-mode slot) "once"))
                              zone-id     (get opts' :zone-id (t/default-zone-id))
                              next-slot   (nth slots (mod (inc slot-idx) (count slots)) nil)
                              gap-end     (when (and (not= fill-mode :flood)
                                                     (= "fixed" (:schedule-slots/anchor next-slot)))
                                            (t/time-of-day-on-same-day
                                             (:schedule-slots/start-time next-slot)
                                             (:next-start cursor')
                                             zone-id))
                              [gap-events cursor'']
                              (if gap-end
                                (apply-filler tx cursor' {} channel
                                              :fallback (:next-start cursor') gap-end
                                              playout-id opts')
                                [[] cursor'])
                              cursor''' (cursor/advance-slot cursor'' (count slots))]
                          [cursor''' (mod (inc slot-idx) (count slots))
                           (if (zero? slot-idx) start cycle-anchor)
                           (into events (into new-events gap-events))])
                        (catch Exception e
                          (log/error "Error processing slot; build failed"
                                    {:playout-id playout-id
                                     :slot-idx slot-idx
                                     :slot-id (:schedule-slots/id slot)
                                     :error (str e)
                                     :error-type (class e)})
                          (let [error-msg (str "Build failed at slot " slot-idx ": " (ex-message e))]
                            (playout-db/bulk-insert-events! tx events)
                            (playout-db/update-playout! tx playout-id
                                                        {:cursor         (cursor/->json cursor)
                                                         :last-built-at  now
                                                         :build-success  false
                                                         :build-message  error-msg})
                            (throw (ex-info error-msg
                                           {:slot-idx slot-idx
                                            :slot-id (:schedule-slots/id slot)
                                            :playout-id playout-id
                                            :cause e})))))]
                  (recur cursor''' next-slot-idx next-cycle-anchor next-events)))))))))))

(defn rebuild-from-now!
  "Wipe the auto-generated timeline from now onward and regenerate it fresh,
   starting the schedule over from its first slot.  Used when configuration
   changes (or to recover a playout whose cursor has run ahead).

   Passes :reset-cursor? so build! discards the saved cursor instead of
   resuming from it — without this the rebuild would silently restart wherever
   the previous build left off (typically the old horizon), leaving a gap
   between now and that point.

   `opts` (optional) is merged into the build options — notably :grout, the
   Grout client used to resolve Grout-backed filler presets.

   Returns number of events generated."
  ([ds playout-id horizon-days] (rebuild-from-now! ds playout-id horizon-days nil))
  ([ds playout-id horizon-days opts]
   (let [playout (playout-db/get-playout ds playout-id)]
     (if playout
       (let [result (build! ds (merge opts {:lookahead-hours (* horizon-days 24)
                                            :reset-cursor?   true}) playout)]
         (if (= result :no-schedule) 0 result))
       0))))

(defn rebuild-horizon!
  "Extend a playout's timeline forward to `new-horizon-days` from now, preserving
   the events already scheduled before the saved cursor (and every show's
   rotation).  Resumes from the saved cursor via build!'s default (non-reset)
   mode — see build!'s docstring for the resume/extend semantics, including how a
   dry channel is refilled from now.  `current-horizon-days` is accepted for call
   compatibility but ignored; the horizon is always measured from now.
   `opts` (optional) is merged into the build options (see rebuild-from-now!).
   Returns number of events generated."
  ([ds playout-id current-horizon-days new-horizon-days]
   (rebuild-horizon! ds playout-id current-horizon-days new-horizon-days nil))
  ([ds playout-id _current-horizon-days new-horizon-days opts]
   (let [playout (playout-db/get-playout ds playout-id)]
     (if playout
       (let [result (build! ds (merge opts {:lookahead-hours (* new-horizon-days 24)}) playout)]
         (if (= result :no-schedule) 0 result))
       0))))

(defn ensure-horizon!
  "Ensure one channel's playout timeline reaches at least `horizon-days` from now,
   without disturbing what's already scheduled in the near term.

   This is the per-channel daily top-up: it extends the timeline forward from
   wherever the last build stopped (advancing each show's rotation) up to
   `now + horizon-days`.  Existing upcoming events are preserved, so repeated
   calls are idempotent — once the timeline already reaches the horizon a further
   call generates nothing.  A channel that has run dry is refilled from now.

   Thin wrapper over rebuild-horizon! (the extend path); named for intent at the
   call site.  `opts` is merged into the build options (notably :grout).
   Returns number of events generated."
  ([ds playout-id horizon-days] (ensure-horizon! ds playout-id horizon-days nil))
  ([ds playout-id horizon-days opts]
   (rebuild-horizon! ds playout-id 0 horizon-days opts)))

(defn ensure-all-horizons!
  "Top up EVERY schedule-backed channel's playout timeline to at least
   `horizon-days` from now (see ensure-horizon!).  This is the bulk daily entry
   point: one call keeps all channels stocked with upcoming content, preserving
   near-term events and rotation.  Channels with no attached schedule (populated
   only via the DailySlot ingest stream) are skipped — PV can't rebuild those.

   `opts` (optional) is merged into each channel's build options (notably
   :grout).  Per-channel failures are isolated: one channel erroring does not
   abort the rest; its entry carries an :error string instead of :events-generated.

   Returns a summary:
     {:channels N
      :horizon-days D
      :events-generated TOTAL
      :results [{:channel-id .. :playout-id .. :events-generated N}
                {:channel-id .. :playout-id .. :error \"...\"} ...]}"
  ([ds horizon-days] (ensure-all-horizons! ds horizon-days nil))
  ([ds horizon-days opts]
   (let [playouts (playout-db/list-playouts-with-schedule ds)
         results  (mapv
                   (fn [p]
                     (let [playout-id (:playouts/id p)
                           channel-id (:playouts/channel-id p)]
                       (try
                         {:channel-id       channel-id
                          :playout-id       playout-id
                          :events-generated (ensure-horizon! ds playout-id horizon-days opts)}
                         (catch Exception e
                           (log/error e "ensure-all-horizons: channel failed"
                                      {:playout-id playout-id :channel-id channel-id})
                           {:channel-id channel-id
                            :playout-id playout-id
                            :error      (.getMessage e)}))))
                   playouts)
         total    (reduce + 0 (keep :events-generated results))]
     (log/info "ensure-all-horizons complete"
               {:channels (count results) :events-generated total})
     {:channels         (count results)
      :horizon-days     horizon-days
      :events-generated total
      :results          results})))
