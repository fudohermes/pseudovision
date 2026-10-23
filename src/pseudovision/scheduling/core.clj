(ns pseudovision.scheduling.core
  "Playout build engine.

   Entry points:
     (build! db opts playout)   — full rebuild from scratch
     (rebuild! db opts playout) — incremental: extend the timeline forward

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
            [pseudovision.db.media     :as media-db]
            [pseudovision.db.playouts  :as playout-db]
            [pseudovision.db.schedules :as schedules-db]
            [pseudovision.db.collections :as col-db]
            [pseudovision.scheduling.cursor      :as cursor]
            [pseudovision.scheduling.enumerators :as enum]
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
   tracked independently."
  [slot]
  (str "collection:" (or (:schedule-slots/collection-id slot)
                         (str "item:" (:schedule-slots/media-item-id slot)))))

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
  (when (or (seq required-tags) (seq excluded-tags))
    (let [item-tags (get-item-tags db (:media-items/id item))]
      (and
        ;; Must have all required tags
        (every? #(contains? item-tags %) required-tags)
        ;; Must not have any excluded tags
        (not-any? #(contains? item-tags %) excluded-tags))))
  ;; If no tag filters, item matches
  true)

(defn- load-items
  "Returns the ordered seq of playable media items for a slot.
   Filters by required/excluded tags if specified."
  [db slot]
  (let [required-tags (or (:schedule-slots/required-tags slot) [])
        excluded-tags (or (:schedule-slots/excluded-tags slot) [])
        items (cond
                (:schedule-slots/collection-id slot)
                (let [coll (media-db/get-collection db (:schedule-slots/collection-id slot))]
                  (col-db/resolve-collection db coll))

                (:schedule-slots/media-item-id slot)
                [(media-db/get-media-item db (:schedule-slots/media-item-id slot))]

                :else [])]
    ;; Apply tag filters if specified
    (if (or (seq required-tags) (seq excluded-tags))
      (do
        (log/debug "Filtering items by tags" {:required required-tags :excluded excluded-tags})
        (filter #(matches-tag-filters? db % required-tags excluded-tags) items))
      items)))

(defn- item-duration
  "Returns the item's playback duration, or zero if the item has not been probed."
  [item]
  (or (some-> (:media-versions/duration item))
      (Duration/ofSeconds 0)))

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

(defn- emit-once
  "Emit one item, return [events cursor]."
  [db cursor slot playout-id opts]
  (let [items   (load-items db slot)
        ckey    (collection-key slot)
        order   (keyword (or (:schedule-slots/playback-order slot) "chronological"))
        enum-opts {:seed (get opts :seed 0)
                   :batch-size (or (:schedule-slots/marathon-batch-size slot) 5)}
        e       (cursor/get-enumerator cursor ckey items order enum-opts)
        [item e'] (enum/next-item e)
        dur     (item-duration item)
        from    (:next-start cursor)
        to      (t/add-duration from dur)
        event   {:playout-id    playout-id
                 :media-item-id (:media-items/id item)
                 :kind          (sql-util/->pg-enum "event_kind" "content")
                 :start-at      from
                 :finish-at     to
                 :guide-group   (:next-guide-group cursor)
                 :slot-id       (:schedule-slots/id slot)
                 :is-manual     false}
        cursor' (-> cursor
                    (assoc :next-start to)
                    (cursor/save-enumerator ckey e')
                    (cursor/bump-guide-group))]
    [[event] cursor']))

(defn- emit-count
  "Emit exactly item-count items, return [events cursor]."
  [db cursor slot playout-id opts]
  (let [items  (load-items db slot)
        ckey   (collection-key slot)
        order  (keyword (or (:schedule-slots/playback-order slot) "chronological"))
        enum-opts {:seed (get opts :seed 0)
                   :batch-size (or (:schedule-slots/marathon-batch-size slot) 5)}
        n      (or (:schedule-slots/item-count slot) 1)
        guide  (:next-guide-group cursor)]
    (loop [i      0
           from   (:next-start cursor)
           e      (cursor/get-enumerator cursor ckey items order enum-opts)
           events []]
      (if (>= i n)
        (let [cursor' (-> cursor
                          (assoc :next-start from)
                          (cursor/save-enumerator ckey e)
                          (cursor/bump-guide-group))]
          [events cursor'])
        (let [[item e'] (enum/next-item e)
              dur       (item-duration item)
              to        (t/add-duration from dur)]
          (recur (inc i) to e'
                 (conj events {:playout-id    playout-id
                               :media-item-id (:media-items/id item)
                               :kind          (sql-util/->pg-enum "event_kind" "content")
                               :start-at      from
                               :finish-at     to
                               :guide-group   guide
                               :slot-id       (:schedule-slots/id slot)
                               :is-manual     false})))))))

(defn- emit-block
  "Fill a fixed-duration block.  Pads the tail with filler if configured.
   Returns [events cursor]."
  [db cursor slot channel playout-id opts]
  (let [items      (load-items db slot)
        ckey       (collection-key slot)
        order      (keyword (or (:schedule-slots/playback-order slot) "chronological"))
        enum-opts  {:seed (get opts :seed 0)
                    :batch-size (or (:schedule-slots/marathon-batch-size slot) 5)}
        block-dur  (:schedule-slots/block-duration slot)
        from       (:next-start cursor)
        block-end  (t/add-duration from block-dur)
        guide      (:next-guide-group cursor)]
    (loop [cursor-time from
           e           (cursor/get-enumerator cursor ckey items order enum-opts)
           events      []]
      (let [remaining (t/duration-between cursor-time block-end)]
        (cond
          ;; Block finished
          (or (.isNegative remaining) (.isZero remaining))
          (let [c' (-> cursor
                       (assoc :next-start block-end)
                       (cursor/save-enumerator ckey e)
                       (cursor/bump-guide-group))]
            [events c'])

          ;; No more items; pad the rest
          (empty? (:items e))
          (let [c' (-> cursor
                       (assoc :next-start block-end)
                       (cursor/save-enumerator ckey e)
                       (cursor/bump-guide-group))]
            [events c'])

          :else
          (let [[item e'] (enum/next-item e)
                dur       (item-duration item)
                to        (t/add-duration cursor-time dur)]
            (if (.isAfter to block-end)
              ;; Item overflows the block — respect tail_mode
              (let [tail-events
                    (case (:schedule-slots/tail-mode slot "none")
                      "filler" []   ;; TODO: inject filler content
                      "offline" []  ;; TODO: inject offline segment
                      ;; "none": trim the overflowing item to fill the block
                      ;; exactly, then replay it in full at the next block start.
                      [{:playout-id    playout-id
                        :media-item-id (:media-items/id item)
                        :kind          (sql-util/->pg-enum "event_kind" "content")
                        :start-at      cursor-time
                        :finish-at     block-end
                        :guide-group   guide
                        :slot-id       (:schedule-slots/id slot)
                        :is-manual     false}])]
                (let [c' (-> cursor
                             (assoc :next-start block-end)
                             (cursor/save-enumerator ckey e)
                             (cursor/bump-guide-group))]
                  [(into events tail-events) c']))
              (recur to e'
                     (conj events {:playout-id    playout-id
                                   :media-item-id (:media-items/id item)
                                   :kind          (sql-util/->pg-enum "event_kind" "content")
                                   :start-at      cursor-time
                                   :finish-at     to
                                   :guide-group   guide
                                   :slot-id       (:schedule-slots/id slot)
                                   :is-manual     false})))))))))

(defn- emit-flood
  "Fill from now until the next fixed-anchor slot.
   Returns [events cursor]."
  [db cursor slot channel playout-id {:keys [flood-end] :as opts}]
  ;; Flood fills from :next-start up to flood-end (the next fixed-anchor time).
  (let [items  (load-items db slot)
        ckey   (collection-key slot)
        order  (keyword (or (:schedule-slots/playback-order slot) "chronological"))
        enum-opts {:seed (get opts :seed 0)
                   :batch-size (or (:schedule-slots/marathon-batch-size slot) 5)}
        guide  (:next-guide-group cursor)
        end    (or flood-end
                   (t/add-duration (:next-start cursor) (t/hours->duration 2)))]
    (loop [cursor-time (:next-start cursor)
           e           (cursor/get-enumerator cursor ckey items order enum-opts)
           events      []]
      (let [remaining (t/duration-between cursor-time end)]
        (if (or (.isNegative remaining) (.isZero remaining) (empty? (:items e)))
          (let [c' (-> cursor
                       (assoc :next-start end)
                       (cursor/save-enumerator ckey e)
                       (cursor/bump-guide-group))]
            [events c'])
          (let [[item e'] (enum/next-item e)
                dur       (item-duration item)
                to        (t/add-duration cursor-time dur)]
            (if (.isAfter to end)
              (let [c' (-> cursor
                           (assoc :next-start end)
                           (cursor/save-enumerator ckey e)
                           (cursor/bump-guide-group))]
                [events c'])
              (recur to e'
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
  (let [zone-id  (get opts :zone-id "UTC")
        dow-mask (:schedule-slots/days-of-week slot)
        now      (:next-start cursor)]
    (if (and (= "fixed" (:schedule-slots/anchor slot))
             (not (t/fires-on-day? dow-mask now zone-id)))
      ;; This slot doesn't air today -- advance to its next valid fire time.
      (let [next-fire (next-fixed-start slot now zone-id)]
        (log/debug "Slot skipped (days_of_week)" {:slot-index (:schedule-slots/slot-index slot)
                                                   :next-fire  (str next-fire)})
        [[] (assoc cursor :next-start next-fire)])
      ;; Normal path -- slot fires today (or is sequential).
      (let [fill (keyword (or (:schedule-slots/fill-mode slot) "once"))]
        (case fill
          :once  (emit-once  db cursor slot playout-id opts)
          :count (emit-count db cursor slot playout-id opts)
          :block (emit-block db cursor slot channel playout-id opts)
          :flood (let [flood-end (next-slot-start
                                  slots
                                  (:schedule-slots/slot-index slot)
                                  now
                                  zone-id)]
                   (emit-flood db cursor slot channel playout-id
                               (assoc opts :flood-end flood-end)))
          (do (log/warn "Unknown fill mode, skipping slot" {:fill fill})
              [[] cursor]))))))

;; ---------------------------------------------------------------------------
;; Public API
;; ---------------------------------------------------------------------------

(defn build!
  "Builds the playout from scratch, replacing all non-manual events.
   Runs inside a transaction; saves the updated cursor on success."
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
        horizon     (t/add-duration now (t/hours->duration
                                         (get opts :lookahead-hours 72)))
        opts'       (assoc opts :seed seed)]
    (if (or (nil? schedule) (empty? slots))
      (do (log/warn "No schedule or slots; nothing to build"
                    {:playout-id playout-id})
          :no-schedule)
      (let [saved-cursor (cursor/<-json (:playouts/cursor playout))
            initial-cur  (or saved-cursor (cursor/init now))]
        (jdbc/with-transaction [tx db]
          ;; Remove all auto-generated events from the future horizon
          (playout-db/delete-non-manual-events-after! tx playout-id now)

          (loop [cursor   initial-cur
                 slot-idx 0
                 events   []]
            (let [slot  (nth slots slot-idx nil)
                  start (:next-start cursor)]
              (if (or (nil? slot) (.isAfter start horizon))
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
                ;; Process this slot
                (let [[new-events cursor'] (process-slot
                                            tx cursor slot channel
                                            playout-id slots opts')
                      cursor''  (cursor/advance-slot cursor' (count slots))]
                  (recur cursor'' (mod (inc slot-idx) (count slots))
                         (into events new-events)))))))))))

(defn rebuild-from-now!
  "Delete all future events and regenerate from NOW.
   Used when configuration changes.
   Returns number of events generated."
  [ds playout-id horizon-days]
  (let [playout (playout-db/get-playout ds playout-id)]
    (if playout
      (let [result (build! ds {:lookahead-hours (* horizon-days 24)} playout)]
        (if (= result :no-schedule) 0 result))
      0)))

(defn rebuild-horizon!
  "Generate events for days beyond current horizon (daily rebuild).
   Builds from current-horizon-days out to new-horizon-days.
   Returns number of events generated."
  [ds playout-id current-horizon-days new-horizon-days]
  (let [playout (playout-db/get-playout ds playout-id)]
    (if playout
      (let [result (build! ds {:lookahead-hours (* new-horizon-days 24)} playout)]
        (if (= result :no-schedule) 0 result))
      0)))
