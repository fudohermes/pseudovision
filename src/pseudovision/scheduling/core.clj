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
        slot-id       (:schedule-slots/id slot)
        collection-id (:schedule-slots/collection-id slot)
        media-id      (:schedule-slots/media-item-id slot)
        items (cond
                collection-id
                (let [coll (media-db/get-collection db collection-id)]
                  (log/info "Loading collection for slot"
                           {:slot-id slot-id
                            :collection-id collection-id
                            :collection-name (:collections/name coll)
                            :collection-kind (:collections/kind coll)})
                  (col-db/resolve-collection db coll))

                media-id
                (let [item (media-db/get-media-item db media-id)]
                  (log/info "Loading single media item for slot"
                           {:slot-id slot-id
                            :media-id media-id
                            :media-name (when item (:media-items/name item))})
                  [item])

                :else (do
                        (log/warn "Slot has no collection or media item"
                                 {:slot-id slot-id})
                        []))]
    (log/info "Loaded items for slot (before tag filtering)"
             {:slot-id slot-id
              :count (count items)
              :has-tags (or (seq required-tags) (seq excluded-tags))})
    ;; Apply tag filters if specified
    (if (or (seq required-tags) (seq excluded-tags))
      (do
        (log/info "Filtering items by tags"
                 {:slot-id slot-id
                  :required required-tags
                  :excluded excluded-tags
                  :before-count (count items)})
        (let [filtered (filter #(matches-tag-filters? db % required-tags excluded-tags) items)]
          (log/info "Items after tag filtering"
                   {:slot-id slot-id
                    :after-count (count filtered)
                    :filtered-out (- (count items) (count filtered))})
          filtered))
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
    (log/info "Emitted event (fill-mode: once)"
             {:slot-id (:schedule-slots/id slot)
              :playback-order order
              :media-id (:media-items/id item)
              :media-name (:media-items/name item)
              :duration-secs (-> dur .getSeconds)
              :start-at from
              :finish-at to
              :next-cursor-time (:next-start cursor')})
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
    (log/info "Beginning emit-count"
             {:slot-id (:schedule-slots/id slot)
              :playback-order order
              :item-count n
              :cursor-time (:next-start cursor)})
    (loop [i      0
           from   (:next-start cursor)
           e      (cursor/get-enumerator cursor ckey items order enum-opts)
           events []
           total-duration (java.time.Duration/ofSeconds 0)]
      (if (>= i n)
        (let [cursor' (-> cursor
                          (assoc :next-start from)
                          (cursor/save-enumerator ckey e)
                          (cursor/bump-guide-group))]
          (log/info "Completed emit-count"
                   {:slot-id (:schedule-slots/id slot)
                    :items-emitted (count events)
                    :total-duration-secs (-> total-duration .getSeconds)
                    :next-cursor-time from})
          [events cursor'])
        (let [[item e'] (enum/next-item e)
              dur       (item-duration item)
              to        (t/add-duration from dur)]
          (log/debug "Emitting count item"
                    {:slot-id (:schedule-slots/id slot)
                     :item-number (inc i)
                     :of-count n
                     :media-id (:media-items/id item)
                     :media-name (:media-items/name item)
                     :duration-secs (-> dur .getSeconds)
                     :start-at from
                     :finish-at to})
          (recur (inc i) to e'
                 (conj events {:playout-id    playout-id
                               :media-item-id (:media-items/id item)
                               :kind          (sql-util/->pg-enum "event_kind" "content")
                               :start-at      from
                               :finish-at     to
                               :guide-group   guide
                               :slot-id       (:schedule-slots/id slot)
                               :is-manual     false})
                 (t/add-duration total-duration dur)))))))

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
    (log/info "Beginning emit-block"
             {:slot-id (:schedule-slots/id slot)
              :playback-order order
              :block-duration-secs (-> block-dur .getSeconds)
              :block-start from
              :block-end block-end})
    (loop [cursor-time from
           e           (cursor/get-enumerator cursor ckey items order enum-opts)
           events      []
           item-count  0]
      (let [remaining (t/duration-between cursor-time block-end)]
        (cond
          ;; Block finished
          (or (.isNegative remaining) (.isZero remaining))
          (let [c' (-> cursor
                       (assoc :next-start block-end)
                       (cursor/save-enumerator ckey e)
                       (cursor/bump-guide-group))]
            (log/info "Completed emit-block (time elapsed)"
                     {:slot-id (:schedule-slots/id slot)
                      :items-emitted item-count
                      :total-duration-secs (-> block-dur .getSeconds)
                      :next-cursor-time block-end})
            [events c'])

          ;; No more items; pad the rest
          (empty? (:items e))
          (let [c' (-> cursor
                       (assoc :next-start block-end)
                       (cursor/save-enumerator ckey e)
                       (cursor/bump-guide-group))]
            (log/info "Completed emit-block (ran out of items)"
                     {:slot-id (:schedule-slots/id slot)
                      :items-emitted item-count
                      :remaining-time-secs (-> remaining .getSeconds)
                      :next-cursor-time block-end})
            [events c'])

          :else
          (let [[item e'] (enum/next-item e)
                dur       (item-duration item)
                to        (t/add-duration cursor-time dur)]
            (if (.isAfter to block-end)
              ;; Item overflows the block — respect tail_mode
              (let [tail-events
                    (case (:schedule-slots/tail-mode slot "none")
                      "filler" [] ;; TODO: inject filler content
                      "offline" [] ;; TODO: inject offline segment
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
                (log/info "Block overflow - applying tail mode"
                         {:slot-id (:schedule-slots/id slot)
                          :media-id (:media-items/id item)
                          :media-name (:media-items/name item)
                          :item-duration-secs (-> dur .getSeconds)
                          :remaining-block-secs (-> (t/duration-between cursor-time block-end) .getSeconds)
                          :tail-mode (or (:schedule-slots/tail-mode slot) "none")
                          :trimmed-to block-end})
                (let [c' (-> cursor
                             (assoc :next-start block-end)
                             (cursor/save-enumerator ckey e)
                             (cursor/bump-guide-group))]
                  [(into events tail-events) c']))
              (do
                (log/debug "Emitting block item"
                          {:slot-id (:schedule-slots/id slot)
                           :item-number (inc item-count)
                           :media-id (:media-items/id item)
                           :media-name (:media-items/name item)
                           :duration-secs (-> dur .getSeconds)
                           :start-at cursor-time
                           :finish-at to
                           :remaining-after-secs (-> (t/duration-between to block-end) .getSeconds)})
                (recur to e'
                       (conj events {:playout-id    playout-id
                                     :media-item-id (:media-items/id item)
                                     :kind          (sql-util/->pg-enum "event_kind" "content")
                                     :start-at      cursor-time
                                     :finish-at     to
                                     :guide-group   guide
                                     :slot-id       (:schedule-slots/id slot)
                                     :is-manual     false})
                       (inc item-count))))))))

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
    (log/info "Beginning emit-flood"
             {:slot-id (:schedule-slots/id slot)
              :playback-order order
              :flood-start (:next-start cursor)
              :flood-end end
              :flood-duration-secs (-> (t/duration-between (:next-start cursor) end) .getSeconds)
              :has-explicit-end (some? flood-end)})
    (loop [cursor-time (:next-start cursor)
           e           (cursor/get-enumerator cursor ckey items order enum-opts)
           events      []
           item-count  0]
      (let [remaining (t/duration-between cursor-time end)]
        (if (or (.isNegative remaining) (.isZero remaining) (empty? (:items e)))
          (let [c' (-> cursor
                       (assoc :next-start end)
                       (cursor/save-enumerator ckey e)
                       (cursor/bump-guide-group))]
            (log/info "Completed emit-flood"
                     {:slot-id (:schedule-slots/id slot)
                      :items-emitted item-count
                      :total-duration-secs (-> (t/duration-between (:next-start cursor) end) .getSeconds)
                      :reason (cond
                                (.isNegative remaining) "time-exhausted"
                                (.isZero remaining) "exact-fit"
                                :else "no-more-items")
                      :next-cursor-time end})
            [events c'])
          (let [[item e'] (enum/next-item e)
                dur       (item-duration item)
                to        (t/add-duration cursor-time dur)]
            (if (.isAfter to end)
              (let [c' (-> cursor
                           (assoc :next-start end)
                           (cursor/save-enumerator ckey e)
                           (cursor/bump-guide-group))]
                (log/info "Flood item would overflow - stopping flood"
                         {:slot-id (:schedule-slots/id slot)
                          :media-id (:media-items/id item)
                          :media-name (:media-items/name item)
                          :item-duration-secs (-> dur .getSeconds)
                          :remaining-flood-secs (-> remaining .getSeconds)
                          :items-emitted item-count
                          :next-cursor-time end})
                [events c'])
              (do
                (log/debug "Emitting flood item"
                          {:slot-id (:schedule-slots/id slot)
                           :item-number (inc item-count)
                           :media-id (:media-items/id item)
                           :media-name (:media-items/name item)
                           :duration-secs (-> dur .getSeconds)
                           :start-at cursor-time
                           :finish-at to
                           :remaining-after-secs (-> (t/duration-between to end) .getSeconds)})
                (recur to e'
                       (conj events {:playout-id    playout-id
                                     :media-item-id (:media-items/id item)
                                     :kind          (sql-util/->pg-enum "event_kind" "content")
                                     :start-at      cursor-time
                                     :finish-at     to
                                     :guide-group   guide
                                     :slot-id       (:schedule-slots/id slot)
                                     :is-manual     false})
                       (inc item-count))))))))

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
        now      (:next-start cursor)
        slot-idx (:schedule-slots/slot-index slot)
        fill-mode (keyword (or (:schedule-slots/fill-mode slot) "once"))]
    (if (and (= "fixed" (:schedule-slots/anchor slot))
             (not (t/fires-on-day? dow-mask now zone-id)))
      ;; This slot doesn't air today -- advance to its next valid fire time.
      (let [next-fire (next-fixed-start slot now zone-id)]
        (log/info "Slot skipped (days_of_week constraint)"
                 {:slot-index slot-idx
                  :slot-id (:schedule-slots/id slot)
                  :current-time now
                  :days-of-week-mask dow-mask
                  :next-fire next-fire})
        [[] (assoc cursor :next-start next-fire)])
      ;; Normal path -- slot fires today (or is sequential).
      (do
        (log/info "Processing slot"
                 {:slot-index slot-idx
                  :slot-id (:schedule-slots/id slot)
                  :fill-mode fill-mode
                  :anchor (or (:schedule-slots/anchor slot) "sequential")
                  :start-time (:schedule-slots/start-time slot)
                  :cursor-time now})
        (case fill-mode
          :once  (emit-once  db cursor slot playout-id opts)
          :count (emit-count db cursor slot playout-id opts)
          :block (emit-block db cursor slot channel playout-id opts)
          :flood (let [flood-end (next-slot-start
                                  slots
                                  slot-idx
                                  now
                                  zone-id)]
                   (emit-flood db cursor slot channel playout-id
                               (assoc opts :flood-end flood-end)))
          (do (log/warn "Unknown fill mode, skipping slot"
                       {:slot-index slot-idx
                        :slot-id (:schedule-slots/id slot)
                        :fill-mode fill-mode})
              [[] cursor])))))))

;; ---------------------------------------------------------------------------
;; Public API
;; ---------------------------------------------------------------------------

(defn build!
  "Builds the playout from scratch, replacing all non-manual events.
   Runs inside a transaction; saves the updated cursor on success.
   If an error occurs, stores the error message in build-message field."
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
    (log/info "Build starting"
             {:playout-id playout-id
              :channel-id channel-id
              :schedule-id schedule-id
              :current-time now
              :horizon horizon
              :lookahead-hours (get opts :lookahead-hours 72)
              :seed seed
              :schedule-name (when schedule (:schedules/name schedule))
              :num-slots (count slots)})
    (if (or (nil? schedule) (empty? slots))
      (do (log/warn "No schedule or slots; nothing to build"
                    {:playout-id playout-id
                     :has-schedule (some? schedule)
                     :num-slots (count slots)})
          (try
            (jdbc/with-transaction [tx db]
              (playout-db/update-playout! tx playout-id
                                          {:build-success false
                                           :build-message "No schedule or slots configured"}))
            (catch Exception e
              (log/error "Failed to record no-schedule error" {:error (.getMessage e)})))
          :no-schedule)
      (try
        (let [saved-cursor (cursor/<-json (:playouts/cursor playout))
              initial-cur  (or saved-cursor (cursor/init now))]
          (log/info "Build resuming from cursor"
                   {:playout-id playout-id
                    :has-saved-cursor (some? saved-cursor)
                    :cursor-time (:next-start initial-cur)})
          (jdbc/with-transaction [tx db]
            ;; Remove all auto-generated events from the future horizon
            (playout-db/delete-non-manual-events-after! tx playout-id now)
            (log/debug "Cleared non-manual events"
                      {:playout-id playout-id
                       :from now})

            (loop [cursor   initial-cur
                   slot-idx 0
                   events   []
                   error    nil]
              (if error
                ;; Error encountered; save it and bail
                (do
                  (log/error "Build failed with error" {:error error})
                  (playout-db/update-playout! tx playout-id
                                              {:cursor         (cursor/->json cursor)
                                               :last-built-at  now
                                               :build-success  false
                                               :build-message  error})
                  (log/warn "Build failure recorded in build-message" {:playout-id playout-id})
                  0)
                (let [slot  (nth slots slot-idx nil)
                      start (:next-start cursor)]
                  (if (or (nil? slot) (.isAfter start horizon))
                    ;; Done; flush events and save cursor
                    (do
                      (log/info "Build horizon reached - flushing events"
                               {:playout-id playout-id
                                :horizon-end horizon
                                :cursor-time start
                                :reason (if (nil? slot) "exhausted-slots" "past-horizon")})
                      (playout-db/bulk-insert-events! tx events)
                      (playout-db/update-playout! tx playout-id
                                                  {:cursor         (cursor/->json cursor)
                                                   :last-built-at  now
                                                   :build-success  true
                                                   :build-message  nil})
                      (log/info "Build complete" {:playout-id playout-id
                                                 :events     (count events)
                                                 :total-duration-secs (-> (t/duration-between (:next-start initial-cur) start) .getSeconds)})
                      (count events))
                    ;; Process this slot, catching errors
                    (try
                      (let [[new-events cursor'] (process-slot
                                                  tx cursor slot channel
                                                  playout-id slots opts')
                            cursor''  (cursor/advance-slot cursor' (count slots))]
                        (log/debug "Slot completed successfully"
                                  {:slot-idx (mod (inc slot-idx) (count slots))
                                   :slot-id (:schedule-slots/id slot)
                                   :events-generated (count new-events)
                                   :next-cursor-time (:next-start cursor'')})
                        (recur cursor'' (mod (inc slot-idx) (count slots))
                               (into events new-events)
                               nil))
                      (catch Exception e
                        (let [slot-num (mod (inc slot-idx) (count slots))
                              error-msg (format "Build failed at slot %d: %s - %s"
                                              slot-num
                                              (.getSimpleName (class e))
                                              (.getMessage e))]
                          (log/error "Exception during slot processing" 
                                   {:slot-index slot-num
                                    :slot-id (:schedule-slots/id slot)
                                    :error error-msg
                                    :exception e})
                          (recur cursor slot-idx events error-msg)))))))))))
        (catch Exception e
          (let [error-msg (format "Build transaction failed: %s - %s"
                                 (.getSimpleName (class e))
                                 (.getMessage e))]
            (log/error "Critical build error" 
                     {:playout-id playout-id
                      :error error-msg
                      :exception e})
            (try
              (playout-db/update-playout! db playout-id
                                          {:build-success false
                                           :build-message error-msg})
              (catch Exception inner-e
                (log/error "Failed to save error state" {:error (.getMessage inner-e)})))
            0)))))))

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
