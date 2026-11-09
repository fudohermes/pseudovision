(ns pseudovision.db.playouts
  (:require [honey.sql         :as sql]
            [honey.sql.helpers :as h]
            [next.jdbc         :as jdbc]
            [pseudovision.db.core :as db]
            [pseudovision.util.sql :as sql-util]))

;; ---------------------------------------------------------------------------
;; Playouts
;; ---------------------------------------------------------------------------

(defn get-playout [ds id]
  (db/query-one ds (-> (h/select :*)
                       (h/from :playouts)
                       (h/where [:= :id id])
                       sql/format)))

(defn get-playout-for-channel [ds channel-id]
  (db/query-one ds (-> (h/select :*)
                       (h/from :playouts)
                       (h/where [:= :channel-id channel-id])
                       sql/format)))

(defn list-playouts-with-schedule
  "All playouts that have a schedule attached (schedule_id IS NOT NULL) — the
   channels the build engine can (re)generate. Playouts with no attached schedule
   are skipped: PV has nothing to build them from. Ordered by channel_id
   for a stable, repeatable iteration."
  [ds]
  (db/query ds (-> (h/select :*)
                   (h/from :playouts)
                   (h/where [:is-not :schedule-id nil])
                   (h/order-by :channel-id)
                   sql/format)))

(defn upsert-playout!
  "Creates the playout row for channel-id if it doesn't exist, or returns the
   existing one. Uses INSERT … ON CONFLICT DO UPDATE to ensure RETURNING works."
  [ds channel-id schedule-id]
  (jdbc/execute-one! ds
                     (-> (h/insert-into :playouts)
                         (h/values [{:channel-id  channel-id
                                     :schedule-id schedule-id
                                     :seed        (rand-int Integer/MAX_VALUE)}])
                         (h/on-conflict :channel-id)
                         (h/do-update-set :schedule-id :seed)  ; Update schedule and seed on conflict
                         (h/returning :*)
                         sql/format)
                     {:return-keys true}))

(defn attach-schedule!
  "Attaches (or re-attaches) a schedule to a channel's playout, creating the
   playout row on first attach. On an existing playout, updates only
   :schedule-id — unlike upsert-playout!, it does NOT reset :seed, so
   swapping in a re-synced schedule (e.g. after a grid re-freeze) doesn't
   reshuffle filler variety or otherwise perturb an already-running channel.
   Does not itself trigger a rebuild; the caller decides when to do that."
  [ds channel-id schedule-id]
  (db/execute-one! ds
                   (-> (h/insert-into :playouts)
                       (h/values [{:channel-id  channel-id
                                   :schedule-id schedule-id
                                   :seed        (rand-int Integer/MAX_VALUE)}])
                       (h/on-conflict :channel-id)
                       (h/do-update-set :schedule-id)
                       (h/returning :*)
                       sql/format)))

(defn update-playout! [ds id attrs]
  (db/execute-one! ds (-> (h/update :playouts)
                          (h/set attrs)
                          (h/where [:= :id id])
                          sql/format)))

(defn save-cursor! [ds playout-id cursor]
  (update-playout! ds playout-id {:cursor (sql-util/->jsonb cursor)}))

;; ---------------------------------------------------------------------------
;; Playout events
;; ---------------------------------------------------------------------------

(defn list-events
  "Returns all events for a playout in chronological order."
  [ds playout-id]
  (db/query ds (-> (h/select :*)
                   (h/from :playout-events)
                   (h/where [:= :playout-id playout-id])
                   (h/order-by :start-at)
                   sql/format)))

(defn list-events-in-window
  "Returns events overlapping [from, to) across all playouts.
   Only includes events from channels where show_in_epg = true."
  [ds from to]
  (db/query ds (-> (h/select :pe.* :c.id :c.uuid :c.name :c.number :c.show-in-epg :m.title :m.plot 
                             :m.episode-number :m.content-rating :m.release-date)
                   (h/from [:playout-events :pe])
                   (h/join [:playouts :p] [:= :pe.playout-id :p.id])
                   (h/join [:channels  :c] [:= :p.channel-id :c.id])
                   (h/left-join [:metadata :m] [:= :m.media-item-id :pe.media-item-id])
                   (h/where [:and
                             [:< :pe.start-at  to]
                             [:> :pe.finish-at from]
                             [:= :c.show-in-epg true]])
                   (h/order-by :c.sort-number :pe.start-at)
                   sql/format)))

(defn list-event-spans-after
  "Returns [start-at finish-at] of every event (manual or not) on a playout that
   finishes after `after`, in chronological order."
  [ds playout-id after]
  (mapv (juxt :playout-events/start-at :playout-events/finish-at)
        (db/query ds (-> (h/select :start-at :finish-at)
                         (h/from :playout-events)
                         (h/where [:and
                                   [:= :playout-id playout-id]
                                   [:> :finish-at after]])
                         (h/order-by :start-at)
                         sql/format))))

(defn get-current-event
  "Returns the event currently playing on a playout, if any."
  [ds playout-id now]
  (db/query-one ds (-> (h/select :*)
                       (h/from :playout-events)
                       (h/where [:and
                                 [:= :playout-id playout-id]
                                 [:<= :start-at  now]
                                 [:>  :finish-at now]])
                       sql/format)))

(defn get-upcoming-events
  "Returns the next N events for a playout starting from `after`.
   Includes any event currently in progress (finish-at > after), not just
   events whose start-at is in the future.
   
   If cursor is provided, starts from events after that start-at timestamp."
  [ds playout-id after limit & {:keys [cursor]}]
  (db/query ds (-> (h/select :*)
                   (h/from :playout-events)
                   (h/where (if cursor
                              [:and
                               [:= :playout-id playout-id]
                               [:> :start-at cursor]]
                              [:and
                               [:= :playout-id playout-id]
                               [:> :finish-at  after]]))
                   (h/order-by :start-at)
                   (h/limit limit)
                   sql/format)))

(defn get-upcoming-events-with-metadata
  "Returns the next N events for a playout with metadata (title, plot, etc).
   Useful for generating EPG or fallback slates.

   If cursor is provided, starts from events after that start-at timestamp."
  [ds playout-id after limit & {:keys [cursor]}]
  (db/query ds (-> (h/select :pe.* :m.title :m.plot :m.release-date)
                   (h/from [:playout-events :pe])
                   (h/left-join [:metadata :m] [:= :m.media-item-id :pe.media-item-id])
                   (h/where (if cursor
                              [:and
                               [:= :pe.playout-id playout-id]
                               [:> :pe.start-at cursor]]
                              [:and
                               [:= :pe.playout-id playout-id]
                               [:> :pe.finish-at  after]]))
                   (h/order-by :pe.start-at)
                   (h/limit limit)
                   sql/format)))

(defn create-event! [ds attrs]
  (db/execute-one! ds (-> (h/insert-into :playout-events)
                          (h/values [attrs])
                          (h/returning :*)
                          sql/format)))

(defn uniform-rows
  "Normalises a collection of row maps to a single, stable column set: the
   union of all keys (in first-seen order), with any key absent from a row
   filled with nil.

   bulk-insert-events! mixes content events (which carry :guide-group and
   :slot-id) with filler events that may not. HoneySQL renders a heterogeneous
   `values` collection by emitting NULL for missing keys, which silently
   breaks NOT NULL columns. Making the shape explicit here keeps column
   ordering deterministic and the insert independent of HoneySQL's behaviour."
  [rows]
  (let [cols (into [] (comp (mapcat keys) (distinct)) rows)]
    (mapv (fn [r] (reduce (fn [m k] (assoc m k (get r k))) (array-map) cols))
          rows)))

(defn bulk-insert-events!
  "Inserts a batch of event maps in one statement."
  [ds events]
  (when (seq events)
    (jdbc/execute! ds
                   (-> (h/insert-into :playout-events)
                       (h/values (uniform-rows events))
                       sql/format))))

(defn update-event! [ds id attrs]
  (db/execute-one! ds (-> (h/update :playout-events)
                          (h/set attrs)
                          (h/where [:= :id id])
                          sql/format)))

(defn delete-event! [ds id]
  (db/execute-one! ds (-> (h/delete-from :playout-events)
                          (h/where [:= :id id])
                          sql/format)))

(defn delete-non-manual-events-after!
  "Deletes all auto-generated events for a playout starting from `from`.
   Preserves is_manual = TRUE events."
  [ds playout-id from]
  (jdbc/execute! ds
                 (-> (h/delete-from :playout-events)
                     (h/where [:and
                               [:= :playout-id playout-id]
                               [:>= :start-at  from]
                               [:= :is-manual  false]])
                     sql/format)))

(defn delete-events!
  "Bulk-deletes events for a playout and returns the number of rows removed.

   Options:
     :keep-manual? (default true) — preserve is_manual = TRUE events.
     :from / :to                   — optional Instant bounds. When supplied,
       only events overlapping the window are removed: an event overlaps when
       it starts before `to` AND finishes after `from`, so a long item that
       straddles either edge of the window is included. `from` and `to` may be
       given independently (open-ended on the missing side); when both are nil
       every event (subject to :keep-manual?) is removed."
  [ds playout-id {:keys [keep-manual? from to] :or {keep-manual? true}}]
  (let [conds (cond-> [[:= :playout-id playout-id]]
                keep-manual? (conj [:= :is-manual false])
                to           (conj [:< :start-at  to])
                from         (conj [:> :finish-at from]))
        res   (jdbc/execute-one! ds
                                 (-> (h/delete-from :playout-events)
                                     (h/where (into [:and] conds))
                                     sql/format))]
    (or (:next.jdbc/update-count res) 0)))

(defn reset-playout!
  "Clears a playout's generated timeline and resets its saved cursor and build
   status, so the next rebuild starts fresh from now (sidestepping a stale
   cursor that would otherwise resume mid-timeline). Removes non-manual events
   by default; pass keep-manual? = false to wipe manual events too. Runs in a
   transaction and returns the number of events deleted."
  [ds playout-id keep-manual?]
  (jdbc/with-transaction [tx ds]
    (let [deleted (delete-events! tx playout-id {:keep-manual? keep-manual?})]
      (update-playout! tx playout-id {:cursor         nil
                                      :last-built-at  nil
                                      :build-success  nil
                                      :build-message  nil})
      deleted)))

(def filler-event-kinds
  "event_kind values that represent filler rather than primary content."
  ["pre" "mid" "post" "pad" "tail" "fallback" "bumper"])

(defn recent-filler-airings
  "Returns, across ALL playouts (every channel), the finish times of filler
   events that finish within [since, until].  Used to compute global filler
   recency so the same bumper is not aired too close together on any channel.

   Result: media_item_id -> vector of finish Instants."
  [ds since until]
  (let [rows (db/query ds (-> (h/select :media-item-id :finish-at)
                              (h/from :playout-events)
                              (h/where [:and
                                        [:in :kind (mapv #(sql-util/->pg-enum "event_kind" %)
                                                         filler-event-kinds)]
                                        [:>= :finish-at since]
                                        [:<= :finish-at until]])
                              sql/format))]
    (reduce (fn [acc r]
              (update acc (:playout-events/media-item-id r)
                      (fnil conj []) (:playout-events/finish-at r)))
            {} rows)))

;; ---------------------------------------------------------------------------
;; Playout history
;; ---------------------------------------------------------------------------

(defn get-collection-history
  "Returns the history record for a specific collection key within a playout."
  [ds playout-id collection-key]
  (db/query-one ds (-> (h/select :*)
                       (h/from :playout-history)
                       (h/where [:and
                                 [:= :playout-id     playout-id]
                                 [:= :collection-key collection-key]])
                       (h/order-by [:aired-at :desc])
                       (h/limit 1)
                       sql/format)))

(defn upsert-history! [ds attrs]
  (jdbc/execute-one! ds
                     (-> (h/insert-into :playout-history)
                         (h/values [attrs])
                         sql/format)
                     {:return-keys true}))

(defn prune-history!
  "Deletes history rows whose events have already finished."
  [ds before]
  (jdbc/execute! ds
                 (-> (h/delete-from :playout-history)
                     (h/where [:<  :event-finish-at before])
                     sql/format)))

;; ---------------------------------------------------------------------------
;; Playout gaps
;; ---------------------------------------------------------------------------

(defn replace-gaps!
  "Replaces all gap rows for a playout with a fresh set."
  [ds playout-id gaps]
  (jdbc/with-transaction [tx ds]
    (jdbc/execute! tx
                   (-> (h/delete-from :playout-gaps)
                       (h/where [:= :playout-id playout-id])
                       sql/format))
    (when (seq gaps)
      (jdbc/execute! tx
                     (-> (h/insert-into :playout-gaps)
                         (h/values (map #(assoc % :playout-id playout-id) gaps))
                         sql/format)))))
