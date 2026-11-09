(ns pseudovision.db.schedules
  (:require [honey.sql         :as sql]
            [honey.sql.helpers :as h]
            [pseudovision.db.core :as db]
            [pseudovision.util.sql :as sql-util]
            [taoensso.timbre :as log]))

;; ---------------------------------------------------------------------------
;; Schedules
;; ---------------------------------------------------------------------------

(defn count-schedules
  "Counts total schedules."
  [ds]
  (let [result (db/query-one ds (-> (h/select [:%count.*])
                                    (h/from :schedules)
                                    sql/format))]
    (or (:count result) 0)))

(defn list-schedules
  "Lists schedules with optional pagination.
   
   opts:
   - :limit  - maximum number of schedules to return
   - :offset - number of schedules to skip"
  ([ds]
   (list-schedules ds nil))
  ([ds opts]
   (db/query ds (-> (h/select :*)
                    (h/from :schedules)
                    (h/order-by :name)
                    (cond->
                      (:limit opts)  (h/limit (:limit opts))
                      (:offset opts) (h/offset (:offset opts)))
                    sql/format))))

(defn get-schedule [ds id]
  (db/query-one ds (-> (h/select :*)
                       (h/from :schedules)
                       (h/where [:= :id id])
                       sql/format)))

(defn create-schedule! [ds attrs]
  (db/execute-one! ds (-> (h/insert-into :schedules)
                          (h/values [attrs])
                          (h/returning :*)
                          sql/format)))

(defn update-schedule! [ds id attrs]
  (db/execute-one! ds (-> (h/update :schedules)
                          (h/set attrs)
                          (h/where [:= :id id])
                          sql/format)))

(defn delete-schedule! [ds id]
  (db/execute-one! ds (-> (h/delete-from :schedules)
                          (h/where [:= :id id])
                          sql/format)))

;; ---------------------------------------------------------------------------
;; Schedule slots
;; ---------------------------------------------------------------------------

(defn count-slots
  "Counts total slots in a schedule."
  [ds schedule-id]
  (let [result (db/query-one ds (-> (h/select [:%count.*])
                                    (h/from :schedule-slots)
                                    (h/where [:= :schedule-id schedule-id])
                                    sql/format))]
    (or (:count result) 0)))

(defn list-slots
  "Lists slots in a schedule with optional pagination.
   
   opts:
   - :limit  - maximum number of slots to return
   - :offset - number of slots to skip"
  ([ds schedule-id]
   (list-slots ds schedule-id nil))
  ([ds schedule-id opts]
   (db/query ds (-> (h/select :*)
                    (h/from :schedule-slots)
                    (h/where [:= :schedule-id schedule-id])
                    (h/order-by :slot-index)
                    (cond->
                      (:limit opts)  (h/limit (:limit opts))
                      (:offset opts) (h/offset (:offset opts)))
                    sql/format))))

(defn get-slot [ds id]
  (db/query-one ds (-> (h/select :*)
                       (h/from :schedule-slots)
                       (h/where [:= :id id])
                       sql/format)))

(defn- coerce-slot-dates
  "active_from / active_until arrive as ISO date strings; bind them as
   LocalDate so Postgres stores DATE values."
  [attrs]
  (reduce (fn [m k]
            (cond-> m
              (string? (get m k)) (update k #(java.time.LocalDate/parse %))))
          attrs
          [:active-from :active-until :active_from :active_until]))

(defn create-slot! [ds attrs]
  (let [;; schedule_slots.anchor defaults to 'sequential' at the DB level when
        ;; omitted, and fill_mode independently defaults to 'once' — so a slot
        ;; created without specifying either ends up 'sequential' + 'once',
        ;; which plays exactly one item and then stops, typically leaving dead
        ;; air until whatever follows. 'flood' plays sequential content until
        ;; the next fixed-anchor slot (or a horizon cap), packing the available
        ;; time with multiple items before padding any remainder with filler —
        ;; a better default for a sequential slot than 'once'. Only applied
        ;; when the caller didn't specify a fill mode explicitly.
        anchor    (or (:anchor attrs) "sequential")
        attrs     (cond-> attrs
                    (and (= anchor "sequential")
                         (nil? (:fill_mode attrs))
                         (nil? (:fill-mode attrs)))
                    (assoc :fill-mode "flood"))
        processed (cond-> attrs
                    (or (:anchor attrs) (:anchor attrs))
                    (update :anchor #(sql-util/->pg-enum "slot_anchor" %))
                    (or (:fill_mode attrs) (:fill-mode attrs))
                    (update :fill-mode #(sql-util/->pg-enum "slot_fill_mode" %))
                    (or (:playback_order attrs) (:playback-order attrs))
                    (update :playback-order #(sql-util/->pg-enum "playback_order" %))
                    (or (:block_duration attrs) (:block-duration attrs))
                    (update :block-duration sql-util/->pg-interval)
                    (or (:start_time attrs) (:start-time attrs))
                    (update :start-time sql-util/->pg-interval)
                    (or (:guide_mode attrs) (:guide-mode attrs))
                    (update :guide-mode #(sql-util/->pg-enum "guide_mode" %)))
                    ;; days-of-week is a plain INTEGER — no coercion needed
        sql-map (-> (h/insert-into :schedule-slots)
                    (h/values [(coerce-slot-dates processed)])
                    (h/returning :*)
                    sql/format)]
    (log/debug "Creating slot" {:attrs attrs :processed processed :sql sql-map})
    (db/execute-one! ds sql-map)))

(defn update-slot! [ds id attrs]
  (db/execute-one! ds (-> (h/update :schedule-slots)
                          (h/set (cond-> (coerce-slot-dates attrs)
                                   (:anchor attrs)
                                   (update :anchor #(sql-util/->pg-enum "slot_anchor" %))
                                   (:fill_mode attrs)
                                   (update :fill_mode #(sql-util/->pg-enum "slot_fill_mode" %))
                                   (:playback_order attrs)
                                   (update :playback_order #(sql-util/->pg-enum "playback_order" %))
                                   (:block_duration attrs)
                                   (update :block_duration sql-util/->pg-interval)
                                   (:start_time attrs)
                                   (update :start_time sql-util/->pg-interval)))
                          (h/where [:= :id id])
                          sql/format)))

(defn delete-slot! [ds id]
  (db/execute-one! ds (-> (h/delete-from :schedule-slots)
                          (h/where [:= :id id])
                          sql/format)))

(defn reorder-slots!
  "Updates the slot_index for a sequence of slot-ids.
   slot-ids is an ordered vector; index is assigned by position."
  [ds schedule-id slot-ids]
  (doseq [[idx slot-id] (map-indexed vector slot-ids)]
    (update-slot! ds slot-id {:slot-index idx})))
