(ns pseudovision.db.filler
  (:require [honey.sql :as sql]
            [honey.sql.helpers :as h]
            [pseudovision.db.core :as db]
            [pseudovision.db.collections :as col-db]
            [pseudovision.util.sql :as sql-util]
            [taoensso.timbre :as log]))

(defn get-filler-preset
  "Fetches a filler_presets row by id. Returns nil if not found or id is nil."
  [ds id]
  (when id
    (db/query-one ds (-> (h/select :*)
                         (h/from :filler-presets)
                         (h/where [:= :id id])
                         sql/format))))

(defn load-filler-items
  "Returns the ordered seq of media items for a filler preset's content source."
  [ds preset]
  (cond
    (:filler-presets/collection-id preset)
    (let [coll (db/query-one ds (-> (h/select :*)
                                    (h/from :collections)
                                    (h/where [:= :id (:filler-presets/collection-id preset)])
                                    sql/format))]
      (if coll (col-db/resolve-collection ds coll) []))

    (:filler-presets/media-item-id preset)
    (let [item (db/query-one ds (-> (h/select :mi.* :mv.duration)
                                    (h/from [:media-items :mi])
                                    (h/left-join [:media-versions :mv]
                                                 [:= :mv.media-item-id :mi.id])
                                    (h/where [:= :mi.id (:filler-presets/media-item-id preset)])
                                    sql/format))]
      (if item [item] []))

    :else []))

;; ---------------------------------------------------------------------------
;; CRUD
;; ---------------------------------------------------------------------------

(defn count-filler-presets [ds]
  (let [result (db/query-one ds (-> (h/select [:%count.*])
                                    (h/from :filler-presets)
                                    sql/format))]
    (or (:count result) 0)))

(defn list-filler-presets
  ([ds] (list-filler-presets ds nil))
  ([ds opts]
   (db/query ds (-> (h/select :*)
                    (h/from :filler-presets)
                    (h/order-by :name)
                    (cond->
                      (:limit opts)  (h/limit (:limit opts))
                      (:offset opts) (h/offset (:offset opts)))
                    sql/format))))

(defn create-filler-preset! [ds attrs]
  (let [prepared (cond-> attrs
                   (:role attrs)       (update :role     #(sql-util/->pg-enum "filler_role"     %))
                   (:mode attrs)       (update :mode     #(sql-util/->pg-enum "filler_mode"     %))
                   (:category attrs)   (update :category #(sql-util/->pg-enum "filler_category" %))
                   (:grout-tags attrs) (update :grout-tags sql-util/->pg-array))
        result   (db/execute-one! ds (-> (h/insert-into :filler-presets)
                                         (h/values [prepared])
                                         (h/returning :*)
                                         sql/format))]
    (log/info "Created filler preset" {:id (:id result) :name (:name attrs)})
    result))

(defn update-filler-preset! [ds id attrs]
  (db/execute-one! ds (-> (h/update :filler-presets)
                           (h/set (cond-> attrs
                                    (:role attrs)       (update :role     #(sql-util/->pg-enum "filler_role"     %))
                                    (:mode attrs)       (update :mode     #(sql-util/->pg-enum "filler_mode"     %))
                                    (:category attrs)   (update :category #(sql-util/->pg-enum "filler_category" %))
                                    (:grout-tags attrs) (update :grout-tags sql-util/->pg-array)))
                           (h/where [:= :id id])
                           sql/format)))

(defn delete-filler-preset! [ds id]
  (db/execute-one! ds (-> (h/delete-from :filler-presets)
                           (h/where [:= :id id])
                           sql/format)))
