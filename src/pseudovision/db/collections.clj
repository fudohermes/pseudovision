(ns pseudovision.db.collections
  "Higher-level collection resolution used by the scheduling engine.
   Handles the different collection kinds and returns a seq of media-item maps."
  (:require [honey.sql         :as sql]
            [honey.sql.helpers :as h]
            [pseudovision.db.core :as db]
            [taoensso.timbre   :as log]))

(defmulti resolve-collection
  "Returns an ordered seq of media-item maps for a collection.
   Dispatch on :kind (keyword, e.g. :manual, :smart, :playlist …)."
  (fn [_ds collection] (keyword (:collections/kind collection))))

(defmethod resolve-collection :manual [ds collection]
  (db/query ds (-> (h/select :mi.* :mv.duration)
                   (h/from [:media-items :mi])
                   (h/join [:collection-items :ci] [:= :ci.media-item-id :mi.id])
                   (h/left-join [:media-versions :mv] [:= :mv.media-item-id :mi.id])
                   (h/where [:= :ci.collection-id (:collections/id collection)])
                   (h/order-by [[:coalesce :ci.custom-order :mi.id]])
                   sql/format)))

(defmethod resolve-collection :smart [ds collection]
  ;; Basic smart collection query support
  ;; Supports filtering by media-type (movie, episode, music_video)
  (let [query-config (get-in collection [:collections/config "query"] {})
        media-type (get query-config "media-type")]
    (log/info "Resolving smart collection" 
              {:id (:collections/id collection) 
               :query query-config
               :media-type media-type})
    (db/query ds (-> (h/select :mi.* :mv.duration)
                     (h/from [:media-items :mi])
                     (h/left-join [:media-versions :mv] [:= :mv.media-item-id :mi.id])
                     (cond->
                       ;; Filter by media type if specified
                       media-type
                       (h/where [:= :mi.kind (keyword media-type)]))
                     (h/order-by :mi.id)
                     sql/format))))

(defmethod resolve-collection :playlist [ds collection]
  ;; Items are stored in the config JSONB as an ordered list of collection
  ;; references; each referenced collection is resolved recursively and the
  ;; results are concatenated in index order.
  (let [items (get-in collection [:collections/config "items"] [])]
    (mapcat (fn [{content-id "content_id" content-kind "content_kind"}]
              (let [child (db/query-one ds
                            (-> (h/select :*)
                                (h/from :collections)
                                (h/where [:= :id content-id])
                                sql/format))]
                (if child
                  (resolve-collection ds child)
                  (do (log/warn "Playlist item references missing collection"
                                {:id content-id})
                      []))))
            items)))

(defmethod resolve-collection :multi [ds collection]
  ;; A multi-collection merges several peer collections; each member is
  ;; resolved recursively and the results are concatenated.
  (let [members (get-in collection [:collections/config "members"] [])]
    (mapcat (fn [{coll-id "collection_id"}]
              (let [child (db/query-one ds
                            (-> (h/select :*)
                                (h/from :collections)
                                (h/where [:= :id coll-id])
                                sql/format))]
                (if child (resolve-collection ds child) [])))
            members)))

(defmethod resolve-collection :trakt [ds collection]
  ;; Trakt items are resolved via the trakt_list_items junction table.
  (db/query ds (-> (h/select :mi.*)
                   (h/from [:media-items :mi])
                   (h/join [:trakt-list-items :tl] [:= :tl.media-item-id :mi.id])
                   (h/where [:= :tl.collection-id (:collections/id collection)])
                   (h/order-by :mi.id)
                   sql/format)))

(defmethod resolve-collection :rerun [ds collection]
  ;; TODO: implement first-run / rerun filtering
  (log/warn "Rerun collection resolution not yet implemented; returning empty"
            {:id (:collections/id collection)})
  [])

(defmethod resolve-collection :default [_ds collection]
  (log/error "Unknown collection kind; returning empty result" 
            {:kind (:collections/kind collection)
             :collection-id (:collections/id collection)
             :collection-name (:collections/name collection)})
  [])
