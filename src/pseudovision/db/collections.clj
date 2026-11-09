(ns pseudovision.db.collections
  "Higher-level collection resolution used by the scheduling engine.
   Handles the different collection kinds and returns a seq of media-item maps."
  (:require [honey.sql         :as sql]
            [honey.sql.helpers :as h]
            [pseudovision.db.core :as db]
            [pseudovision.db.media :as media]
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

;; ---------------------------------------------------------------------------
;; Smart-collection helpers
;; ---------------------------------------------------------------------------

(defn- tag-exists-subq
  "HoneySQL EXISTS subquery: does mi.id have a metadata tag matching `name-clause`?"
  [name-clause]
  [:exists {:select [1]
            :from   [[:metadata :m]]
            :join   [[:metadata-tags :mt] [:= :mt.metadata-id :m.id]]
            :where  [:and [:= :m.media-item-id :mi.id] name-clause]}])

(defn- smart-tag-clause
  "Returns a HoneySQL WHERE fragment for required/excluded tag lists.
   match-mode is \"all\" (default) or \"any\" — applies to include-tags only.

   match=all  → item must have every include-tag (one EXISTS per tag, ANDed)
   match=any  → item must have at least one include-tag (one EXISTS with OR)
   exclude    → item must have none of the exclude-tags (NOT EXISTS with OR)"
  [match-mode include-tags exclude-tags]
  (let [include? (seq include-tags)
        exclude? (seq exclude-tags)
        any?     (= match-mode "any")
        clauses  (cond-> []
                   (and include? (not any?))
                   (into (map (fn [t] (tag-exists-subq [:= :mt.name t])) include-tags))
                   (and include? any?)
                   (conj (tag-exists-subq (into [:or] (map (fn [t] [:= :mt.name t]) include-tags))))
                   exclude?
                   (conj [:not (tag-exists-subq (into [:or] (map (fn [t] [:= :mt.name t]) exclude-tags)))]))]
    (when (seq clauses)
      (into [:and] clauses))))

(defmethod resolve-collection :smart [ds collection]
  ;; Supported config keys (all optional; :show-id and :category are checked
  ;; first and, when present, short-circuit the rest — they resolve through
  ;; pseudovision.db.media's season-aware / tag-inheritance-aware queries
  ;; instead of the flat query below,
  ;; which only matches tags on the row's OWN metadata and cannot express
  ;; "all episodes of show X" or "all episodes whose SHOW carries genre Y"):
  ;;   :show-id        — internal id of a show; resolves to its ordered,
  ;;                     season-aware episode list (a named-series strip).
  ;;   :category       — a genre/category tag (bare or `genre:`-prefixed);
  ;;                     resolves matching shows (expanded to episodes) and
  ;;                     movies, exactly like a `random:<category>` pool at
  ;;                     air time.
  ;;   :media-type     — "movie" | "episode" | "music_video" etc.
  ;;   :include-tags   — item must have ALL (or ANY) of these tags
  ;;   :exclude-tags   — item must have NONE of these tags
  ;;   :match          — "all" (default) | "any"  applies to include-tags
  ;;   :order-by       — "id" (default) | "title" | "random" | "year"
  ;;
  ;; Config keys are keyword-typed because the JSONB column reader in db/core
  ;; parses every JSON value with csk/->kebab-case-keyword. (Tests must
  ;; pre-kebab their fixtures too — see test/pseudovision/db/collections_test.clj.)
  (let [q           (get-in collection [:collections/config :query] {})
        show-id     (get q :show-id)
        category    (get q :category)
        channel-tag (get q :channel-tag)]
    (cond
      show-id
      (do (log/info "Resolving smart collection (show-id)"
                    {:id (:collections/id collection) :show-id show-id})
          (media/list-show-episodes-by-id ds show-id))

      category
      (do (log/info "Resolving smart collection (category)"
                    {:id (:collections/id collection) :category category
                     :channel-tag channel-tag})
          (media/resolve-playable-by-tag ds category
                                         :require-tags (when channel-tag [channel-tag])))

      :else
      (let [media-type   (get q :media-type)
            include-tags (get q :include-tags [])
            exclude-tags (get q :exclude-tags [])
            match-mode   (get q :match "all")
            order-kw     (case (get q :order-by "id")
                           "title"  :mi.title
                           "year"   :mi.year
                           "random" [[:random]]
                           :mi.id)
            tag-clause   (smart-tag-clause match-mode include-tags exclude-tags)]
        (log/info "Resolving smart collection"
                  {:id (:collections/id collection)
                   :media-type media-type
                   :include-tags include-tags
                   :exclude-tags exclude-tags
                   :match match-mode})
        (db/query ds (cond-> (-> (h/select :mi.* :mv.duration)
                                 (h/from [:media-items :mi])
                                 (h/left-join [:media-versions :mv] [:= :mv.media-item-id :mi.id])
                                 (h/order-by order-kw))
                       media-type  (h/where [:= :mi.kind (keyword media-type)])
                       tag-clause  (h/where tag-clause)
                       true        sql/format))))))

(defmethod resolve-collection :playlist [ds collection]
  ;; Items are stored in the config JSONB as an ordered list of collection
  ;; references; each referenced collection is resolved recursively and the
  ;; results are concatenated in index order.
  ;;
  ;; Config keys are keyword-typed because the JSONB column reader in db/core
  ;; parses every JSON value with csk/->kebab-case-keyword (which kebab-cases
  ;; underscores — so the JSON key "content_id" arrives as :content-id).
  (let [items (get-in collection [:collections/config :items] [])]
    (mapcat (fn [{content-id :content-id content-kind :content-kind}]
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
  ;;
  ;; Config keys are keyword-typed because the JSONB column reader in db/core
  ;; parses every JSON value with csk/->kebab-case-keyword (which kebab-cases
  ;; underscores — so the JSON key "collection_id" arrives as :collection-id).
  (let [members (get-in collection [:collections/config :members] [])]
    (mapcat (fn [{coll-id :collection-id}]
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
  (log/error "Unknown collection kind" {:kind (:collections/kind collection)})
  [])
