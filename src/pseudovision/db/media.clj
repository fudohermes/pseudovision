(ns pseudovision.db.media
  (:require [clojure.string    :as str]
            [honey.sql         :as sql]
            [honey.sql.helpers :as h]
            [pseudovision.db.core :as db]
            [pseudovision.util.sql :as sql-util]
            [pseudovision.util.tags :as tags]
            [taoensso.timbre :as log]))

;; ---------------------------------------------------------------------------
;; Media sources and libraries
;; ---------------------------------------------------------------------------

(defn count-media-sources
  "Counts total media sources."
  [ds]
  (let [result (db/query-one ds (-> (h/select [:%count.*])
                                    (h/from :media-sources)
                                    sql/format))]
    (or (:count result) 0)))

(defn list-media-sources
  "Lists media sources with optional pagination.
   
   opts:
   - :limit  - maximum number of sources to return
   - :offset - number of sources to skip"
  ([ds]
   (list-media-sources ds nil))
  ([ds opts]
   (db/query ds (-> (h/select :*)
                    (h/from :media-sources)
                    (cond->
                     (:limit opts)  (h/limit (:limit opts))
                     (:offset opts) (h/offset (:offset opts)))
                    sql/format))))

(defn get-media-source [ds id]
  (db/query-one ds (-> (h/select :*)
                       (h/from :media-sources)
                       (h/where [:= :id id])
                       sql/format)))

(defn create-media-source! [ds attrs]
  (let [prepared (cond-> attrs
                   (:kind attrs)                (update :kind #(sql-util/->pg-enum "media_source_kind" %))
                   (:connection-config attrs)   (update :connection-config sql-util/->jsonb)
                   (:path-replacements attrs)   (update :path-replacements sql-util/->jsonb))
        sql-map  (-> (h/insert-into :media-sources)
                     (h/values [prepared])
                     sql/format)]
    (log/info "Creating media source" {:attrs attrs :prepared prepared :sql sql-map})
    (let [result (db/execute-one! ds sql-map)]
      (log/info "Created media source"
                {:source-id (:media-sources/id result)
                 :name      (:name attrs)
                 :kind      (:kind attrs)})
      result)))

(defn update-media-source! [ds id attrs]
  (let [prepared (cond-> attrs
                   (:kind attrs)               (update :kind #(sql-util/->pg-enum "media_source_kind" %))
                   (:connection-config attrs)  (update :connection-config sql-util/->jsonb)
                   (:path-replacements attrs)  (update :path-replacements sql-util/->jsonb))
        result (db/execute-one! ds (-> (h/update :media-sources)
                                       (h/set prepared)
                                       (h/where [:= :id id])
                                       sql/format))]
    (log/info "Updated media source" {:source-id id})
    result))

(defn delete-media-source! [ds id]
  (let [result (db/execute-one! ds (-> (h/delete-from :media-sources)
                                       (h/where [:= :id id])
                                       sql/format))]
    (log/info "Deleted media source" {:source-id id})
    result))

(defn count-libraries
  "Counts total libraries."
  [ds]
  (let [result (db/query-one ds (-> (h/select [:%count.*])
                                    (h/from :libraries)
                                    sql/format))]
    (or (:count result) 0)))

(defn list-libraries
  "Lists libraries with optional pagination.
   
   opts:
   - :limit  - maximum number of libraries to return
   - :offset - number of libraries to skip"
  ([ds]
   (list-libraries ds nil))
  ([ds opts]
   (db/query ds (-> (h/select :*)
                    (h/from :libraries)
                    (h/order-by :name)
                    (cond->
                     (:limit opts)  (h/limit (:limit opts))
                     (:offset opts) (h/offset (:offset opts)))
                    sql/format))))

(defn list-libraries-for-source [ds source-id]
  (db/query ds (-> (h/select :*)
                   (h/from :libraries)
                   (h/where [:= :media-source-id source-id])
                   sql/format)))

(defn get-library [ds id]
  (db/query-one ds (-> (h/select :*)
                       (h/from :libraries)
                       (h/where [:= :id id])
                       sql/format)))

(defn create-library! [ds attrs]
  (let [result (db/execute-one! ds (-> (h/insert-into :libraries)
                                       (h/values [(update attrs :kind #(sql-util/->pg-enum "library_kind" %))])
                                       sql/format))]
    (log/info "Created library"
              {:library-id      (:libraries/id result)
               :name            (:name attrs)
               :kind            (:kind attrs)
               :media-source-id (:media-source-id attrs)
               :external-id     (:external-id attrs)})
    result))

(defn update-library! [ds id attrs]
  (let [result (db/execute-one! ds (-> (h/update :libraries)
                                       (h/set attrs)
                                       (h/where [:= :id id])
                                       sql/format))]
    (log/info "Updated library" {:library-id id})
    result))

(defn delete-library! [ds id]
  (let [result (db/execute-one! ds (-> (h/delete-from :libraries)
                                       (h/where [:= :id id])
                                       sql/format))]
    (log/info "Deleted library" {:library-id id})
    result))

(defn list-library-paths [ds library-id]
  (db/query ds (-> (h/select :*)
                   (h/from :library-paths)
                   (h/where [:= :library-id library-id])
                   sql/format)))

(defn create-library-path! [ds attrs]
  (let [result (db/execute-one! ds (-> (h/insert-into :library-paths)
                                       (h/values [attrs])
                                       (h/returning :*)
                                       sql/format))]
    (log/info "Created library path"
              {:library-path-id (:library-paths/id result)
               :library-id      (:library-id attrs)
               :path            (:path attrs)})
    result))

(defn delete-library-path! [ds id]
  (db/execute-one! ds (-> (h/delete-from :library-paths)
                          (h/where [:= :id id])
                          sql/format)))

;; ---------------------------------------------------------------------------
;; Media items
;; ---------------------------------------------------------------------------

(defn- item-ref->int
  "If `ref` denotes an internal integer media-item id (an integer, or a string
   of digits that fits the SERIAL `id` column), returns it as a long. Otherwise
   returns nil, meaning the ref should be matched against `remote_key` (e.g. a
   Jellyfin item id)."
  [ref]
  (cond
    (integer? ref) ref
    (and (string? ref) (re-matches #"\d+" ref))
    (let [n (try (Long/parseLong ref) (catch Exception _ nil))]
      (when (and n (<= 1 n Integer/MAX_VALUE)) n))
    :else nil))

(defn- item-ref-match
  "HoneySQL predicate selecting a media item by either its internal integer id
   (`id-col`) or its `remote_key` (`remote-col`, e.g. a Jellyfin item id).

   Numeric refs are matched against both columns so an all-numeric remote_key
   still resolves; non-numeric refs match remote_key only."
  [id-col remote-col ref]
  (if-let [n (item-ref->int ref)]
    [:or [:= id-col n] [:= remote-col (str ref)]]
    [:= remote-col (str ref)]))

(defn resolve-media-item-id
  "Resolves a media-item reference — an internal integer id or a remote_key
   (e.g. a Jellyfin item id) — to its internal integer id, or nil if no such
   item exists. Use this before writing the integer id into a foreign key."
  [ds ref]
  (let [row (db/query-one ds (-> (h/select :mi.id)
                                 (h/from [:media-items :mi])
                                 (h/where (item-ref-match :mi.id :mi.remote-key ref))
                                 sql/format))]
    (when row
      (some row [:id :media-items/id :mi/id]))))

(def ^:private item-attr->col
  "Maps attribute keyword → HoneySQL select expression."
  {:id              :mi.id
   :kind            :mi.kind
   :state           :mi.state
   :parent-id       :mi.parent-id
   :position        :mi.position
   :library-path-id :mi.library-path-id
   :remote-key      [:mi.remote_key :remote-key]
   :remote-etag     [:mi.remote_etag :remote-etag]
   :name            [:m.title :name]
   :year            :m.year
   :release-date    :m.release-date
   :plot            :m.plot
   :tagline         :m.tagline
   :content-rating  :m.content-rating})

(def ^:private metadata-attrs
  "Attributes that require a LEFT JOIN with the metadata table."
  #{:name :year :release-date :plot :tagline :content-rating})

(def ^:private default-item-attrs [:id :name])

(defn- like-escape
  "Escapes LIKE/ILIKE wildcard characters (\\ % _) so a user-supplied term is
   matched as a literal substring rather than a pattern."
  [s]
  (-> s
      (str/replace "\\" "\\\\")
      (str/replace "%" "\\%")
      (str/replace "_" "\\_")))

(defn- search-term
  "Returns the trimmed, non-blank search string from opts, or nil."
  [opts]
  (some-> (:search opts) str/trim not-empty))

(defn- build-media-items-base-query
  "Builds the base query for media items with filters, without pagination.

   When `:search` is a non-blank string, joins the metadata table and adds a
   case-insensitive substring filter on the item title."
  [library-id opts]
  (let [search (search-term opts)
        base (-> (h/from [:media-items :mi])
                 (h/join [:library-paths :lp] [:= :lp.id :mi.library-path-id])
                 (h/where [:= :lp.library-id library-id]))
        with-type (cond-> base
                    (:type opts)
                    (h/where [:= :mi.kind (sql-util/->pg-enum "media_item_kind" (name (:type opts)))]))
        with-parent (cond-> with-type
                      (contains? opts :parent-id)
                      (h/where [:= :mi.parent-id (:parent-id opts)]))
        with-search (cond-> with-parent
                      search
                      (-> (h/left-join [:metadata :m] [:= :m.media-item-id :mi.id])
                          (h/where [:ilike :m.title (str "%" (like-escape search) "%")])))]
    with-search))

(defn count-media-items
  "Counts total media items in a library with optional filtering.
   
   opts:
   - :type      - media_item_kind keyword or string to filter by (e.g. :movie)
   - :parent-id - when present in opts (even if nil), filters by parent_id
   - :search    - case-insensitive substring matched against the item title"
  [ds library-id opts]
  (let [query (-> (build-media-items-base-query library-id opts)
                  (h/select [:%count.*])
                  sql/format)
        result (db/query-one ds query)]
    (or (:count result) 0)))

(defn list-media-items
  "List media items in a library with optional attribute selection and filtering.

   opts:
   - :attrs     - seq of attribute name strings/keywords to include
                  (default: [:id :name]). Special value: :child-count adds a
                  correlated subquery counting direct children of each item.
   - :type      - media_item_kind keyword or string to filter by (e.g. :movie)
   - :parent-id - when present in opts (even if nil), adds a WHERE clause on
                  parent_id; pass an integer to list children of that item.
   - :search    - case-insensitive substring matched against the item title
   - :limit     - maximum number of items to return
   - :offset    - number of items to skip"
  [ds library-id opts]
  (let [attrs       (mapv keyword (or (seq (:attrs opts)) default-item-attrs))
         ;; Always ensure id is present so downstream consumers (including
         ;; response coercion) have a primary key.
        attrs       (if (some #{:id} attrs) attrs (conj attrs :id))
        need-meta?  (some metadata-attrs attrs)
        need-count? (some #{:child-count} attrs)
        col-attrs   (remove #{:child-count} attrs)
        select-cols (cond-> (mapv item-attr->col (filter item-attr->col col-attrs))
                      need-count?
                      (conj [{:select [:%count.*]
                              :from   [[:media-items :ch]]
                              :where  [:= :ch.parent-id :mi.id]}
                             :child-count]))
        base        (-> (build-media-items-base-query library-id opts)
                        (as-> q (apply h/select q select-cols)))
         ;; The base query already joins metadata when a search term is present;
         ;; only add the join here if it's needed for projection and not already
         ;; present, to avoid a duplicate-alias error.
        with-meta   (cond-> base
                      (and need-meta? (not (search-term opts)))
                      (h/left-join [:metadata :m] [:= :m.media-item-id :mi.id]))
        with-order  (h/order-by with-meta :mi.id)
        with-pagination (cond-> with-order
                          (:limit opts)  (h/limit (:limit opts))
                          (:offset opts) (h/offset (:offset opts)))]
    (db/query ds (sql/format with-pagination))))

(defn get-media-item
  "Fetches a media item by reference. `id` may be the internal integer id or a
   remote_key (e.g. a Jellyfin item id)."
  [ds id]
  (db/query-one ds (-> (h/select :mi.id :mi.kind :mi.state :mi.parent-id :mi.position
                                 [:mi.remote_key :remote-key]
                                 [:mi.remote_etag :remote-etag]
                                 [:m.title :name]
                                 :m.year
                                 [:m.release-date :release-date]
                                 :m.plot
                                 [:m.content-rating :content-rating])
                       (h/from [:media-items :mi])
                       (h/left-join [:metadata :m] [:= :m.media-item-id :mi.id])
                       (h/where (item-ref-match :mi.id :mi.remote-key id))
                       sql/format)))

(defn get-media-item-with-source
  "Returns a media item joined to its media source, including the source kind
   and connection_config needed to construct a playback URL. `id` may be the
   internal integer id or a remote_key (e.g. a Jellyfin item id)."
  [ds id]
  (db/query-one ds (-> (h/select :mi.id :mi.kind :mi.state :mi.remote-key
                                 :m.title
                                 :ms.id   :ms.kind
                                 :ms.connection-config)
                       (h/from [:media-items :mi])
                       (h/join [:library-paths :lp] [:= :lp.id :mi.library-path-id])
                       (h/join [:libraries :l]      [:= :l.id :lp.library-id])
                       (h/join [:media-sources :ms]  [:= :ms.id :l.media-source-id])
                       (h/left-join [:metadata :m]   [:= :m.media-item-id :mi.id])
                       (h/where (item-ref-match :mi.id :mi.remote-key id))
                       sql/format)))

(defn get-media-file-path
  "Returns the absolute filesystem path backing a media item (its first version's
   first file), or nil. Used to stream co-mounted local media (e.g. Grout clips)
   directly, without a remote server URL. `id` may be the internal integer id or
   a remote_key."
  [ds id]
  (:media-files/path
   (db/query-one ds (-> (h/select :mf.path)
                        (h/from [:media-items :mi])
                        (h/join [:media-versions :mv] [:= :mv.media-item-id :mi.id])
                        (h/join [:media-files    :mf] [:= :mf.media-version-id :mv.id])
                        (h/where (item-ref-match :mi.id :mi.remote-key id))
                        (h/order-by :mv.id :mf.id)
                        (h/limit 1)
                        sql/format))))

;; ---------------------------------------------------------------------------
;; Media item children
;; ---------------------------------------------------------------------------

(defn count-children
  "Counts direct children of a media item."
  [ds parent-id]
  (let [result (db/query-one
                ds
                (-> (h/select [:%count.*])
                    (h/from :media-items)
                    (h/where [:= :parent-id parent-id])
                    sql/format))]
    (or (:count result) 0)))

(defn list-children
  "Lists direct children of a media item, ordered by position then name.

   opts:
   - :limit  - maximum number of items to return
   - :offset - number of items to skip
   - :search - case-insensitive substring matched against the item title"
  [ds parent-id opts]
  (let [search (some-> (:search opts) str/trim not-empty)
        base (-> (h/select :mi.id :mi.kind :mi.state :mi.parent-id :mi.position
                           [:mi.remote_key :remote-key]
                           [:mi.remote_etag :remote-etag]
                           [:m.title :name]
                           :m.year
                           [:m.release-date :release-date]
                           :m.plot
                           [:m.content-rating :content-rating])
                 (h/from [:media-items :mi])
                 (h/left-join [:metadata :m] [:= :m.media-item-id :mi.id])
                 (h/where [:= :mi.parent-id parent-id]))
        with-search (cond-> base
                      search
                      (h/where [:ilike :m.title (str "%" (like-escape search) "%")]))
        with-order (h/order-by with-search :mi.position :mi.id)
        with-pagination (cond-> with-order
                          (:limit opts)  (h/limit (:limit opts))
                          (:offset opts) (h/offset (:offset opts)))]
    (db/query ds (sql/format with-pagination))))

;; ---------------------------------------------------------------------------
;; Season-aware / tag-scoped playable resolution
;;
;; Shared by the daily-slots ingest (pseudovision.http.api.daily-slots) and the
;; native scheduling engine's collection resolver (pseudovision.db.collections)
;; so "all episodes of show X" and "all episodes matching genre tag Y" are
;; resolved identically everywhere, instead of maintaining two copies of the
;; season-traversal / parent-tag-inheritance logic that previously only lived
;; in daily-slots.clj.
;; ---------------------------------------------------------------------------

(defn list-show-episodes-by-id
  "Ordered playable episodes for the show with internal id `show-id`.
   Episodes hang off seasons (episode.parent_id -> season -> show), so each
   episode's parent-as-season is joined to reach the show; a direct
   show->episode link (flat libraries) is also honoured.

   Each row carries `:_top-id` (the show's own id) so callers can look up
   show-level tags — genre/channel/etc. tags live on the show's metadata, not
   the episode's."
  [ds show-id]
  (mapv #(assoc % :_top-id show-id)
        (db/query ds
          (-> (h/select :mi.* [:mv.duration :duration])
              (h/from [:media-items :mi])
              (h/left-join [:media-items :season]
                           [:and [:= :season.id :mi.parent-id]
                                 [:= :season.kind (sql-util/->pg-enum "media_item_kind" "season")]])
              (h/left-join [:media-versions :mv] [:= :mv.media-item-id :mi.id])
              (h/where [:and
                        [:= :mi.kind (sql-util/->pg-enum "media_item_kind" "episode")]
                        [:= :mi.state (sql-util/->pg-enum "media_item_state" "normal")]
                        [:or [:= :mi.parent-id show-id]
                             [:= :season.parent-id show-id]]])
              (h/order-by :season.position :mi.position :mi.id)
              sql/format))))

(defn- exact-tag-exists-subq
  "HoneySQL EXISTS subquery: does the top-level item (`:mtop`) carry the exact
   tag `tag-name` (case-insensitive)? Used for extra scoping tags (e.g.
   `channel:<slug>`) that must ALSO be present, ANDed with the category match
   in `resolve-playable-by-tag` — unlike the category match, these are exact,
   not bare-or-`genre:`-prefixed."
  [tag-name]
  [:exists {:select [1]
            :from   [[:metadata-tags :et]]
            :where  [:and [:= :et.metadata-id :mtop.id]
                          [:= [:lower :et.name] (str/lower-case tag-name)]]}])

(defn resolve-playable-by-tag
  "Resolves a genre/category tag (bare or `genre:`-prefixed, case-insensitive,
   kebab-case tolerant) to concrete *playable* items: matching shows are
   expanded to their (season-nested) episodes, matching movies resolve to
   themselves. Mirrors the matching semantics of a `random:<category>` pool
   at air time.

   `:require-tags` (optional) — exact tag strings (e.g. `channel:hua`) the
   top-level item must ALSO carry, ANDed with the category match. Used to
   scope a category pool to one channel's own mapped media, same as the
   `channel-tag` scoping `publish-daily-slots!` applies to `random:<category>`
   pools today.

   Each row carries `:_top-id` — the id of the top-level item (show or movie)
   it was resolved from — for show-level tag lookups downstream."
  [ds category & {:keys [require-tags]}]
  (db/query ds
    (-> (h/select-distinct :play.* [:mvp.duration :duration] [:top.id :_top-id])
        (h/from [:media-items :top])
        (h/join [:metadata :mtop] [:= :mtop.media-item-id :top.id])
        (h/left-join [:metadata-tags :t] [:= :t.metadata-id :mtop.id])
        (h/left-join [:media-items :season]
                     [:and [:= :season.parent-id :top.id]
                           [:= :season.kind (sql-util/->pg-enum "media_item_kind" "season")]])
        (h/left-join [:media-items :play]
                     [:or
                      ;; Movies and programs are flat: they ARE their own
                      ;; playable item (play.id = top.id).
                      [:and [:in :top.kind [(sql-util/->pg-enum "media_item_kind" "movie")
                                            (sql-util/->pg-enum "media_item_kind" "program")]]
                            [:= :play.id :top.id]]
                      [:and [:= :top.kind (sql-util/->pg-enum "media_item_kind" "show")]
                            [:= :play.kind (sql-util/->pg-enum "media_item_kind" "episode")]
                            [:or [:= :play.parent-id :top.id]
                                 [:= :play.parent-id :season.id]]]])
        (h/left-join [:media-versions :mvp] [:= :mvp.media-item-id :play.id])
        (h/where (into [:and
                        [:= :top.state (sql-util/->pg-enum "media_item_state" "normal")]
                        [:= :play.state (sql-util/->pg-enum "media_item_state" "normal")]
                        [:in :top.kind [(sql-util/->pg-enum "media_item_kind" "show")
                                        (sql-util/->pg-enum "media_item_kind" "movie")
                                        (sql-util/->pg-enum "media_item_kind" "program")]]
                        [:or [:= [:lower :t.name] (str/lower-case category)]
                             [:= [:lower :t.name] (str "genre:" (str/lower-case category))]
                             [:= [:lower :t.name] (tags/kebab-case category)]
                             [:= [:lower :t.name] (str "genre:" (tags/kebab-case category))]]]
                       (map exact-tag-exists-subq)
                       require-tags))
        (h/order-by :play.id)
        sql/format)))

(defn list-items-for-library-path [ds library-path-id]
  (db/query ds (-> (h/select :*)
                   (h/from :media-items)
                   (h/where [:= :library-path-id library-path-id])
                   sql/format)))

(defn list-items-for-library [ds library-id]
  (db/query ds (-> (h/select :mi.*)
                   (h/from [:media-items :mi])
                   (h/join [:library-paths :lp] [:= :lp.id :mi.library-path-id])
                   (h/where [:= :lp.library-id library-id])
                   (h/order-by :mi.id)
                   sql/format)))

(defn list-items-for-collection
  "Returns all media items in a manual collection, in playback order."
  [ds collection-id]
  (db/query ds (-> (h/select :mi.*)
                   (h/from [:media-items :mi])
                   (h/join [:collection-items :ci] [:= :ci.media-item-id :mi.id])
                   (h/where [:= :ci.collection-id collection-id])
                   (h/order-by :ci.custom-order :mi.id)
                   sql/format)))

(defn get-item-by-path [ds path]
  (db/query-one ds (-> (h/select :mi.*)
                       (h/from [:media-items :mi])
                       (h/join [:media-versions  :mv] [:= :mv.media-item-id :mi.id])
                       (h/join [:media-files     :mf] [:= :mf.media-version-id :mv.id])
                       (h/where [:= :mf.path path])
                       sql/format)))

(defn upsert-media-item!
  "Upserts a single `media_items` row and returns the row map (with
  unqualified kebab-case keys, including `:id`), or nil if no row was
  written (should never happen for a valid insert).

  IMPORTANT: every INSERT here must end in `(h/returning :id)`. Without
  it, the next.jdbc driver uses `Statement.RETURN_GENERATED_KEYS` which
  returns an empty result set for `ON CONFLICT DO UPDATE` (no keys are
  generated on the update path), so the caller would see `(:id result) =>
  nil` even though the row was written. We hit this exact bug on
  2026-07-09: scans reported `:synced N` for every library, `media_items`
  was getting upserted, but every subsequent call gated on `(:id
  item-row)` was silently skipped, so `media_versions`,
  `media_files`, and `media_streams` stayed empty and no playout could
  ever resolve a file path. The `(h/returning :id)` clause makes the
  RETURNING explicit so the driver always returns the row, regardless
  of whether the insert hit the conflict-update branch or the fresh-
  insert branch."
  [ds attrs]
  (let [prepared (cond-> attrs
                   (:kind attrs)  (update :kind #(sql-util/->pg-enum "media_item_kind" %))
                   (:state attrs) (update :state #(sql-util/->pg-enum "media_item_state" %)))]
    (if (:remote-key attrs)
      ;; For items with remote_key, use ON CONFLICT
      (let [sql-vec  (-> (h/insert-into :media-items)
                         (h/values [prepared])
                         (h/on-conflict :library-path-id :remote-key)
                         (h/do-update-set :state :remote-key :remote-etag :position)
                         (h/returning :id)
                         sql/format)]
        (log/debug "Media item upsert SQL (with remote-key)" {:sql (first sql-vec)})
        (let [result (db/execute-one! ds sql-vec)]
          (log/debug "Upserted media item"
                    {:media-item-id   (:id result)
                     :kind            (:kind attrs)
                     :library-path-id (:library-path-id attrs)
                     :remote-key      (:remote-key attrs)
                     :parent-id       (:parent-id attrs)})
          result))
      ;; For items without remote_key, just insert
      (let [result (db/execute-one! ds (-> (h/insert-into :media-items)
                                           (h/values [prepared])
                                           (h/returning :id)
                                           sql/format))]
        (log/debug "Inserted media item (no remote-key)"
                  {:media-item-id   (:id result)
                   :kind            (:kind attrs)
                   :library-path-id (:library-path-id attrs)
                   :parent-id       (:parent-id attrs)})
        result))))

;; ---------------------------------------------------------------------------
;; Collections
;; ---------------------------------------------------------------------------

(defn count-collections
  "Counts total collections."
  [ds]
  (let [result (db/query-one ds (-> (h/select [:%count.*])
                                    (h/from :collections)
                                    sql/format))]
    (or (:count result) 0)))

(defn list-collections
  "Lists collections with optional pagination.
   
   opts:
   - :limit  - maximum number of collections to return
   - :offset - number of collections to skip"
  ([ds]
   (list-collections ds nil))
  ([ds opts]
   (db/query ds (-> (h/select :*)
                    (h/from :collections)
                    (h/order-by :name)
                    (cond->
                     (:limit opts)  (h/limit (:limit opts))
                     (:offset opts) (h/offset (:offset opts)))
                    sql/format))))

(defn get-collection [ds id]
  (db/query-one ds (-> (h/select :*)
                       (h/from :collections)
                       (h/where [:= :id id])
                       sql/format)))

(defn create-collection! [ds attrs]
  ;; TEMP-DIAGNOSTIC-LOG: 2026-07-13 (round 2) — the previous diagnostic
  ;; showed that the post-cond-> map has :config as a PGobject with the
  ;; full JSON, yet the stored row's config column ends up as {} (the
  ;; DEFAULT). This new diagnostic logs the *exact* SQL string and
  ;; parameter vector that next.jdbc is about to send, so we can see
  ;; whether HoneySQL is preserving the PGobject all the way to the
  ;; PreparedStatement, or whether something in the SQL pipeline is
  ;; stripping it.
  (log/debug "create-collection!: attrs at db entry" {:attrs attrs})
  (let [honey-map    (-> (h/insert-into :collections)
                         (h/values [(cond-> attrs
                                      (:kind attrs)   (update :kind #(sql-util/->pg-enum "collection_kind" %))
                                      (:config attrs) (update :config sql-util/->jsonb))]))
        ;; Call sql/format with {:params true} to get a [sql-string params]
        ;; vector back. Default sql/format just returns the HoneySQL map (which
        ;; next.jdbc consumes directly via its own format call). The previous
        ;; diagnostic tried to destructure sql/format as a vector and crashed
        ;; with ISeq-from-PGobject because the format was a HoneySQL map.
        [sql-str params] (sql/format honey-map {:params true})
        param-shapes (mapv (fn [p]
                             (cond
                               (instance? org.postgresql.util.PGobject p)
                               {:type "PGobject"
                                :pg-type (.getType ^org.postgresql.util.PGobject p)
                                :value-preview (let [v (.getValue ^org.postgresql.util.PGobject p)]
                                                 (if (> (count v) 80)
                                                   (str (subs v 0 80) "...[" (count v) " chars]")
                                                   v))}
                               :else {:type (.getName (class p)) :value (str p)}))
                           params)]
    (log/debug "create-collection!: formatted SQL + param shapes"
               {:sql sql-str
                :param-count (count params)
                :param-shapes param-shapes})
    (let [result (db/execute-one! ds honey-map)]
      (log/debug "create-collection!: result from RETURNING"
                 {:id            (:id result)
                  :config        (:config result)
                  :config-class  (some-> (:config result) class .getName)
                  :all-keys      (keys result)})
      (log/info "Created collection"
                {:collection-id (:id result)
                 :name          (:name attrs)
                 :kind          (:kind attrs)})
      result)))

(defn update-collection! [ds id attrs]
  ;; TEMP-DIAGNOSTIC-LOG: 2026-07-13 (round 2) — same depth as
  ;; create-collection!: log the exact SQL + param shapes, and the
  ;; round-tripped row's :config. PUT /api/media/collections/{id}
  ;; also returns config: {} in the response, so we need to see
  ;; whether the bug is in the SQL path or in the response path.
  (let [honey-map    (-> (h/update :collections)
                          (h/set (cond-> attrs
                                   (:config attrs) (update :config sql-util/->jsonb)))
                          (h/where [:= :id id]))
        [sql-str params] (sql/format honey-map {:params true})
        param-shapes (mapv (fn [p]
                             (cond
                               (instance? org.postgresql.util.PGobject p)
                               {:type "PGobject"
                                :pg-type (.getType ^org.postgresql.util.PGobject p)
                                :value-preview (let [v (.getValue ^org.postgresql.util.PGobject p)]
                                                 (if (> (count v) 80)
                                                   (str (subs v 0 80) "...[" (count v) " chars]")
                                                   v))}
                               :else {:type (.getName (class p)) :value (str p)}))
                           params)]
    (log/debug "update-collection!: formatted SQL + param shapes"
               {:sql sql-str
                :param-count (count params)
                :param-shapes param-shapes
                :attrs-config-key? (contains? attrs :config)})
    (let [result (db/execute-one! ds honey-map)]
      (log/debug "update-collection!: result from RETURNING"
                 {:id            (:id result)
                  :config        (:config result)
                  :config-class  (some-> (:config result) class .getName)
                  :all-keys      (keys result)})
      (log/info "Updated collection" {:collection-id id})
      result)))

(defn delete-collection! [ds id]
  (db/execute-one! ds (-> (h/delete-from :collections)
                          (h/where [:= :id id])
                          sql/format)))

(defn list-items-in-collection
  "Returns collection_items rows for a manual collection, ordered by custom_order."
  [ds collection-id]
  (db/query ds (-> (h/select :ci.* [:mi.id :media-item-id] [:m.title :name])
                   (h/from [:collection-items :ci])
                   (h/join [:media-items :mi] [:= :mi.id :ci.media-item-id])
                   (h/left-join [:metadata :m] [:= :m.media-item-id :mi.id])
                   (h/where [:= :ci.collection-id collection-id])
                   (h/order-by [[:coalesce :ci.custom-order :mi.id]])
                   sql/format)))

(defn add-item-to-collection! [ds collection-id media-item-id]
  (let [result (db/execute-one! ds (-> (h/insert-into :collection-items)
                                       (h/values [{:collection-id collection-id
                                                   :media-item-id media-item-id}])
                                       (h/on-conflict :collection-id :media-item-id)
                                       (h/do-nothing)
                                       sql/format))]
    (log/info "Added item to collection"
              {:collection-id collection-id
               :media-item-id media-item-id})
    result))

(defn remove-item-from-collection! [ds collection-id media-item-id]
  (db/execute-one! ds (-> (h/delete-from :collection-items)
                          (h/where [:and
                                    [:= :collection-id collection-id]
                                    [:= :media-item-id media-item-id]])
                          sql/format)))
