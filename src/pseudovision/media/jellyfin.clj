(ns pseudovision.media.jellyfin
  "Jellyfin media server scanner.

   Connect → Discover Libraries → Fetch Items → Upsert

   For each synced library:
     1. Fetch items from the Jellyfin API with full metadata fields
     2. Map Jellyfin item types to pseudovision media_item_kinds
     3. Upsert media_items using remote_key (Jellyfin item ID)
     4. Skip unchanged items via remote_etag (Jellyfin Etag header)
     5. Upsert media_versions, media_files, media_streams, and metadata"
  (:require [clj-http.client              :as http]
            [clojure.string              :as str]
            [camel-snake-kebab.core      :as csk]
            [pseudovision.util.tags      :as tags]
            [next.jdbc                   :as jdbc]
            [honey.sql                   :as sql]
            [honey.sql.helpers           :as h]
            [pseudovision.db.core        :as db-core]
            [pseudovision.db.media       :as db]
            [pseudovision.media.connection :as conn]
            [pseudovision.util.sql       :as sql-util]
            [taoensso.timbre             :as log])
  (:import [java.security MessageDigest]
           [java.util HexFormat]))

;; ---------------------------------------------------------------------------
;; Connection config multimethods
;; ---------------------------------------------------------------------------

(defmethod conn/->connection-config "jellyfin" [params]
  {:api-key     (:api-key params)
   :connections (or (:connections params) [])})

(defmethod conn/<-connection-config "jellyfin" [source]
  (let [config (or (:media-sources/connection-config source)
                   (:connection-config source))
        source-id (or (:media-sources/id source) (:id source))
        base-url (conn/active-uri (:connections config))
        api-key  (:api-key config)]
    (log/info "Extracting Jellyfin connection config"
              {:source-id source-id
               :has-config (boolean config)
               :has-connections (boolean (:connections config))
               :connections-count (count (:connections config))
               :has-base-url (boolean base-url)
               :has-api-key (boolean api-key)})
    {:base-url base-url
     :api-key  api-key}))

;; ---------------------------------------------------------------------------
;; Jellyfin API client
;; ---------------------------------------------------------------------------

(defn- jellyfin-get
  "Performs an authenticated GET against the Jellyfin server.
   Returns the parsed JSON body or nil on failure."
  [base-url path api-key & {:keys [query-params]}]
  (try
    (let [url  (str base-url path)
          resp (http/get url
                         {:headers          {"X-Emby-Token" api-key}
                          :query-params     query-params
                          :as               :json
                          :throw-exceptions false
                          :socket-timeout   30000
                          :connection-timeout 10000})]
      (when (<= 200 (:status resp) 299)
        (:body resp)))
    (catch Exception e
      (log/warn e "Jellyfin API request failed" {:path path})
      nil)))

;; ---------------------------------------------------------------------------
;; Server discovery
;; ---------------------------------------------------------------------------

(defn check-server
  "Verifies connectivity with the Jellyfin server. Returns server info or nil."
  [base-url api-key]
  (log/info "Checking Jellyfin server connectivity" {:url base-url})
  (let [result (jellyfin-get base-url "/System/Info" api-key)]
    (if result
      (log/info "Jellyfin server check successful"
                {:url base-url
                 :server-name (:ServerName result)
                 :version (:Version result)})
      (log/warn "Jellyfin server check failed" {:url base-url}))
    result))

(defn fetch-libraries
  "Fetches the virtual folder (library) list from Jellyfin."
  [base-url api-key]
  (log/info "Fetching libraries from Jellyfin" {:url base-url})
  (let [result (jellyfin-get base-url "/Library/VirtualFolders" api-key)]
    (if result
      (log/info "Successfully fetched Jellyfin libraries"
                {:url base-url :count (count result)})
      (log/warn "Failed to fetch Jellyfin libraries" {:url base-url}))
    result))

;; ---------------------------------------------------------------------------
;; Item fetching
;; ---------------------------------------------------------------------------

(def ^:private item-fields
  "Fields to request from the Jellyfin /Items endpoint.

   `Etag` is the Jellyfin-provided change marker (a hash of the item's state).
   It MUST be in this list - without it, every item arrives with `:Etag nil`
   and the tiered scan in `upsert-item!` falls back to the slow path because
   `(or (:Etag item) ...)` always evaluates to `\"\"`. We rely on Etag to skip
   the ~99% of items that don't change between nightly scans."
  (str "Etag,Path,Overview,Genres,Studios,People,MediaStreams,Chapters,"
       "DateCreated,ProviderIds,OfficialRating,CommunityRating,"
       "ProductionYear,PremiereDate,SortName,MediaSources,"
       "SeasonId,SeriesId"))

(def ^:private include-item-types
  "Jellyfin item types to include when scanning."
  "Movie,Series,Season,Episode,MusicVideo,Audio,Photo")

(defn- fetch-items-page
  "Fetches a single page of items from a Jellyfin library."
  [base-url api-key parent-id start-index limit]
  (jellyfin-get base-url "/Items" api-key
                :query-params {"ParentId"         parent-id
                               "Recursive"        "true"
                               "Fields"           item-fields
                               "IncludeItemTypes" include-item-types
                               "StartIndex"       (str start-index)
                               "Limit"            (str limit)
                               "SortBy"           "SortName"
                               "SortOrder"        "Ascending"}))

(defn- fetch-all-items
  "Pages through all items in a Jellyfin library."
  [base-url api-key parent-id]
  (let [page-size 100]
    (loop [start  0
           result []]
      (let [page  (fetch-items-page base-url api-key parent-id start page-size)
            items (get page :Items [])
            total (get page :TotalRecordCount 0)]
        (if (or (empty? items) (>= (+ start (count items)) total))
          (into result items)
          (recur (+ start (count items))
                 (into result items)))))))

;; ---------------------------------------------------------------------------
;; Type mapping
;; ---------------------------------------------------------------------------

(defn- jellyfin-type->kind
  "Maps a Jellyfin item Type string to a pseudovision media_item_kind keyword."
  [jf-type]
  (case jf-type
    "Movie"      :movie
    "Series"     :show
    "Season"     :season
    "Episode"    :episode
    "MusicVideo" :music_video
    "Audio"      :song
    "Photo"      :image
    nil))

(defn- needs-parent?
  "Returns true for item kinds that require a parent_id in the schema."
  [kind]
  (contains? #{:season :episode :music_video} kind))

(defn- item-parent-jf-id
  "Returns the Jellyfin ID of an item's logical parent.
   Episodes use SeasonId, falling back to SeriesId when the episode has no Season
   (e.g. flat Jellyfin libraries). Seasons use SeriesId. Other types use ParentId."
  [item kind]
  (let [result (case kind
                 :episode (or (:SeasonId item) (:SeriesId item) (:ParentId item))
                 :season  (or (:SeriesId item) (:ParentId item))
                 (:ParentId item))]
    result))

;; ---------------------------------------------------------------------------
;; Stream mapping
;; ---------------------------------------------------------------------------

(defn- jellyfin-stream->attrs
  "Converts a Jellyfin MediaStream to a media_streams attribute map.

  The `:kind` field is wrapped in a PGobject for the `stream_kind` enum so
  the INSERT doesn't blow up on `column \"kind\" is of type stream_kind but
  expression is of type character varying`. Without this, the whole
  transaction (media_items + media_versions + media_files) rolls back
  when the streams insert fails, leaving the playout data path empty."
  [idx stream]
  {:stream-index    idx
   :kind            (sql-util/->pg-enum "stream_kind"
                                         (case (:Type stream)
                                           "Video"    "video"
                                           "Audio"    "audio"
                                           "Subtitle" "subtitle"
                                           "video"))
   :codec           (:Codec stream)
   :profile         (:Profile stream)
   :language        (:Language stream)
   :channels        (or (:Channels stream) 0)
   :title           (:DisplayTitle stream)
   :is-default      (boolean (:IsDefault stream))
   :is-forced       (boolean (:IsForced stream))
   :pixel-format    (:PixelFormat stream)
   :color-range     (:ColorRange stream)
   :color-space     (:ColorSpace stream)
   :color-transfer  (:ColorTransfer stream)
   :color-primaries (:ColorPrimaries stream)})

;; ---------------------------------------------------------------------------
;; Version / file mapping
;; ---------------------------------------------------------------------------

(defn- ticks->duration
  "Converts Jellyfin RunTimeTicks (100ns units) to a java.time.Duration.
   Returns Duration/ZERO when ticks is nil (e.g. for Season/Series items)."
  [ticks]
  (if ticks
    (java.time.Duration/ofMillis (quot ticks 10000))
    java.time.Duration/ZERO))

(defn- path-hash
  "SHA-256 hex of a path string for deduplication."
  [^String path]
  (let [md  (MessageDigest/getInstance "SHA-256")
        raw (.digest md (.getBytes path "UTF-8"))]
    (.formatHex (HexFormat/of) raw)))

(defn- item->version-attrs
  "Extracts media_versions attributes from a Jellyfin item."
  [item]
  (let [streams (:MediaStreams item [])
        video   (first (filter #(= "Video" (:Type %)) streams))]
    {:duration             (-> (:RunTimeTicks item)
                               ticks->duration
                               sql-util/duration->pg-interval
                               sql-util/->pg-interval)
     :width                (or (:Width video) 0)
     :height               (or (:Height video) 0)
     :r-frame-rate         (some-> (:RealFrameRate video) str)
     :display-aspect-ratio (:AspectRatio video)
     :video-scan-kind      (if (:IsInterlaced video) "interlaced" "progressive")}))

;; ---------------------------------------------------------------------------
;; Metadata extraction
;; ---------------------------------------------------------------------------

(defn- item->metadata-attrs
  "Extracts metadata table attributes from a Jellyfin item."
  [item kind]
  (cond-> {:kind           (name kind)
           :title          (:Name item)
           :sort-title     (:SortName item)
           :original-title (:OriginalTitle item)
           :year           (or (:ProductionYear item)
                               (some-> (:PremiereDate item) (subs 0 4) parse-long))
           :plot           (:Overview item)
           :content-rating (:OfficialRating item)}
    (= kind :episode)
    (assoc :episode-number (:IndexNumber item))

    (= kind :song)
    (assoc :album        (:Album item)
           :track-number (:IndexNumber item))))

;; ---------------------------------------------------------------------------
;; Upsert helpers
;; ---------------------------------------------------------------------------

(defn- find-parent-item
  "Looks up the parent media_item by its Jellyfin ID (remote_key) in our DB."
  [tx library-path-id parent-jf-id]
  (when parent-jf-id
    (db-core/query-one tx
                  (-> (h/select :id)
                      (h/from :media-items)
                      (h/where [:and
                                [:= :library-path-id library-path-id]
                                [:= :remote-key parent-jf-id]])
                      sql/format))))

(defn- upsert-version-and-file!
  "Upserts a media version and its backing file for an item with a physical path.

  Each INSERT ends in `(h/returning :id)` so the next.jdbc driver returns the
  inserted row's id via a `RETURNING id` clause (PR #123). All `execute-one!`
  calls go through `db/execute-one!` (the project's wrapper), which sets
  `:builder-fn rs/as-unqualified-kebab-maps` and `:return-keys true` so the
  returned row map has unqualified kebab-case keys (`:id`, not
  `:media-versions/id`). Using the raw `next.jdbc/execute-one!` (which has
  the default `as-maps` builder-fn and returns *qualified* keys) silently
  makes `(:id ver)` return nil and the `(when ver-id ...)` block never
  runs — see `db.upsert-media-item!` docstring for the full writeup."
  [tx item-id item]
  (when (:Path item)
    ;; Delete existing version(s) for this item and let cascade clean up files/streams
    (db-core/execute-one! tx
                          (sql/format
                           (-> (h/delete-from :media-versions)
                               (h/where [:= :media-item-id item-id]))))
    (let [ver-attrs (assoc (item->version-attrs item) :media-item-id item-id)
          ver       (db-core/execute-one! tx
                                          (sql/format
                                           (-> (h/insert-into :media-versions)
                                               (h/values [ver-attrs])
                                               (h/returning :id))))
          ver-id    (:id ver)]
      (log/debug "upsert-version-and-file!: version insert result"
                 {:item-id item-id
                  :path    (:Path item)
                  :ver-row ver
                  :ver-id  ver-id
                  :sql-explicit-returning? true})
      (when ver-id
        (log/info "Created media version"
                  {:media-version-id ver-id
                   :media-item-id item-id
                   :width (:width ver-attrs)
                   :height (:height ver-attrs)
                   :duration (str (:duration ver-attrs))})
        ;; Insert file
        (let [file-result (db-core/execute-one! tx
                                                (sql/format
                                                 (-> (h/insert-into :media-files)
                                                     (h/values [{:media-version-id ver-id
                                                                 :path             (:Path item)
                                                                 :path-hash        (path-hash (:Path item))}])
                                                     (h/on-conflict :path-hash)
                                                     (h/do-update-set :media-version-id :path)
                                                     (h/returning :id))))]
          (log/debug "upsert-version-and-file!: file insert result"
                     {:item-id item-id
                      :ver-id  ver-id
                      :path    (:Path item)
                      :file-row file-result
                      :file-id  (:id file-result)})
          (log/info "Upserted media file"
                    {:media-file-id (:id file-result)
                     :media-version-id ver-id
                     :path (:Path item)}))
        ;; Insert streams
        (let [streams (map-indexed jellyfin-stream->attrs (:MediaStreams item []))]
          (when (seq streams)
            (db-core/execute! tx
                              (sql/format
                               (-> (h/insert-into :media-streams)
                                   (h/values (mapv #(assoc % :media-version-id ver-id) streams)))))
            (log/info "Created media streams"
                      {:media-version-id ver-id
                       :stream-count (count streams)
                       :video-streams (count (filter #(= "video" (:kind %)) streams))
                       :audio-streams (count (filter #(= "audio" (:kind %)) streams))
                       :subtitle-streams (count (filter #(= "subtitle" (:kind %)) streams))}))))
      ver-id)))

(defn- upsert-metadata!
  "Upserts metadata, genres, and studios for an item."
  [tx item-id item kind]
  (let [meta-attrs (-> (item->metadata-attrs item kind)
                       (assoc :media-item-id item-id)
                       (update :kind #(sql-util/->pg-enum "media_item_kind" %)))]
    (jdbc/execute-one! tx
                       (sql/format
                        (-> (h/insert-into :metadata)
                            (h/values [meta-attrs])
                            (h/on-conflict :media-item-id)
                            (h/do-update-set :title :sort-title :original-title
                                             :year :plot :content-rating
                                             :episode-number :album :track-number
                                             :date-updated)))))
  ;; Fetch the metadata row ID for genre/studio insertion. The metadata row
  ;; is keyed by `:id` (unqualified, kebab-case — see `db/execute-one!`'s
  ;; builder-fn). The insert above is also affected by the same RETURNING
  ;; pitfall, but the immediate SELECT below is idempotent and doesn't need
  ;; a RETURNING clause to recover the id.
  (let [meta-row (jdbc/execute-one! tx
                                    (sql/format
                                     (-> (h/select :id)
                                         (h/from :metadata)
                                         (h/where [:= :media-item-id item-id]))))
        meta-id  (:id meta-row)]
    (when meta-id
      (log/info "Upserted metadata"
                {:metadata-id meta-id
                 :media-item-id item-id
                 :title (:Name item)
                 :year (:ProductionYear item)
                 :kind (name kind)})
      ;; Genres — written as `genre:<kebab>` rows in `metadata_tags`
      ;; (the canonical post-DIMENSION_CLEANUP storage). The `metadata_genres`
      ;; table was dropped in migration 20260703-001.
      (when (seq (:Genres item))
        (jdbc/execute-one! tx
                           (sql/format
                            (-> (h/delete-from :metadata-tags)
                                (h/where [:and
                                          [:= :metadata-id meta-id]
                                          [:like :name "genre:%"]]))))
        (jdbc/execute! tx
                       (sql/format
                        (-> (h/insert-into :metadata-tags)
                            (h/values
                             (mapv (fn [g]
                                     {:metadata-id meta-id
                                      :name (str "genre:" (tags/kebab-case g))})
                                   (:Genres item))))))
        (log/info "Added metadata genre tags"
                  {:metadata-id meta-id
                   :genre-count (count (:Genres item))
                   :genres (:Genres item)}))
      ;; Studios
      (when (seq (:Studios item))
        (jdbc/execute-one! tx
                           (sql/format
                            (-> (h/delete-from :metadata-studios)
                                (h/where [:= :metadata-id meta-id]))))
        (jdbc/execute! tx
                       (sql/format
                        (-> (h/insert-into :metadata-studios)
                            (h/values (mapv (fn [s] {:metadata-id meta-id :name (:Name s)})
                                            (:Studios item))))))
        (log/info "Added metadata studios"
                  {:metadata-id meta-id
                   :studio-count (count (:Studios item))
                   :studios (mapv :Name (:Studios item))})))))

;; ---------------------------------------------------------------------------
;; Single item upsert
;; ---------------------------------------------------------------------------

(defn- upsert-tier
  "Pure function: given a Jellyfin item's etag and the pre-fetched
   etag-state entry for it (or nil if the item is brand new), return one
   of `:new` / `:unchanged` / `:repair-duration` / `:changed`.

   `existing` is `nil` (item not in PV) or `{:id :remote-etag :has-zero-duration?}`.

   This is split out from `upsert-item!` so the tier logic can be tested
   without a real DB or any macro-magic on `with-transaction`."
  [etag existing]
  (cond
    (nil? existing)              :new
    (not= etag (:remote-etag existing)) :changed
    (:has-zero-duration? existing)       :repair-duration
    :else                              :unchanged))

(defn- upsert-item!
  "Upserts a single Jellyfin item into the database.

   `etag-state` is a pre-fetched map of `remote_key → {:id :remote-etag
   :has-zero-duration?}` from `db/list-library-etag-state`. The per-item
   upsert is tiered on it:

   - NEW (no entry in etag-state)        → full upsert (item + version + metadata)
   - UNCHANGED (same etag, has duration) → skip entirely; zero SQL, zero work
   - REPAIR-DURATION (same etag, d=0)    → version-only re-upsert to refill
                                            `media-versions.duration`
   - CHANGED (different etag)            → full upsert; this is how metadata
                                            edits propagate

   The UNCHANGED skip is what makes the nightly scan take seconds instead of
   hours: the pre-fetch turns N roundtrips into 1 SELECT, and unchanged
   items cost nothing.

   The REPAIR-DURATION branch is the defensive guard for the bug PR #121
   fixed (commit `690fed7`): items that never had their `RunTimeTicks`
   recorded kept `duration = INTERVAL '0'` forever, which broke
   `playable-item?`. With this tiered approach, the only way duration can
   stay zero is if Jellyfin changes the item's Etag AND keeps the duration
   at zero (which Jellyfin does not do), so we keep that guard.

   Returns the upserted media_items row, or nil if the item was skipped
   (parent not yet synced, or kind not recognized)."
  [db library-path-id item etag-state]
  (let [jf-id   (:Id item)
        jf-type (:Type item)
        kind    (jellyfin-type->kind jf-type)
        etag    (or (:Etag item) (str (:DateLastSaved item)))]
    (when kind
      (let [existing (get etag-state jf-id)
            tier     (upsert-tier etag existing)]
        (case tier
          ;; Both NEW and CHANGED run the full upsert path. They differ
          ;; only in the log line: operators benefit from seeing whether
          ;; a scan is mostly discovering new content or mostly refreshing
          ;; edits to existing content.
          (:new :changed)
          (jdbc/with-transaction [tx db]
            (let [parent-jf-id (item-parent-jf-id item kind)
                  parent-row   (when (needs-parent? kind)
                                 (find-parent-item tx library-path-id parent-jf-id))
                  parent-id    (or (:media-items/id parent-row) (:id parent-row))]
              (if (and (needs-parent? kind) (nil? parent-id))
                (do (log/debug "Skipping item - parent not yet synced"
                               {:id           jf-id
                                :type         jf-type
                                :parent-jf-id parent-jf-id
                                :parent-row   parent-row})
                    nil)
                (let [item-row (db/upsert-media-item! tx
                                                      (cond-> {:kind             (name kind)
                                                               :state            "normal"
                                                               :library-path-id  library-path-id
                                                               :remote-key       jf-id
                                                               :remote-etag      etag}
                                                        parent-id
                                                        (assoc :parent-id parent-id)

                                                        (some? (:IndexNumber item))
                                                        (assoc :position (:IndexNumber item))))
                      item-id  (:id item-row)]
                  (log/debug "upsert-item!: full upsert"
                             {:jf-id jf-id :kind kind :tier tier :item-id item-id})
                  (when item-id
                    (upsert-version-and-file! tx item-id item)
                    (upsert-metadata! tx item-id item kind))
                  item-row))))

          ;; REPAIR-DURATION: same etag, but the prior scan never recorded
          ;; RunTimeTicks (PR #121's bug). Refresh the version+streams+file
          ;; so duration comes back, but skip metadata (it's already right).
          :repair-duration
          (jdbc/with-transaction [tx db]
            (let [item-id (:id existing)]
              (log/debug "upsert-item!: REPAIR-DURATION - re-upsert version"
                         {:jf-id jf-id :kind kind :item-id item-id})
              (upsert-version-and-file! tx item-id item)
              ;; Return a synthetic row for the caller's tallying
              {:id item-id :kind (name kind) :repaired true}))

          ;; UNCHANGED: same etag, has duration. Skip entirely.
          :unchanged
          (do (log/debug "upsert-item!: UNCHANGED - skip"
                         {:jf-id jf-id :kind kind
                          :etag-match (:remote-etag existing)})
              nil))))))

;; ---------------------------------------------------------------------------
;; Library discovery
;; ---------------------------------------------------------------------------

(defn- jf-collection-type->kind
  "Maps a Jellyfin CollectionType string to a pseudovision library_kind string."
  [collection-type]
  (case (some-> collection-type str/lower-case)
    "movies"       "movies"
    "tvshows"      "shows"
    "music"        "songs"
    "musicvideos"  "music_videos"
    "homevideos"   "other_videos"
    "photos"       "images"
    "books"        "other_videos"
    "other_videos"))

(defn discover-libraries!
  "Connects to a Jellyfin source and returns a seq of library attribute maps
   (`:name`, `:kind`, `:external-id`, `:should-sync`) ready to pass to
   `db/create-library!` after associng `:media-source-id`.
   Throws ex-info if the server is unreachable or misconfigured."
  [source]
  (let [source-id (or (:media-sources/id source) (:id source))
        {:keys [base-url api-key]} (conn/<-connection-config source)]
    (log/info "Attempting to discover Jellyfin libraries"
              {:source-id source-id
               :has-base-url (boolean base-url)
               :has-api-key (boolean api-key)
               :connection-config (or (:media-sources/connection-config source)
                                      (:connection-config source))})
    (when-not base-url
      (log/error "No active connection for Jellyfin source"
                 {:source-id source-id
                  :connection-config (or (:media-sources/connection-config source)
                                         (:connection-config source))})
      (throw (ex-info "No active connection for Jellyfin source"
                      {:source-id source-id})))
    (when-not api-key
      (log/error "No API key configured for Jellyfin source"
                 {:source-id source-id})
      (throw (ex-info "No API key configured for Jellyfin source"
                      {:source-id source-id})))
    (log/info "Discovering Jellyfin libraries" {:url base-url :source-id source-id})
    (let [folders (fetch-libraries base-url api-key)]
      (when-not folders
        (log/error "Failed to fetch libraries from Jellyfin"
                   {:url base-url :source-id source-id})
        (throw (ex-info "Failed to fetch libraries from Jellyfin" {:url base-url})))
      (log/info "Discovered Jellyfin libraries"
                {:count (count folders)
                 :libraries (mapv #(select-keys % [:Name :CollectionType :ItemId]) folders)
                 :source-id source-id})
      (mapv (fn [folder]
              {:name        (csk/->kebab-case (:Name folder))
               :kind        (jf-collection-type->kind (:CollectionType folder))
               :external-id (:ItemId folder)
               :should-sync true})
            folders))))

;; ---------------------------------------------------------------------------
;; Public: scan a Jellyfin library
;; ---------------------------------------------------------------------------

(defn scan-library!
  "Scans a single library from a Jellyfin media source and upserts all items.
   `source` must be a media_sources row with connection_config.
   `library` must have :external_id set to the Jellyfin library ID."
  [db source library]
  (let [{:keys [base-url api-key]} (conn/<-connection-config source)]
    (when-not base-url
      (throw (ex-info "No active connection for Jellyfin source"
                      {:source-id (:media-sources/id source)})))
    (when-not api-key
      (throw (ex-info "No API key configured for Jellyfin source"
                      {:source-id (:media-sources/id source)})))

    (log/info "Checking Jellyfin server connectivity" {:url base-url})
    (let [server-info (check-server base-url api-key)]
      (when-not server-info
        (throw (ex-info "Cannot connect to Jellyfin server" {:url base-url})))
      (log/info "Connected to Jellyfin" {:server-name (:ServerName server-info)
                                         :version     (:Version server-info)}))

    (let [jf-library-id  (or (:libraries/external-id library)
                             (:external-id library))
          library-id     (or (:libraries/id library) (:id library))
          ;; Use a synthetic path for the library_path row (Jellyfin has no local paths)
          synthetic-path (str base-url "/library/" jf-library-id)
          lp             (or (first (db/list-library-paths db library-id))
                             (db/create-library-path! db {:library-id library-id
                                                          :path       synthetic-path}))
          lp-id          (or (:library-paths/id lp) (:id lp))]

      (log/info "Fetching items from Jellyfin library"
                {:library-id jf-library-id :library-name (:libraries/name library)})

      (let [items  (fetch-all-items base-url api-key jf-library-id)
            ;; Sort so parents (shows) come before children (seasons → episodes)
            sorted (sort-by (fn [item]
                              (case (:Type item)
                                "Series"     0
                                "Season"     1
                                "Episode"    2
                                "Movie"      0
                                "MusicVideo" 1
                                "Audio"      0
                                "Photo"      0
                                3))
                            items)]
        (log/info "Found items to sync" {:count (count items)})

        ;; Pre-fetch every existing item in this library_path so the per-item
        ;; upsert can tier itself: NEW (no row) → full, CHANGED (etag differs)
        ;; → full, UNCHANGED → skip, REPAIR-DURATION → version-only.
        ;; This is what turns the nightly scan from 4-5 hours into seconds:
        ;; the cost is one SELECT for the whole library (~200ms) plus per-item
        ;; work that, on the steady state, is just a hashmap lookup.
        (let [etag-state (db/list-library-etag-state db lp-id)
              results    (atom {:synced 0 :skipped 0 :repaired 0 :errors 0})]
          (log/info "Pre-fetched library etag state"
                    {:library-path-id lp-id
                     :items-in-db     (count etag-state)})
          (doseq [item sorted]
            (try
              (let [result (upsert-item! db lp-id item etag-state)]
                (cond
                  (:repaired result) (swap! results update :repaired inc)
                  result              (swap! results update :synced inc)
                  :else               (swap! results update :skipped inc)))
              (catch Exception e
                (log/error e "Failed to upsert Jellyfin item"
                           {:id (:Id item) :name (:Name item)})
                (swap! results update :errors inc))))
          (log/info "Jellyfin library scan complete"
                    (assoc @results :total (count sorted)))
          @results)))))
