(ns pseudovision.media.grout-source
  "Bridges Grout clips into Pseudovision's media model.

   Grout clips live on a filesystem shared with Pseudovision, so once a clip is
   ingested as an ordinary local-path `media_item` the existing scheduler
   (packer, enumerators, event emission) and the live streamer can reference it
   by `media_item_id` with no special-casing.

   Ingest is idempotent: each clip is keyed by `grout:<id>` in `remote_key`
   under a dedicated \"Grout\" local media source, so re-ingesting the same clip
   is a no-op that returns the existing item.  Grout clips are content-addressed
   and immutable, so an already-ingested clip is never re-probed or rewritten."
  (:require [camel-snake-kebab.core :as csk]
            [clojure.string         :as str]
            [next.jdbc              :as jdbc]
            [honey.sql              :as sql]
            [honey.sql.helpers      :as h]
            [pseudovision.db.core   :as db]
            [pseudovision.media.grout :as grout]
            [pseudovision.util.sql  :as sql-util]
            [pseudovision.util.tags :as tags]
            [taoensso.timbre        :as log])
  (:import [java.security MessageDigest]
           [java.time Duration]
           [java.util HexFormat]))

(def ^:private source-name "Grout")
(def ^:private library-name "grout-filler")
(def ^:private content-library-name "grout-content")
(def ^:private default-media-dir "/data/media/grout")

;; ---------------------------------------------------------------------------
;; Shared Grout library (media source → library → library path)
;; ---------------------------------------------------------------------------

(defn- find-or-create!
  "Returns the id of an existing row matching `where`, creating it from `attrs`
   (with RETURNING) if absent. `id-key` is accepted for documentation only —
   the returned id is always read with the unqualified `:id` key, because
   `db/query-one` and `db/execute-one!` use different result-set builders
   (`as-kebab-maps*` qualified vs `as-unqualified-kebab-maps*`) and the
   caller-supplied qualified key only matches the lookup branch. Reading
   `:id` from both branches keeps the insert path from returning nil when
   it shouldn't."
  [ds table id-key where attrs]
  (or (:id (db/query-one ds (-> (h/select :id) (h/from table) (h/where where) sql/format)))
      (:id (db/execute-one! ds (-> (h/insert-into table)
                                    (h/values [attrs])
                                    (h/returning :*)
                                    sql/format)))))

(defn- ensure-library-path*!
  "Ensures the singleton Grout media source and the named library / library-path
   exist, rooted at `media-dir`. Returns the library_path id. Filler and content
   share the one \"Grout\" media source but live under distinct libraries
   (`grout-filler` vs `grout-content`) so the two pools stay separable."
  [ds media-dir library]
  (let [root       (or media-dir default-media-dir)
        source-id  (find-or-create! ds :media-sources :media-sources/id
                                     [:= :name source-name]
                                     {:name source-name
                                      :kind (sql-util/->pg-enum "media_source_kind" "local")})
        library-id (find-or-create! ds :libraries :libraries/id
                                     [:and [:= :media-source-id source-id]
                                           [:= :name library]]
                                     {:media-source-id source-id
                                      :name            library
                                      :kind            (sql-util/->pg-enum "library_kind" "other_videos")
                                      :should-sync     false})]
    (find-or-create! ds :library-paths :library-paths/id
                     [:and [:= :library-id library-id] [:= :path root]]
                     {:library-id library-id :path root})))

(defn ensure-library-path!
  "Ensures the Grout **filler** library-path exists, rooted at `media-dir`.
   Returns the library_path id used for all Grout filler clips."
  [ds media-dir]
  (ensure-library-path*! ds media-dir library-name))

(defn ensure-content-library-path!
  "Ensures the Grout **content** library-path exists, rooted at `media-dir`.
   Returns the library_path id used for all Grout `program` items."
  [ds media-dir]
  (ensure-library-path*! ds media-dir content-library-name))

;; ---------------------------------------------------------------------------
;; Clip ingest
;; ---------------------------------------------------------------------------

(defn- path-hash
  "SHA-256 hex of a path string (matches the media scanner's dedup key)."
  [^String path]
  (.formatHex (HexFormat/of)
              (.digest (MessageDigest/getInstance "SHA-256") (.getBytes path "UTF-8"))))

(defn- remote-key [clip] (str "grout:" (:id clip)))

(defn- existing-item
  "Returns {:media-items/id .. :media-versions/duration ..} for an
   already-ingested clip, or nil."
  [ds lib-path-id clip]
  (db/query-one ds (-> (h/select :mi.id [:mv.duration :duration])
                       (h/from [:media-items :mi])
                       (h/left-join [:media-versions :mv] [:= :mv.media-item-id :mi.id])
                       (h/where [:and
                                 [:= :mi.library-path-id lib-path-id]
                                 [:= :mi.remote-key (remote-key clip)]])
                       sql/format)))

(defn ingest-clip!
  "Idempotently materialises one Grout clip as a local-path media item under the
   Grout library path. Returns a candidate map shaped for the filler packer
   ({:media-items/id .. :media-versions/duration <Duration>}), or nil if the
   clip is unusable (no path or non-positive duration)."
  [ds lib-path-id clip]
  (let [path (:path clip)
        ms   (:duration-ms clip)]
    (cond
      (str/blank? path)
      (do (log/warn "Skipping Grout clip with no path" {:id (:id clip)}) nil)

      (or (nil? ms) (not (pos? ms)))
      (do (log/warn "Skipping Grout clip with non-positive duration"
                    {:id (:id clip) :duration-ms ms})
          nil)

      :else
      (if-let [found (existing-item ds lib-path-id clip)]
        {:media-items/id        (:media-items/id found)
         :media-versions/duration (or (:duration found) (Duration/ofMillis ms))}
        (jdbc/with-transaction [tx ds]
          (let [item    (db/execute-one! tx
                          (-> (h/insert-into :media-items)
                              (h/values [{:kind            (sql-util/->pg-enum "media_item_kind" "other_video")
                                          :state           (sql-util/->pg-enum "media_item_state" "normal")
                                          :library-path-id lib-path-id
                                          :remote-key      (remote-key clip)}])
                              (h/returning :*)
                              sql/format))
                ;; Both rows come from `db/execute-one!`, which uses
                ;; `rs/as-unqualified-kebab-maps` — so the returned keys are
                ;; `:id` and `:id` (not `:media-items/id` / `:media-versions/id`).
                ;; The previous qualified lookups silently returned nil and
                ;; the `media-versions` insert below violated the NOT NULL
                ;; constraint on `media_item_id`, rolling back the whole Grout
                ;; clip ingest. With these, both inserts succeed and Grout
                ;; clips can flow into `media_files` like Jellyfin items.
                item-id (:id item)
                ver     (db/execute-one! tx
                          (-> (h/insert-into :media-versions)
                              (h/values [{:media-item-id item-id
                                          :duration (-> (Duration/ofMillis ms)
                                                        sql-util/duration->pg-interval
                                                        sql-util/->pg-interval)
                                          :width  (or (:width clip) 0)
                                          :height (or (:height clip) 0)}])
                              (h/returning :*)
                              sql/format))]
            (db/execute-one! tx
              (-> (h/insert-into :media-files)
                  (h/values [{:media-version-id (:id ver)
                              :path             path
                              :path-hash        (path-hash path)}])
                  (h/on-conflict :path-hash)
                  (h/do-update-set :media-version-id :path)
                  sql/format))
            (log/info "Ingested Grout clip" {:grout-id (:id clip) :media-item-id item-id :path path})
            ;; Return the same shape callers expect — `item-id` is the local
            ;; unqualified id (the only kind this function actually has), and
            ;; `duration` is the resolved Java Duration.
            {:media-items/id item-id
             :media-versions/duration (Duration/ofMillis ms)}))))))

;; ---------------------------------------------------------------------------
;; Filler candidate loading (called by the scheduler)
;; ---------------------------------------------------------------------------

(defn- channel-tag
  "Derives Grout's `channel` tag from a Pseudovision channel's name
   (kebab-cased, e.g. \"Britannia\" -> \"britannia\"). Returns nil for a generic
   / channel-less fill so Grout returns null-channel (generic) filler."
  [channel]
  (let [nm (:channels/name channel)]
    (when-not (str/blank? nm) (csk/->kebab-case nm))))

(defn grout-filler-items
  "Queries Grout for filler candidates that fit the gap [from, to] on `channel`,
   ingests each as a local-path media item, and returns the candidate vector for
   the filler packer. Returns [] when Grout is disabled/unreachable or has no
   match — the caller then falls back to an empty fill (a gap), exactly as with
   an empty local collection.

   `grout` is the client, `preset` supplies :filler-presets/grout-tags, and the
   duration window is derived from the gap so oversized clips aren't fetched."
  [ds grout channel from to preset]
  (if-not (grout/enabled? grout)
    []
    (let [gap-ms (when (and from to) (max 0 (.toMillis (Duration/between from to))))
          clips  (->> (grout/find-filler grout
                                         (cond-> {:tags   (:filler-presets/grout-tags preset)
                                                  :random true}
                                           (channel-tag channel) (assoc :channel (channel-tag channel))
                                           (and gap-ms (pos? gap-ms)) (assoc :max-ms gap-ms)))
                      ;; Bumpers are reserved for the dedicated small-gap
                      ;; injector (see grout-bumper-items / fill-gap-with-bumper)
                      ;; so they keep their "Coming up next" overlay treatment —
                      ;; never surface them here as plain tail/fallback/pre/mid/
                      ;; post filler.
                      (remove #(= "bumper" (:kind %))))]
      (if (empty? clips)
        (do (log/info "Grout returned no filler candidates"
                      {:channel (channel-tag channel)
                       :tags    (:filler-presets/grout-tags preset)
                       :gap-ms  gap-ms})
            [])
        (let [lib-path-id (ensure-library-path! ds (:media-dir grout))]
          (into [] (keep #(ingest-clip! ds lib-path-id %)) clips))))))

(defn grout-bumper-items
  "Queries Grout for **bumper**-kind clips on `channel` whose duration falls
   within [min-ms, max-ms], ingests each as a local-path media item, and
   returns the candidate vector for the small-gap bumper injector
   (`pseudovision.scheduling.filler/fill-gap-with-bumper`) to pick from.

   This is the bumper counterpart to `grout-filler-items` above: same
   ingest-and-return-candidates shape, but scoped to :kind \"bumper\" and a
   tight duration window (a bucket ± tolerance) instead of an open-ended gap
   ceiling, since bumpers fill a fixed-size slot rather than an arbitrary gap.

   Returns [] when Grout is disabled/unreachable or has no match — the caller
   then falls back to regular filler, exactly as with an empty local
   collection."
  [ds grout channel min-ms max-ms]
  (if-not (grout/enabled? grout)
    []
    (let [clips (grout/find-filler grout
                                   (cond-> {:kind    "bumper"
                                            :random  true
                                            :min-ms  min-ms
                                            :max-ms  max-ms}
                                     (channel-tag channel) (assoc :channel (channel-tag channel))))]
      (if (empty? clips)
        (do (log/info "Grout returned no bumper candidates"
                      {:channel (channel-tag channel) :min-ms min-ms :max-ms max-ms})
            [])
        (let [lib-path-id (ensure-library-path! ds (:media-dir grout))]
          (into [] (keep #(ingest-clip! ds lib-path-id %)) clips))))))

;; ---------------------------------------------------------------------------
;; Long-form content sync (program items → catalog + scheduler)
;;
;; Unlike filler (ingested lazily at build time as bare media items that only
;; need to stream + pack), long-form *content* must be visible to the catalog
;; aggregate and the daily-slot resolver BEFORE a schedule is built. So content
;; is pulled by an explicit sync that materialises each Grout `program` item as
;; a `program`-kind media item WITH metadata + metadata_tags — the shape the
;; catalog and scheduler read. Once ingested, the existing streamer resolves it
;; by path off the shared mount with no special-casing, exactly like filler.
;; ---------------------------------------------------------------------------

(def ^:private grout-internal-tag-prefixes
  "Tag prefixes from the Grout intake/audit trail that should NOT be exposed as
   PV metadata_tags. `parent-directory:` and `filename:` leak the on-disk
   intake layout (`parent-directory:food-shorts`, `filename:2025-09-26 ...`),
   `content-type:` is an audit tag describing the intake kind (`filler`,
   `program`) and is meaningless to the catalog/scheduler. Any new prefix added
   here will be silently dropped on the next sync — Grout owns these, PV
   doesn't need them in metadata_tags."
  #{"parent-directory:" "filename:" "content-type:"})

(defn- grout-internal-tag?
  "True if a Grout tag is an intake/audit marker that should not be mirrored
   into PV's metadata_tags. See `grout-internal-tag-prefixes`."
  [t]
  (boolean (and (string? t)
                (some #(str/starts-with? t %) grout-internal-tag-prefixes))))

(defn- grout-filename-tag-value
  "Returns the value of the first `filename:` tag on a Grout clip, or nil if no
   such tag is present. `filename:` is one of Grout's intake/audit tags and is
   filtered out of `metadata_tags` (see `grout-internal-tag?`), but we still
   want it for display — see `display-title`."
  [clip]
  (some (fn [t]
          (when (and (string? t) (str/starts-with? t "filename:"))
            (not-empty (str/trim (subs t 9)))))
        (:tags clip)))

(defn- grout-parent-directory-tag-value
  "Returns the value of the first `parent-directory:` tag on a Grout clip, or
   nil if no such tag is present. See `grout-filename-tag-value` for context."
  [clip]
  (some (fn [t]
          (when (and (string? t) (str/starts-with? t "parent-directory:"))
            (not-empty (str/trim (subs t 17)))))
        (:tags clip)))

(defn- strip-extension
  "Strip the trailing file extension (after the last `.`) from a filename. No-op
   if there is no `.` or the trailing segment is empty. Trims surrounding
   whitespace from the result so internal-padding whitespace before the
   extension (e.g. `clip   .mp4`) doesn't survive into the display title."
  [filename]
  (when-let [dot-idx (.lastIndexOf ^String filename ".")]
    (when (pos? dot-idx)
      (str/trim (subs filename 0 dot-idx)))))

(defn display-title
  "Best-available display title for a Grout clip, in order of preference:

   1. Grout's `name` field — human/AI-set (e.g. bumpers generated via Tunabrain)
   2. Original upload filename (stripped of extension), extracted from the
      `filename:` intake/audit tag
   3. `<parent-directory>:<filename-stem>` if both are available, since bare
      filenames like `2024-12-31 ...mp4` are ambiguous without their directory
   4. Channel-aware placeholder: \"Untitled <channel> program\"
   5. The Grout clip id
   6. The literal string \"Unknown\"

   Without this fallback chain, the PV API exposes `name: null` for bulk-uploaded
   Grout items, and any UI that renders the title falls back to a numeric
   \"Media Item #<id>\" placeholder — opaque and unsearchable. The first 3
   options are real, useful, human-meaningful identifiers that already exist on
   the Grout side; the rest are progressively more opaque."
  [clip]
  (or (not-empty (:name clip))
      (let [filename (grout-filename-tag-value clip)]
        (when filename
          (let [stem      (or (not-empty (strip-extension filename)) filename)
                parent    (grout-parent-directory-tag-value clip)]
            (if parent
              (str parent ":" stem)
              stem))))
      (when-let [chan (:channel clip)]
        (when-not (str/blank? chan)
          (str "Untitled " chan " program")))
      (when-let [id (:id clip)] (str "Unknown Grout item (" id ")"))
      "Unknown"))

(defn- program-tags
  "metadata_tags names for a Grout program: its freeform Grout tags (with
   Grout-internal intake/audit tags filtered out — see
   `grout-internal-tag?`), plus a synthesized `channel:<slug>` tag from the
   clip's `channel` column so the item lands in the right channel's catalog
   slice. Tags are passed through verbatim otherwise (an already-`genre:`-
   prefixed tag flows straight into the catalog's genre aggregate; a bare tag
   is still matchable by `random:<tag>`). Deduped; blanks dropped."
  [clip]
  (let [base     (->> (:tags clip)
                      (filter string?)
                      (map str/trim)
                      (remove str/blank?)
                      (remove grout-internal-tag?))
        chan     (:channel clip)
        chan-tag (when-not (str/blank? chan) (str "channel:" (tags/kebab-case chan)))]
    (->> (cond-> (vec base) chan-tag (conj chan-tag))
         distinct
         vec)))

(defn- upsert-program-metadata!
  "Writes/refreshes the `metadata` row (title/plot) and fully replaces the
   `metadata_tags` for a program media item from its Grout clip. Grout owns
   content metadata, so each sync makes PV's copy match Grout — including tag
   removals.

   `title` is computed via `display-title`: Grout's `name` field when present
   (e.g. bumpers generated via Tunabrain), otherwise derived from the
   `filename:` and `parent-directory:` intake/audit tags so unnamed bulk
   uploads still get a useful display title in the API rather than rendering
   as \"Media Item #<n>\" in Marquee."
  [tx item-id clip]
  (db/execute-one! tx
    (-> (h/insert-into :metadata)
        (h/values [{:media-item-id item-id
                    :kind  (sql-util/->pg-enum "media_item_kind" "program")
                    :title (display-title clip)
                    :plot  (not-empty (:description clip))}])
        (h/on-conflict :media-item-id)
        (h/do-update-set :title :plot :date-updated)
        sql/format))
  (let [meta-id (:metadata/id (db/query-one tx (-> (h/select :id)
                                                   (h/from :metadata)
                                                   (h/where [:= :media-item-id item-id])
                                                   sql/format)))
        tag-names (program-tags clip)]
    (when meta-id
      (db/execute-one! tx (-> (h/delete-from :metadata-tags)
                              (h/where [:= :metadata-id meta-id])
                              sql/format))
      (when (seq tag-names)
        (jdbc/execute! tx (-> (h/insert-into :metadata-tags)
                              (h/values (mapv (fn [t] {:metadata-id meta-id :name t}) tag-names))
                              sql/format))))))

(defn sync-program!
  "Idempotently materialises one Grout `program` clip as a local-path media item
   (kind `program`) with metadata + metadata_tags, keyed by `grout:<id>` under
   the Grout content library. On re-sync of an already-ingested clip the
   immutable file/version are left untouched and only metadata + tags are
   refreshed (Grout is the source of truth). Returns :synced (new), :updated
   (existing, metadata refreshed), or :skipped (unusable clip)."
  [ds lib-path-id clip]
  (let [path (:path clip)
        ms   (:duration-ms clip)]
    (cond
      (str/blank? path)
      (do (log/warn "Skipping Grout program with no path" {:id (:id clip)}) :skipped)

      (or (nil? ms) (not (pos? ms)))
      (do (log/warn "Skipping Grout program with non-positive duration"
                    {:id (:id clip) :duration-ms ms})
          :skipped)

      :else
      (jdbc/with-transaction [tx ds]
        (let [existing (existing-item tx lib-path-id clip)
              item-id  (or (:media-items/id existing)
                           (let [item (db/execute-one! tx
                                        (-> (h/insert-into :media-items)
                                            (h/values [{:kind            (sql-util/->pg-enum "media_item_kind" "program")
                                                        :state           (sql-util/->pg-enum "media_item_state" "normal")
                                                        :library-path-id lib-path-id
                                                        :remote-key      (remote-key clip)}])
                                            (h/returning :*)
                                            sql/format))
                                 ver  (db/execute-one! tx
                                        (-> (h/insert-into :media-versions)
                                            (h/values [{:media-item-id (:id item)
                                                        :duration (-> (Duration/ofMillis ms)
                                                                      sql-util/duration->pg-interval
                                                                      sql-util/->pg-interval)
                                                        :width  (or (:width clip) 0)
                                                        :height (or (:height clip) 0)}])
                                            (h/returning :*)
                                            sql/format))]
                             (db/execute-one! tx
                               (-> (h/insert-into :media-files)
                                   (h/values [{:media-version-id (:id ver)
                                               :path             path
                                               :path-hash        (path-hash path)}])
                                   (h/on-conflict :path-hash)
                                   (h/do-update-set :media-version-id :path)
                                   sql/format))
                             (:id item)))]
          (upsert-program-metadata! tx item-id clip)
          (if existing :updated :synced))))))

(defn sync-programs!
  "Pulls every `program`-kind item from Grout and materialises it into PV's
   catalog (see sync-program!). Best-effort and safe to re-run: no-op when Grout
   is disabled/unreachable. Returns a summary map
   {:enabled :total :synced :updated :skipped :errors}."
  [ds grout]
  (if-not (grout/enabled? grout)
    {:enabled false :total 0 :synced 0 :updated 0 :skipped 0 :errors 0}
    (let [clips       (grout/list-programs grout)
          lib-path-id (when (seq clips) (ensure-content-library-path! ds (:media-dir grout)))
          result      (reduce
                        (fn [acc clip]
                          (let [outcome (try (sync-program! ds lib-path-id clip)
                                             (catch Exception e
                                               (log/warn e "Failed to sync Grout program"
                                                         {:id (:id clip)})
                                               :error))]
                            (update acc (case outcome
                                          :synced  :synced
                                          :updated :updated
                                          :skipped :skipped
                                          :error   :errors) inc)))
                        {:enabled true :total (count clips)
                         :synced 0 :updated 0 :skipped 0 :errors 0}
                        clips)]
      (log/info "Grout content sync complete" result)
      result)))
