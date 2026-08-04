(ns pseudovision.http.api.media
  (:require [clojure.string                 :as str]
            [clj-http.client                :as http]
            [pseudovision.db.media          :as db]
            [pseudovision.jobs.runner       :as runner]
            [pseudovision.media.scanner     :as scanner]
            [pseudovision.media.jellyfin    :as jellyfin]
            [pseudovision.media.connection  :as conn]
            [pseudovision.util.pagination   :as pagination]
            [taoensso.timbre                :as log]))

(defn- unqualify-keys [m]
  (when m
    (reduce-kv (fn [acc k v] (assoc acc (keyword (name k)) v)) {} m)))

(defn- display-title-for-item
  "Best-available display title for a media item, in order of preference:

   1. `metadata.title` (the underlying column, aliased to `:title` after
      `unqualify-keys` strips the `m/` prefix). Set for Jellyfin items and
      for Grout program-kind items (see
      `pseudovision.media.grout-source/display-title`); null for Grout
      filler / bumper items that don't get a metadata row.
   2. `remote-key` — at least something non-numeric and discoverable
      (e.g. `grout:5c707253...` instead of the numeric PV id).
   3. The literal string \"Unknown\".

   Without this fallback chain, every Grout bulk-uploaded filler item renders
   as the opaque \"Media Item #<n>\" placeholder from
   marquee/pages/media_detail.cljs:148."
  [item]
  (or (not-empty (:title item))
      (not-empty (:remote-key item))
      "Unknown"))

;; ---------------------------------------------------------------------------
;; Playback URL helpers
;; ---------------------------------------------------------------------------

(defmulti ^:private build-stream-url
  "Builds a direct-play stream URL for a media item given its source kind.
   Dispatches on the source kind string."
  (fn [_item _conn-config kind] kind))

(defmethod build-stream-url "jellyfin" [item conn-config _kind]
  (let [base-url (conn/active-uri (:connections conn-config))
        api-key  (:api-key conn-config)
        item-id  (or (:media-items/remote-key item) (:remote-key item))]
    (when (and base-url api-key item-id)
      (str base-url "/Videos/" item-id "/stream?static=true&api_key=" api-key))))

(defmethod build-stream-url :default [_item _conn-config kind]
  (log/warn "Playback URL not supported for source kind" {:kind kind})
  nil)

(defn list-sources-handler [{:keys [db]}]
  (fn [req]
    (let [qp     (get-in req [:parameters :query])
          limit  (or (:limit qp) 100)
          offset (or (:offset qp) 0)
          total  (db/count-media-sources db)
          items  (mapv unqualify-keys (db/list-media-sources db {:limit limit :offset offset}))]
      {:status 200
       :body (pagination/offset-pagination-response items limit offset total)})))

(defn create-source-handler [{:keys [db]}]
  (fn [req]
    (let [params (get-in req [:parameters :body])
          _      (log/info "create-source-handler received params" {:params params})
          config (conn/->connection-config params)
          _      (log/info "connection config" {:config config})
          attrs  (-> (select-keys params [:name :kind :path-replacements])
                     (cond-> config (assoc :connection-config config)))]
      (log/info "final attrs" {:attrs attrs})
      {:status 201 :body (db/create-media-source! db attrs)})))

(defn update-source-handler [{:keys [db]}]
  (fn [req]
    (let [source-id (get-in req [:parameters :path :id])
          params    (get-in req [:parameters :body])
          attrs     (select-keys params [:name :path-replacements])]
      (if-let [s (db/update-media-source! db source-id attrs)]
        {:status 200 :body (unqualify-keys s)}
        {:status 404 :body {:error "Media source not found"}}))))

(defn delete-source-handler [{:keys [db]}]
  (fn [req]
    (let [source-id (get-in req [:parameters :path :id])]
      (db/delete-media-source! db source-id)
      {:status 204 :body nil})))

(defn list-all-libraries-handler [{:keys [db]}]
  (fn [req]
    (let [qp     (get-in req [:parameters :query])
          limit  (or (:limit qp) 100)
          offset (or (:offset qp) 0)
          total  (db/count-libraries db)
          items  (mapv unqualify-keys (db/list-libraries db {:limit limit :offset offset}))]
      {:status 200
       :body (pagination/offset-pagination-response items limit offset total)})))

(defn list-libraries-handler [{:keys [db]}]
  (fn [req]
    (let [source-id (get-in req [:parameters :path :id])]
      {:status 200 :body (mapv unqualify-keys (db/list-libraries-for-source db source-id))})))

(defn create-library-handler [{:keys [db]}]
  (fn [req]
    (let [source-id (get-in req [:parameters :path :id])
          params    (get-in req [:parameters :body])
          attrs     (assoc params :media-source-id source-id)]
      {:status 201 :body (db/create-library! db attrs)})))

(defn update-library-handler [{:keys [db]}]
  (fn [req]
    (let [library-id (get-in req [:parameters :path :id])
          attrs      (get-in req [:parameters :body])]
      (if-let [lib (db/update-library! db library-id attrs)]
        {:status 200 :body (unqualify-keys lib)}
        {:status 404 :body {:error "Library not found"}}))))

(defn delete-library-handler [{:keys [db]}]
  (fn [req]
    (let [library-id (get-in req [:parameters :path :id])]
      (db/delete-library! db library-id)
      {:status 204 :body nil})))

(defn list-library-paths-handler [{:keys [db]}]
  (fn [req]
    (let [library-id (get-in req [:parameters :path :id])]
      {:status 200 :body (mapv unqualify-keys (db/list-library-paths db library-id))})))

(defn create-library-path-handler [{:keys [db]}]
  (fn [req]
    (let [library-id (get-in req [:parameters :path :id])
          path       (get-in req [:parameters :body :path])
          attrs      {:library-id library-id :path path}]
      {:status 201 :body (unqualify-keys (db/create-library-path! db attrs))})))

(defn delete-library-path-handler [{:keys [db]}]
  (fn [req]
    (let [path-id (get-in req [:parameters :path :path-id])]
      (db/delete-library-path! db path-id)
      {:status 204 :body nil})))

(defn- parse-attrs
  "Splits a comma-separated attrs string into a seq of trimmed strings, or
   returns nil if the input is blank/nil."
  [s]
  (when (not (str/blank? s))
    (->> (str/split s #",")
         (map str/trim)
         (remove str/blank?)
         seq)))

(defn list-library-items-handler [{:keys [db]}]
  (fn [req]
    (let [library-id (get-in req [:parameters :path :id])
          qp         (get-in req [:parameters :query])
          attrs      (parse-attrs (:attrs qp))
          item-type  (not-empty (:type qp))
          search     (not-empty (some-> (:search qp) str/trim))
          limit      (or (:limit qp) 50)
          offset     (or (:offset qp) 0)
          opts       (cond-> {:limit limit :offset offset}
                       attrs                       (assoc :attrs attrs)
                       item-type                   (assoc :type item-type)
                       search                      (assoc :search search)
                       (contains? qp :parent-id)   (assoc :parent-id (:parent-id qp)))]
      (log/info "list-library-items handler called" {:library-id library-id :attrs attrs :opts opts})
      (let [total  (db/count-media-items db library-id opts)
            items  (mapv unqualify-keys (db/list-media-items db library-id opts))]
        (log/info "list-library-items result" {:count (count items) :total total :limit limit :offset offset})
        {:status 200
         :body (pagination/offset-pagination-response items limit offset total)}))))

(defn trigger-scan-handler [{:keys [db media ffmpeg jobs]}]
  (fn [req]
    (let [library-id (get-in req [:parameters :path :id])
          library    (db/get-library db library-id)]
      (if library
        (let [source (db/get-media-source db (:libraries/media-source-id library))
              kind   (keyword (:media-sources/kind source))
              job    (runner/submit!
                      jobs
                      {:type :media/library-scan
                       :metadata {:library-id library-id :kind kind}}
                      (fn [_report-progress]
                        (log/info "Starting library scan" {:library-id library-id :kind kind})
                        (case kind
                          :jellyfin (jellyfin/scan-library! db source library)
                          (scanner/scan-library! db media ffmpeg library))
                        (log/info "Library scan completed" {:library-id library-id :kind kind})
                        {:library-id library-id
                         :library    (:libraries/name library)
                         :kind       (name kind)
                         :status     "ok"}))]
          {:status 202 :body {:job job}})
        {:status 404 :body {:error "Library not found"}}))))

(defn scan-all-handler
  "POST /api/media/scan-all — scan every Jellyfin-backed library across all
   sources, as a single async job. This is the nightly catalog-ingest entry
   point: one call instead of a per-library fan-out. Non-Jellyfin libraries
   (e.g. the local `grout-content` library, which is populated by the Grout
   content sync rather than a filesystem scan) are skipped. Returns 202 with
   the job; poll GET /api/jobs/:job-id for the per-library summary in :result."
  [{:keys [db media ffmpeg jobs]}]
  (fn [_req]
    (let [job (runner/submit!
                jobs
                {:type :media/scan-all}
                (fn [_report-progress]
                  (let [libraries (db/list-libraries db {:limit 10000 :offset 0})
                        results
                        (mapv
                         (fn [library]
                           (let [source (db/get-media-source db (:libraries/media-source-id library))
                                 kind   (keyword (:media-sources/kind source))]
                             (if (not= kind :jellyfin)
                               {:library-id (:libraries/id library)
                                :library    (:libraries/name library)
                                :kind       (name kind)
                                :status     "skipped"}
                               (try
                                 (jellyfin/scan-library! db source library)
                                 {:library-id (:libraries/id library)
                                  :library    (:libraries/name library)
                                  :kind       (name kind)
                                  :status     "ok"}
                                 (catch Exception e
                                   (log/error e "scan-all: library scan failed"
                                              {:library-id (:libraries/id library)})
                                   {:library-id (:libraries/id library)
                                    :library    (:libraries/name library)
                                    :kind       (name kind)
                                    :status     "error"
                                    :error      (.getMessage e)})))))
                         libraries)]
                    {:libraries (count results)
                     :scanned   (count (filter #(= "ok" (:status %)) results))
                     :skipped   (count (filter #(= "skipped" (:status %)) results))
                     :failed    (count (filter #(= "error" (:status %)) results))
                     :results   results})))]
      {:status 202 :body {:job job}})))

(defn discover-libraries-handler [{:keys [db]}]
  (fn [req]
    (let [source-id (get-in req [:parameters :path :id])
          source    (db/get-media-source db source-id)]
      (if (nil? source)
        {:status 404 :body {:error "Media source not found"}}
        (let [kind (keyword (str (:media-sources/kind source)))]
          (if (not= kind :jellyfin)
            {:status 400 :body {:error "Library discovery is only supported for Jellyfin sources"}}
            (let [candidates   (jellyfin/discover-libraries! source)
                  existing     (db/list-libraries-for-source db source-id)
                  existing-ids (set (keep :libraries/external-id existing))
                  new-libs     (remove #(existing-ids (:external-id %)) candidates)
                  created      (mapv #(db/create-library! db (assoc % :media-source-id source-id))
                                     new-libs)]
              {:status 201
               :body   {:discovered (count candidates)
                        :created    (count created)
                        :libraries  (into existing created)}})))))))

(defn list-collections-handler [{:keys [db]}]
  (fn [req]
    (let [qp     (get-in req [:parameters :query])
          limit  (or (:limit qp) 100)
          offset (or (:offset qp) 0)
          total  (db/count-collections db)
          items  (mapv unqualify-keys (db/list-collections db {:limit limit :offset offset}))]
      {:status 200
       :body (pagination/offset-pagination-response items limit offset total)})))

(defn create-collection-handler [{:keys [db]}]
  (fn [req]
    {:status 201 :body (unqualify-keys (db/create-collection! db (get-in req [:parameters :body])))}))

(defn get-collection-handler [{:keys [db]}]
  (fn [req]
    (let [id (get-in req [:parameters :path :id])]
      (if-let [c (db/get-collection db id)]
        {:status 200 :body (unqualify-keys c)}
        {:status 404 :body {:error "Collection not found"}}))))

(defn update-collection-handler [{:keys [db]}]
  (fn [req]
    (let [id    (get-in req [:parameters :path :id])
          attrs (get-in req [:parameters :body])]
      (if-let [c (db/update-collection! db id attrs)]
        {:status 200 :body (unqualify-keys c)}
        {:status 404 :body {:error "Collection not found"}}))))

(defn delete-collection-handler [{:keys [db]}]
  (fn [req]
    (db/delete-collection! db (get-in req [:parameters :path :id]))
    {:status 204 :body nil}))

(defn list-collection-items-handler [{:keys [db]}]
  (fn [req]
    (let [id (get-in req [:parameters :path :id])]
      {:status 200 :body (mapv unqualify-keys (db/list-items-in-collection db id))})))

(defn add-collection-item-handler [{:keys [db]}]
  (fn [req]
    (let [id      (get-in req [:parameters :path :id])
          item-ref (get-in req [:parameters :body :media-item-id])]
      (if-let [item-id (db/resolve-media-item-id db item-ref)]
        (do (db/add-item-to-collection! db id item-id)
            {:status 204 :body nil})
        {:status 404 :body {:error "Media item not found"}}))))

(defn remove-collection-item-handler [{:keys [db]}]
  (fn [req]
    (let [id       (get-in req [:parameters :path :id])
          item-ref (get-in req [:parameters :path :item-id])]
      (when-let [item-id (db/resolve-media-item-id db item-ref)]
        (db/remove-item-from-collection! db id item-id))
      {:status 204 :body nil})))

;; ---------------------------------------------------------------------------
;; Single item + playback URL
;; ---------------------------------------------------------------------------

(defn get-media-item-handler [{:keys [db]}]
  (fn [req]
    (let [item-id   (get-in req [:parameters :path :id])
          item      (db/get-media-item db item-id)
          qualified (when item (unqualify-keys item))]
      (if item
        {:status 200 :body (assoc qualified :name (display-title-for-item qualified))}
        {:status 404 :body {:error "Media item not found"}}))))

(defn get-media-item-children-handler [{:keys [db]}]
  (fn [req]
    (let [item-id (get-in req [:parameters :path :id])
          qp      (get-in req [:parameters :query])
          limit   (min (or (:limit qp) 50) 1000)
          offset  (or (:offset qp) 0)
          search  (not-empty (some-> (:search qp) str/trim))
          opts    (cond-> {:limit limit :offset offset}
                    search (assoc :search search))]
      (if-let [item (db/get-media-item db item-id)]
        (let [total  (db/count-children db (:media-items/id item))
              items  (mapv unqualify-keys (db/list-children db (:media-items/id item) opts))]
          {:status 200
           :body (pagination/offset-pagination-response items limit offset total)})
        {:status 404 :body {:error "Media item not found"}}))))

(defn- resolve-stream-url [db item-id]
  (when-let [row (db/get-media-item-with-source db item-id)]
    (let [kind        (or (some-> row :media-sources/kind str) "")
          conn-config (or (:media-sources/connection-config row)
                          (:connection-config row))]
      {:url  (build-stream-url row conn-config kind)
       :kind kind})))

(defn get-item-playback-url-handler [{:keys [db]}]
  (fn [req]
    (let [item-id (get-in req [:parameters :path :id])
          result  (resolve-stream-url db item-id)]
      (cond
        (nil? result)        {:status 404 :body {:error "Media item not found"}}
        (nil? (:url result)) {:status 422 :body {:error (str "Playback URL not supported for source kind: " (:kind result))}}
        :else                {:status 200 :body result}))))

(defmulti ^:private stream-media
  "Streams media content for a given source kind. Returns a Ring response map
   with the proxied stream as the body."
  (fn [_item _conn-config kind _req] kind))

(defmethod stream-media "jellyfin" [item conn-config _kind req]
  (let [base-url (conn/active-uri (:connections conn-config))
        api-key  (:api-key conn-config)
        item-id  (or (:media-items/remote-key item) (:remote-key item))]
    (when (and base-url api-key item-id)
      (let [stream-url (str base-url "/Videos/" item-id "/stream?static=true&api_key=" api-key)]
        (log/info "Proxying stream from Jellyfin" {:url stream-url :item-id item-id})
        (try
          (let [response (http/get stream-url
                                   {:as :stream
                                    :throw-exceptions false
                                    :headers (select-keys (:headers req) ["range" "Range"])})
                status (:status response)]
            (if (<= 200 status 299)
              {:status status
               :headers (select-keys (:headers response)
                                     ["content-type" "content-length" "accept-ranges"
                                      "content-range" "Content-Type" "Content-Length"
                                      "Accept-Ranges" "Content-Range"])
               :body (:body response)}
              (do
                (log/error "Failed to fetch stream from Jellyfin"
                           {:status status :url stream-url})
                {:status 502 :body {:error "Failed to fetch stream from upstream server"}})))
          (catch Exception e
            (log/error e "Exception while proxying stream" {:url stream-url})
            {:status 502 :body {:error "Failed to proxy stream"}}))))))

(defmethod stream-media :default [_item _conn-config kind _req]
  (log/warn "Stream proxying not supported for source kind" {:kind kind})
  {:status 422 :body {:error (str "Stream proxying not supported for source kind: " kind)}})

(defn redirect-to-stream-handler [{:keys [db]}]
  (fn [req]
    (let [item-id (get-in req [:parameters :path :id])]
      (if-let [row (db/get-media-item-with-source db item-id)]
        (let [kind        (or (some-> row :media-sources/kind str) "")
              conn-config (or (:media-sources/connection-config row)
                              (:connection-config row))]
          (if (str/blank? kind)
            {:status 404 :body {:error "Media item not found"}}
            (stream-media row conn-config kind req)))
        {:status 404 :body {:error "Media item not found"}}))))
