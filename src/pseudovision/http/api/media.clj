(ns pseudovision.http.api.media
  (:require [clojure.string                 :as str]
            [clj-http.client                :as http]
            [pseudovision.db.media          :as db]
            [pseudovision.media.scanner     :as scanner]
            [pseudovision.media.jellyfin    :as jellyfin]
            [pseudovision.media.connection  :as conn]
            [taoensso.timbre                :as log]))

(defn- unqualify-keys [m]
  (when m
    (reduce-kv (fn [acc k v] (assoc acc (keyword (name k)) v)) {} m)))

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
  (fn [_req] {:status 200 :body (mapv unqualify-keys (db/list-media-sources db))}))

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

(defn delete-source-handler [{:keys [db]}]
  (fn [req]
    (let [source-id (get-in req [:parameters :path :id])]
      (db/delete-media-source! db source-id)
      {:status 204 :body nil})))

(defn update-source-handler [{:keys [db]}]
  (fn [req]
    (let [source-id (get-in req [:parameters :path :id])
          params    (get-in req [:parameters :body])
          existing  (db/get-media-source db source-id)
          config    (when-let [new-config (conn/->connection-config params)]
                      (merge (:media-sources/connection-config existing) new-config))
          attrs     (-> (select-keys params [:name :path-replacements])
                        (cond-> config (assoc :connection-config config)))]
      (if (nil? existing)
        {:status 404 :body {:error "Media source not found"}}
        (if-let [s (db/update-media-source! db source-id attrs)]
          {:status 200 :body (unqualify-keys s)}
          {:status 404 :body {:error "Media source not found"}})))))

(defn list-all-libraries-handler [{:keys [db]}]
  (fn [_req] {:status 200 :body (mapv unqualify-keys (db/list-libraries db))}))

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

(defn delete-library-handler [{:keys [db]}]
  (fn [req]
    (let [library-id (get-in req [:parameters :path :id])]
      (db/delete-library! db library-id)
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
          opts       (cond-> {}
                       attrs                       (assoc :attrs attrs)
                       item-type                   (assoc :type item-type)
                       (contains? qp :parent-id)   (assoc :parent-id (:parent-id qp)))]
      (log/info "list-library-items handler called" {:library-id library-id :attrs attrs :opts opts})
      (let [result (mapv unqualify-keys (db/list-media-items db library-id opts))]
        (log/info "list-library-items result sample" {:count (count result) :first (first result)})
        {:status 200 :body result}))))

(defn trigger-scan-handler [{:keys [db media ffmpeg]}]
  (fn [req]
    (let [library-id (get-in req [:parameters :path :id])
          library    (db/get-library db library-id)]
      (if library
        (let [source (db/get-media-source db (:libraries/media-source-id library))
              kind   (keyword (:media-sources/kind source))]
          (log/info "Triggering library scan" {:library-id library-id
                                               :library-name (:libraries/name library)
                                               :kind kind})
          (future
            (try
              (log/info "Starting library scan" {:library-id library-id :kind kind})
              (case kind
                :jellyfin (jellyfin/scan-library! db source library)
                (scanner/scan-library! db media ffmpeg library))
              (log/info "Library scan completed" {:library-id library-id :kind kind})
              (catch Exception e
                (log/error e "Library scan failed" {:library-id library-id
                                                    :kind       kind}))))
          {:status 202 :body {:message "Scan triggered"}})
        {:status 404 :body {:error "Library not found"}}))))

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
  (fn [_req] {:status 200 :body (mapv unqualify-keys (db/list-collections db))}))

(defn create-collection-handler [{:keys [db]}]
  (fn [req]
    {:status 201 :body (db/create-collection! db (get-in req [:parameters :body]))}))

;; ---------------------------------------------------------------------------
;; Single item + playback URL
;; ---------------------------------------------------------------------------

(defn get-media-item-handler [{:keys [db]}]
  (fn [req]
    (let [item-id (get-in req [:parameters :path :id])
          item    (db/get-media-item db item-id)]
      (if item
        {:status 200 :body (unqualify-keys item)}
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
