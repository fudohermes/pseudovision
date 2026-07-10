(ns pseudovision.system
  (:require [integrant.core              :as ig]
            [pseudovision.db.core        :as db]
            [pseudovision.http.core      :as http]
            [pseudovision.cleanup        :as cleanup]
            [pseudovision.jobs.runner    :as jobs]
            [pseudovision.media.grout    :as grout]
            [pseudovision.streaming.manager :as stream-mgr]
            [pseudovision.streaming.segment-store :as store]
            [taoensso.timbre             :as log])
  (:import [java.util.concurrent Executors TimeUnit]))

;; ---------------------------------------------------------------------------
;; Config → Integrant keys
;; ---------------------------------------------------------------------------

(defn- parse-log-level [level]
  (cond
    (keyword? level) level
    (string? level)  (keyword level)
    :else            :info))

(defn ->system-config
  [{:keys [log-level server database ffmpeg media scheduling daily-slots streaming grout]}]
  (letfn [(parse-int [i] (if (string? i) (Integer/parseInt i) i))]
    {:pseudovision/logger    {:level     (parse-log-level (or log-level :info))}
     :pseudovision/db        {:jdbc-url  (:jdbc-url database)
                              :username  (:username database)
                              :password  (:password database)}
     :pseudovision/ffmpeg    {:ffmpeg-path  (:ffmpeg-path  ffmpeg)
                              :ffprobe-path (:ffprobe-path ffmpeg)}
     :pseudovision/media     (merge {:scan-concurrency 4
                                     :probe-timeout-ms 30000}
                                    media)
     :pseudovision/scheduling (merge {:lookahead-hours 72
                                      :rebuild-interval-minutes 60}
                                     scheduling)
     :pseudovision/daily-slots (merge {:fitter-strictness :warn-and-fill}
                                      daily-slots)
     :pseudovision/streaming (merge {:db (ig/ref :pseudovision/db)}
                                    streaming)
      :pseudovision/grout     (or grout {})
      :pseudovision/jobs      {}
      :pseudovision/cleanup   {:db (ig/ref :pseudovision/db)}
      :pseudovision/http      {:port        (or (some-> server :port (parse-int)) 8080)
                               :db          (ig/ref :pseudovision/db)
                               :ffmpeg      (ig/ref :pseudovision/ffmpeg)
                               :media       (ig/ref :pseudovision/media)
                               :scheduling  (ig/ref :pseudovision/scheduling)
                               :daily-slots (ig/ref :pseudovision/daily-slots)
                               :streams     (ig/ref :pseudovision/streaming)
                               :grout       (ig/ref :pseudovision/grout)
                               :jobs        (ig/ref :pseudovision/jobs)}}))

;; ---------------------------------------------------------------------------
;; Logger
;; ---------------------------------------------------------------------------

(defmethod ig/init-key :pseudovision/logger [_ {:keys [level]}]
  (log/set-min-level! level)
  (log/info "Logger initialised at level" level)
  level)

(defmethod ig/halt-key! :pseudovision/logger [_ _]
  (log/info "Logger shut down"))

;; ---------------------------------------------------------------------------
;; Database (connection pool + migrations)
;; ---------------------------------------------------------------------------

(defmethod ig/init-key :pseudovision/db [_ opts]
  (let [ds (db/make-datasource opts)]
    (log/info "Database ready")
    ds))

(defmethod ig/halt-key! :pseudovision/db [_ ds]
  (db/close-datasource! ds)
  (log/info "Database connection closed"))

;; ---------------------------------------------------------------------------
;; FFmpeg config (just passes the map through; used by media scanner)
;; ---------------------------------------------------------------------------

(defmethod ig/init-key :pseudovision/ffmpeg [_ opts] opts)
(defmethod ig/halt-key! :pseudovision/ffmpeg [_ _] nil)

;; ---------------------------------------------------------------------------
;; Media scanner config
;; ---------------------------------------------------------------------------

(defmethod ig/init-key :pseudovision/media [_ opts] opts)
(defmethod ig/halt-key! :pseudovision/media [_ _] nil)

;; ---------------------------------------------------------------------------
;; Scheduling config
;; ---------------------------------------------------------------------------

(defmethod ig/init-key :pseudovision/scheduling [_ opts] opts)

(defmethod ig/halt-key! :pseudovision/scheduling [_ _] nil)

;; ---------------------------------------------------------------------------
;; Streaming (Channel Stream Manager + idle reaper)
;; ---------------------------------------------------------------------------

(def ^:private stream-idle-ms (* 60 60 1000))   ; reap channels idle > 1h

(defmethod ig/init-key :pseudovision/streaming [_ {:keys [db streams-dir scratch-dir]}]
  (let [manager  (stream-mgr/make-manager
                   {:db db
                    :store (store/local-disk-store (or streams-dir "/tmp/pseudovision/streams"))
                    :scratch-dir (or scratch-dir "/tmp/pseudovision/scratch")})
        reaper   (Executors/newSingleThreadScheduledExecutor)]
    (.scheduleAtFixedRate reaper
                          #(try (stream-mgr/reap-idle! manager stream-idle-ms)
                                (catch Exception e (log/error e "stream reaper failed")))
                          60 60 TimeUnit/SECONDS)
    (log/info "Channel Stream Manager ready")
    (assoc manager :reaper reaper)))

;; Daily-slots configuration (slot-pack strictness policy). Just passes the
;; map through; read once at handler init time in
;; `pseudovision.http.api.daily-slots/ingest-daily-slots-handler`.
(defmethod ig/init-key :pseudovision/daily-slots [_ opts] opts)
(defmethod ig/halt-key! :pseudovision/daily-slots [_ _] nil)

(defmethod ig/halt-key! :pseudovision/streaming [_ {:keys [reaper] :as manager}]
  (when reaper (.shutdownNow ^java.util.concurrent.ScheduledExecutorService reaper))
  (stream-mgr/shutdown-all! manager)
  (log/info "Channel Stream Manager stopped"))

;; ---------------------------------------------------------------------------
;; Grout filler source (optional; nil when GROUT_URL is unset)
;; ---------------------------------------------------------------------------

(defmethod ig/init-key :pseudovision/grout [_ config]
  (let [client (grout/client config)]
    (log/info "Grout filler source"
              {:enabled  (grout/enabled? client)
               :base-url (:base-url client)})
    client))

(defmethod ig/halt-key! :pseudovision/grout [_ _] nil)

;; ---------------------------------------------------------------------------
;; Async job runner
;; ---------------------------------------------------------------------------

(defmethod ig/init-key :pseudovision/jobs [_ opts]
  (let [runner (jobs/create opts)]
    (log/info "Async job runner ready")
    runner))

(defmethod ig/halt-key! :pseudovision/jobs [_ runner]
  (jobs/shutdown! runner)
  (log/info "Async job runner stopped"))

;; ---------------------------------------------------------------------------
;; Cleanup daemon
;; ---------------------------------------------------------------------------

(defmethod ig/init-key :pseudovision/cleanup [_ {:keys [db]}]
  (cleanup/start-cleanup-daemon db))

(defmethod ig/halt-key! :pseudovision/cleanup [_ daemon]
  (cleanup/stop-cleanup-daemon daemon))

;; ---------------------------------------------------------------------------
;; HTTP server
;; ---------------------------------------------------------------------------

(defmethod ig/init-key :pseudovision/http
  [_ {:keys [port] :as opts}]
  (let [server (http/start-server! opts)]
    (log/info "HTTP server listening on port" port)
    server))

(defmethod ig/halt-key! :pseudovision/http [_ server]
  (http/stop-server! server)
  (log/info "HTTP server stopped"))


