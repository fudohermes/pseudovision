(ns pseudovision.http.api.playouts
  (:require [pseudovision.db.playouts      :as db]
            [pseudovision.db.channels      :as db-channels]
            [pseudovision.scheduling.core  :as sched]
            [pseudovision.jobs.runner       :as runner]
            [pseudovision.util.time         :as t]
            [pseudovision.util.sql          :as sql-util]
            [pseudovision.util.pagination   :as pagination]))

(defn- unqualify-keys [m]
  (when m
    (reduce-kv (fn [acc k v] (assoc acc (keyword (name k)) v)) {} m)))

(defn- resolve-channel-id
  "Resolves a path parameter to the internal channel ID. The value is always an
   integer (enforced by the Malli schema), but it may refer to a channel number
   rather than the internal primary key. Try the primary key first; if no
   channel matches, fall back to a channel-number lookup."
  [ds id]
  (let [int-id (cond
                 (integer? id) id
                 (string? id) (try (Integer/parseInt id) (catch Exception _ nil))
                 :else nil)
        str-id (when id (str id))]
    (or (when int-id
          (when-let [ch (db-channels/get-channel ds int-id)]
            (:channels/id ch)))
        (when str-id
          (when-let [ch (db-channels/get-channel-by-number ds str-id)]
            (:channels/id ch))))))

(defn get-playout-handler [{:keys [db]}]
  (fn [req]
    (let [raw-id    (get-in req [:parameters :path :channel-id])
          channel-id (resolve-channel-id db raw-id)]
      (if-let [p (and channel-id (db/get-playout-for-channel db channel-id))]
        {:status 200 :body (unqualify-keys p)}
        {:status 404 :body {:error "No playout for this channel"}}))))

(defn attach-schedule-handler
  "PUT /api/channels/:channel-id/playout — attaches a schedule to the
   channel's playout, creating the playout row on first attach. This is the
   missing link the native scheduling engine's schedules/slots/collections
   needed: schedule_id lives on `playouts`, not `channels`, and until this
   endpoint there was no HTTP path to set it (pseudovision.db.playouts/
   upsert-playout! was previously called only from a dev/test helper).
   Does not itself rebuild the timeline — POST .../playout afterwards (or
   pass ?rebuild=true) to regenerate events against the newly-attached
   schedule."
  [{:keys [db jobs grout]}]
  (fn [req]
    (let [channel-id  (resolve-channel-id db (get-in req [:parameters :path :channel-id]))
          schedule-id (get-in req [:parameters :body :schedule-id])
          qp          (get-in req [:parameters :query])
          rebuild?    (boolean (:rebuild qp))
          horizon-days (or (:horizon qp) 14)]
      (if (nil? channel-id)
        {:status 404 :body {:error "Channel not found"}}
        (let [playout (db/attach-schedule! db channel-id schedule-id)]
          (when rebuild?
            (runner/submit!
             jobs
             {:type     :playout/rebuild
              :metadata {:channel-id channel-id :from "now" :horizon-days horizon-days}}
             (fn [_report-progress]
               (let [count (sched/rebuild-from-now! db (:playouts/id playout) horizon-days {:grout grout})]
                 {:message          "Rebuild complete"
                  :events-generated count
                  :horizon-days     horizon-days
                  :from             "now"}))))
          {:status 200 :body (unqualify-keys playout)})))))

(defn rebuild-playout-handler
  "Rebuilding a playout's timeline can take minutes, so it runs as an
   asynchronous job rather than blocking the request. Returns 202 with the
   initial job record; poll `GET /api/jobs/:job-id` for progress and the final
   `:result` ({:events-generated N :horizon-days D :from \"now\"|\"horizon\"})."
  [{:keys [db jobs grout]}]
  (fn [req]
    (let [channel-id   (resolve-channel-id db (get-in req [:parameters :path :channel-id]))
          playout      (and channel-id (db/get-playout-for-channel db channel-id))
          qp           (get-in req [:parameters :query])
          from         (or (:from qp) "now")
          horizon-days (or (:horizon qp) 14)
          sched-opts   {:grout grout}]
      (if playout
        (let [playout-id (:playouts/id playout)
              job (runner/submit!
                    jobs
                    {:type     :playout/rebuild
                     :metadata {:channel-id   channel-id
                                :from         from
                                :horizon-days horizon-days}}
                    (fn [_report-progress]
                      (let [count (case from
                                    "now"     (sched/rebuild-from-now! db playout-id horizon-days sched-opts)
                                    "horizon" (sched/rebuild-horizon! db playout-id 7 horizon-days sched-opts)
                                    (sched/rebuild-from-now! db playout-id horizon-days sched-opts))]
                        (cond-> {:message          "Rebuild complete"
                                 :events-generated count
                                 :horizon-days     horizon-days
                                 :from             from}
                          ;; build! returns 0 without touching the playout when
                          ;; no schedule is attached, which looked exactly like
                          ;; "already at the horizon". Say so in the result.
                          (nil? (:playouts/schedule-id playout))
                          (assoc :warning "no schedule attached; nothing to build")))))]
          {:status 202 :body {:job job}})
        {:status 404 :body {:error "No playout for this channel"}}))))

(defn ensure-horizon-all-handler
  "POST /api/playouts/ensure — daily top-up across all channels.

   Extends every schedule-backed channel's playout timeline to at least
   ?horizon days from now (default 7), preserving the events already scheduled
   in the near term and advancing each show's rotation. Idempotent — a channel
   already covered past the horizon is left untouched — so this is safe to call
   on a daily cron to guarantee upcoming content. Channels with no attached
   schedule are skipped.

   Like the per-channel rebuild this can take a while, so it runs as an async
   job: returns 202 with the job record; poll GET /api/jobs/:job-id for the
   per-channel summary in :result."
  [{:keys [db jobs grout]}]
  (fn [req]
    (let [horizon-days (or (get-in req [:parameters :query :horizon]) 7)
          job (runner/submit!
                jobs
                {:type     :playout/ensure-all
                 :metadata {:horizon-days horizon-days}}
                (fn [_report-progress]
                  (sched/ensure-all-horizons! db horizon-days {:grout grout})))]
      {:status 202 :body {:job job}})))

(defn- parse-instant [s]
  (when s
    (try (java.time.Instant/parse s)
         (catch Exception _ nil))))

(defn clear-playout-handler
  "DELETE /api/channels/:channel-id/playout — clears the channel's entire
   playout timeline and resets the saved cursor so the next rebuild starts
   fresh from now. Manually-injected events are preserved unless ?manual=true."
  [{:keys [db]}]
  (fn [req]
    (let [channel-id   (resolve-channel-id db (get-in req [:parameters :path :channel-id]))
          playout      (and channel-id (db/get-playout-for-channel db channel-id))
          wipe-manual? (boolean (get-in req [:parameters :query :manual]))]
      (if playout
        (let [deleted (db/reset-playout! db (:playouts/id playout) (not wipe-manual?))]
          {:status 200
           :body {:message               "Playout cleared and cursor reset"
                  :events-deleted        deleted
                  :manual-events-removed wipe-manual?}})
        {:status 404 :body {:error "No playout for this channel"}}))))

(defn clear-events-handler
  "DELETE /api/channels/:channel-id/playout/events — bulk-deletes events,
   optionally restricted to a [from, to) window. An event is removed when it
   overlaps the window (starts before `to` and finishes after `from`), so items
   straddling either edge are included. `from`/`to` are optional ISO-8601
   timestamps and may be given independently. Manual events are preserved
   unless ?manual=true. Returns 400 on an unparseable timestamp."
  [{:keys [db]}]
  (fn [req]
    (let [channel-id   (resolve-channel-id db (get-in req [:parameters :path :channel-id]))
          playout      (and channel-id (db/get-playout-for-channel db channel-id))
          qp           (get-in req [:parameters :query])
          from-raw     (:from qp)
          to-raw       (:to qp)
          from         (parse-instant from-raw)
          to           (parse-instant to-raw)
          wipe-manual? (boolean (:manual qp))]
      (cond
        (nil? playout)
        {:status 404 :body {:error "No playout for this channel"}}

        ;; Reject malformed bounds rather than silently widening the delete.
        (or (and from-raw (nil? from))
            (and to-raw   (nil? to)))
        {:status 400 :body {:error "Invalid 'from'/'to' timestamp; expected ISO-8601"}}

        :else
        (let [deleted (db/delete-events! db (:playouts/id playout)
                                         {:keep-manual? (not wipe-manual?)
                                          :from from :to to})]
          {:status 200
           :body {:message               "Events deleted"
                  :events-deleted        deleted
                  :manual-events-removed wipe-manual?}})))))

(defn- enrich-event
  "Adds display-friendly fields: resolves title from metadata and adds a
   relative API link to the media item."
  [event]
  (let [base         (unqualify-keys event)
        media-id     (:media-item-id base)
        title        (:title base)
        display-name (or (:custom-title base) title)]
    (assoc base
           :title          display-name
           :media-item-link (when media-id
                              (str "/api/media/items/" media-id)))))

(defn list-events-handler [{:keys [db]}]
  (fn [req]
    (let [channel-id (resolve-channel-id db (get-in req [:parameters :path :channel-id]))
          qp         (get-in req [:parameters :query])
          limit      (or (:limit qp) 100)
          cursor     (parse-instant (:cursor qp))
          playout    (and channel-id (db/get-playout-for-channel db channel-id))]
      (if playout
        (let [now    (t/now)
              events (db/get-upcoming-events-with-metadata
                       db (:playouts/id playout) now limit
                       :cursor cursor)
              items  (mapv enrich-event events)]
          {:status 200
           :body (pagination/cursor-pagination-response
                   items
                   limit
                   (fn [last-item]
                     (when-let [start-at (:start-at last-item)]
                       (str start-at))))})
        {:status 404 :body {:error "No playout for this channel"}}))))

(defn- ->instant [x]
  (cond
    (nil? x)        nil
    (string? x)     (java.time.Instant/parse x)
    :else           x))

(defn inject-event-handler
  "Injects a manual event (bumper, ad, etc.) into the playout timeline.
   The event is inserted with is_manual=true so rebuilds preserve it."
  [{:keys [db]}]
  (fn [req]
    (let [channel-id (resolve-channel-id db (get-in req [:parameters :path :channel-id]))
          playout    (and channel-id (db/get-playout-for-channel db channel-id))]
      (if playout
        (let [body  (get-in req [:parameters :body])
              kind  (or (:kind body) "content")
              attrs (-> body
                        (assoc :playout-id (:playouts/id playout)
                               :is-manual  true
                               :kind       (sql-util/->pg-enum "event_kind" kind))
                        (cond->
                          (:start-at body)  (update :start-at  ->instant)
                          (:finish-at body) (update :finish-at ->instant)))]
          {:status 201 :body (db/create-event! db attrs)})
        {:status 404 :body {:error "No playout for this channel"}}))))

(defn update-event-handler [{:keys [db]}]
  (fn [req]
    (let [id    (get-in req [:parameters :path :id])
          body  (get-in req [:parameters :body])
          attrs (cond-> body
                  (:start-at  body) (update :start-at  ->instant)
                  (:finish-at body) (update :finish-at ->instant)
                  (:kind      body) (update :kind #(sql-util/->pg-enum "event_kind" %)))]
      (if-let [ev (db/update-event! db id attrs)]
        {:status 200 :body ev}
        {:status 404 :body {:error "Event not found"}}))))

(defn delete-event-handler [{:keys [db]}]
  (fn [req]
    (db/delete-event! db (get-in req [:parameters :path :id]))
    {:status 204 :body nil}))
