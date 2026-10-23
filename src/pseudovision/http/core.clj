(ns pseudovision.http.core
  (:require [reitit.ring                            :as ring]
            [reitit.openapi                         :as openapi]
            [reitit.swagger-ui                      :as swagger-ui]
            [reitit.coercion.malli                  :as malli-coercion]
            [reitit.ring.coercion                   :as rrc]
            [reitit.ring.middleware.parameters      :as parameters]
            [reitit.ring.middleware.muuntaja        :as muuntaja-mw]
            [ring.adapter.jetty      :as jetty]
            [pseudovision.http.middleware     :as mw]
            [pseudovision.http.schemas        :as s]
            [pseudovision.http.api.channels   :as ch]
            [pseudovision.http.api.schedules  :as sc]
            [pseudovision.http.api.playouts   :as pl]
            [pseudovision.http.api.epg        :as epg]
            [pseudovision.http.api.m3u        :as m3u]
            [pseudovision.http.api.media      :as med]
            [pseudovision.http.api.streaming  :as streaming]
            [pseudovision.http.api.tags       :as tags]
            [pseudovision.http.api.test       :as test]
            [pseudovision.http.api.ffmpeg     :as ffmpeg]
            [pseudovision.http.api.logos      :as logos]
            [pseudovision.http.api.metrics    :as metrics]))

(defn- routes [ctx]
  [""
   ;; ── OpenAPI spec ────────────────────────────────────────────────────────
   ["/openapi.json"
    {:get {:no-doc  true
           :openapi {:info {:title       "Pseudovision API"
                            :version     "0.1.0"
                            :description "Pseudovision REST API"}}
           :handler (openapi/create-openapi-handler)}}]

   ;; ── Health ──────────────────────────────────────────────────────────────
   ["/health"
    {:get {:tags      ["health"]
           :summary   "Liveness probe"
           :responses {200 {:body s/Health}}
           :handler   (fn [_] {:status 200 :body {:status "ok"}})}}]

   ;; ── Channels ────────────────────────────────────────────────────────────
   ["/api/channels"
    {:tags    ["channels"]
     :get     {:summary     "List channels (optionally filter by UUID)"
               :parameters  {:query [:map
                                     [:uuid {:optional true} :uuid]]}
               :responses   {200 {:body [:or s/Channel [:vector s/Channel]]}
                             404 {:body s/APIError}}
               :handler     (ch/list-channels-handler ctx)}
     :post    {:summary     "Create a channel"
               :parameters  {:body s/ChannelCreate}
               :responses   {201 {:body s/Channel}
                             400 {:body s/CoercionError}}
               :handler     (ch/create-channel-handler ctx)}}]
   ["/api/channels/:id"
    {:tags    ["channels"]
     :parameters {:path [:map [:id s/ChannelId]]}
     :get     {:summary     "Get a channel by id"
               :responses   {200 {:body s/Channel}
                             404 {:body s/APIError}}
               :handler     (ch/get-channel-handler ctx)}
     :put     {:summary     "Update a channel"
               :parameters  {:body s/ChannelUpdate}
               :responses   {200 {:body s/Channel}
                             404 {:body s/APIError}
                             400 {:body s/CoercionError}}
               :handler     (ch/update-channel-handler ctx)}
     :delete  {:summary     "Delete a channel"
               :responses   {204 {}}
               :handler     (ch/delete-channel-handler ctx)}}]

   ;; ── Schedules ───────────────────────────────────────────────────────────
   ["/api/schedules"
    {:tags ["schedules"]
     :get  {:summary    "List schedules"
            :responses  {200 {:body [:vector s/Schedule]}}
            :handler    (sc/list-schedules-handler ctx)}
     :post {:summary    "Create a schedule"
            :parameters {:body s/ScheduleCreate}
            :responses  {201 {:body s/Schedule}
                         400 {:body s/CoercionError}}
            :handler    (sc/create-schedule-handler ctx)}}]
   ["/api/schedules/:id"
    {:tags       ["schedules"]
     :parameters {:path [:map [:id s/ScheduleId]]}
     :get     {:summary   "Get a schedule"
               :responses {200 {:body s/Schedule}
                           404 {:body s/APIError}}
               :handler   (sc/get-schedule-handler ctx)}
     :put     {:summary    "Update a schedule"
               :parameters {:body s/ScheduleUpdate}
               :responses  {200 {:body s/Schedule}
                            404 {:body s/APIError}
                            400 {:body s/CoercionError}}
               :handler    (sc/update-schedule-handler ctx)}
     :delete  {:summary   "Delete a schedule"
               :responses {204 {}}
               :handler   (sc/delete-schedule-handler ctx)}}]
   ["/api/schedules/:schedule-id/slots"
    {:tags       ["schedules"]
     :parameters {:path [:map [:schedule-id s/ScheduleId]]}
     :get  {:summary   "List slots for a schedule"
            :responses {200 {:body [:vector s/Slot]}}
            :handler   (sc/list-slots-handler ctx)}
     :post {:summary    "Create a slot"
            :parameters {:body s/SlotCreate}
            :responses  {201 {:body s/Slot}
                         400 {:body s/CoercionError}}
            :handler    (sc/create-slot-handler ctx)}}]
   ["/api/schedules/:schedule-id/slots/:id"
    {:tags       ["schedules"]
     :parameters {:path [:map
                         [:schedule-id s/ScheduleId]
                         [:id          s/SlotId]]}
     :get     {:summary   "Get a slot"
               :responses {200 {:body s/Slot}
                           404 {:body s/APIError}}
               :handler   (sc/get-slot-handler ctx)}
     :put     {:summary    "Update a slot"
               :parameters {:body s/SlotUpdate}
               :responses  {200 {:body s/Slot}
                            404 {:body s/APIError}
                            400 {:body s/CoercionError}}
               :handler    (sc/update-slot-handler ctx)}
     :delete  {:summary   "Delete a slot"
               :responses {204 {}}
               :handler   (sc/delete-slot-handler ctx)}}]

   ;; ── Playouts ────────────────────────────────────────────────────────────
   ["/api/channels/:channel-id/playout"
    {:tags       ["playouts"]
     :parameters {:path [:map [:channel-id s/ChannelId]]}
     :get  {:summary   "Get the playout for a channel"
            :responses {200 {:body s/Playout}
                        404 {:body s/APIError}}
            :handler   (pl/get-playout-handler ctx)}
     :post {:summary    "Rebuild a playout's event timeline"
            :parameters {:query s/RebuildQuery}
            :responses  {200 {:body s/RebuildResult}
                         404 {:body s/APIError}}
            :handler    (pl/rebuild-playout-handler ctx)}}]
   ["/api/channels/:channel-id/playout/events"
    {:tags       ["playouts"]
     :parameters {:path [:map [:channel-id s/ChannelId]]}
     :get  {:summary   "List upcoming playout events"
            :responses {200 {:body [:vector s/PlayoutEvent]}
                        404 {:body s/APIError}}
            :handler   (pl/list-events-handler ctx)}
     :post {:summary    "Inject a manual event into the timeline"
            :parameters {:body s/ManualEventCreate}
            :responses  {201 {:body s/PlayoutEvent}
                         404 {:body s/APIError}
                         400 {:body s/CoercionError}}
            :handler    (pl/inject-event-handler ctx)}}]
   ["/api/channels/:channel-id/playout/events/:id"
    {:tags       ["playouts"]
     :parameters {:path [:map
                         [:channel-id s/ChannelId]
                         [:id         s/EventId]]}
     :put    {:summary    "Update a manual event"
              :parameters {:body s/ManualEventUpdate}
              :responses  {200 {:body s/PlayoutEvent}
                           404 {:body s/APIError}
                           400 {:body s/CoercionError}}
              :handler    (pl/update-event-handler ctx)}
     :delete {:summary   "Delete a manual event"
              :responses {204 {}}
              :handler   (pl/delete-event-handler ctx)}}]

   ;; ── Media ───────────────────────────────────────────────────────────────
   ["/api/media/sources"
    {:tags ["media"]
     :get  {:summary   "List media sources"
            :responses {200 {:body [:vector s/MediaSource]}}
            :handler   (med/list-sources-handler ctx)}
     :post {:summary    "Create a media source"
            :parameters {:body s/MediaSourceCreate}
            :responses  {201 {:body s/MediaSource}
                         400 {:body s/CoercionError}}
            :handler    (med/create-source-handler ctx)}}]
   ["/api/media/sources/:id"
    {:tags       ["media"]
     :parameters {:path [:map [:id s/MediaSourceId]]}
     :put    {:summary    "Update a media source"
              :parameters {:body s/MediaSourceUpdate}
              :responses  {200 {:body s/MediaSource}
                           404 {:body s/APIError}}
              :handler    (med/update-source-handler ctx)}
     :delete {:summary   "Delete a media source"
              :responses {204 {}}
              :handler   (med/delete-source-handler ctx)}}]
   ["/api/media/libraries"
    {:tags ["media"]
     :get  {:summary   "List all libraries across sources"
            :responses {200 {:body [:vector s/MediaLibrary]}}
            :handler   (med/list-all-libraries-handler ctx)}}]
   ["/api/media/sources/:id/libraries"
    {:tags       ["media"]
     :parameters {:path [:map [:id s/MediaSourceId]]}
     :get  {:summary   "List libraries for a media source"
            :responses {200 {:body [:vector s/MediaLibrary]}}
            :handler   (med/list-libraries-handler ctx)}
     :post {:summary    "Create a library under a media source"
            :parameters {:body s/MediaLibraryCreate}
            :responses  {201 {:body s/MediaLibrary}
                         400 {:body s/CoercionError}}
            :handler    (med/create-library-handler ctx)}}]
   ["/api/media/sources/:id/libraries/discover"
    {:tags       ["media"]
     :parameters {:path [:map [:id s/MediaSourceId]]}
     :post {:summary   "Discover libraries from a remote source"
            :responses {201 {:body s/DiscoveryResult}
                        400 {:body s/APIError}
                        404 {:body s/APIError}}
            :handler   (med/discover-libraries-handler ctx)}}]
   ["/api/media/libraries/:id"
    {:tags       ["media"]
     :parameters {:path [:map [:id s/LibraryId]]}
     :delete {:summary   "Delete a library"
              :responses {204 {}}
              :handler   (med/delete-library-handler ctx)}}]
   ["/api/media/libraries/:id/items"
    {:tags       ["media"]
     :parameters {:path  [:map [:id s/LibraryId]]
                  :query [:map
                          [:attrs     {:optional true} :string]
                          [:type      {:optional true} :string]
                          [:parent-id {:optional true} :int]]}
     :get {:summary   "List media items in a library"
           :responses {200 {:body [:vector s/MediaItem]}}
           :handler   (med/list-library-items-handler ctx)}}]
   ["/api/media/libraries/:id/scan"
    {:tags       ["media"]
     :parameters {:path [:map [:id s/LibraryId]]}
     :post {:summary   "Trigger an asynchronous library scan"
            :responses {202 {:body s/ScanTriggerResult}
                        404 {:body s/APIError}}
            :handler   (med/trigger-scan-handler ctx)}}]
   ["/api/media/items/:id"
    {:tags       ["media"]
     :parameters {:path [:map [:id s/MediaItemId]]}
     :get {:summary   "Get a media item"
           :responses {200 {:body s/MediaItem}
                       404 {:body s/APIError}}
           :handler   (med/get-media-item-handler ctx)}}]
   ["/api/media/items/:id/playback-url"
    {:tags       ["media"]
     :parameters {:path [:map [:id s/MediaItemId]]}
     :get {:summary   "Resolve a direct playback URL for a media item"
           :responses {200 {:body s/PlaybackUrl}
                       404 {:body s/APIError}
                       422 {:body s/APIError}}
           :handler   (med/get-item-playback-url-handler ctx)}}]
   ["/api/media/items/:id/stream"
    {:tags       ["media"]
     :parameters {:path [:map [:id s/MediaItemId]]}
     :get {:summary "Proxy a media item's stream from its source"
           :handler (med/redirect-to-stream-handler ctx)}}]
   ["/api/media/collections"
    {:tags ["media"]
     :get  {:summary   "List collections"
            :responses {200 {:body [:vector s/Collection]}}
            :handler   (med/list-collections-handler ctx)}
     :post {:summary    "Create a collection"
            :parameters {:body s/CollectionCreate}
            :responses  {201 {:body s/Collection}
                         400 {:body s/CoercionError}}
            :handler    (med/create-collection-handler ctx)}}]

   ;; ── Tags ────────────────────────────────────────────────────────────────
   ["/api/tags"
    {:tags ["tags"]
     :get {:summary   "List all tags with usage counts"
           :responses {200 {:body [:vector s/TagUsage]}}
           :handler   (tags/list-all-tags-handler ctx)}}]
   ["/api/media-items/:id/tags"
    {:tags       ["tags"]
     :parameters {:path [:map [:id s/MediaItemId]]}
     :get  {:summary   "List tags on a media item"
            :responses {200 {:body [:vector s/TagName]}}
            :handler   (tags/get-tags-handler ctx)}
     :post {:summary    "Add tags to a media item"
            :parameters {:body s/TagCreate}
            :responses  {200 {:body s/TagAddResult}
                         400 {:body s/APIError}
                         404 {:body s/APIError}}
            :handler    (tags/add-tags-handler ctx)}}]
   ["/api/media-items/:id/tags/:tag"
    {:tags       ["tags"]
     :parameters {:path [:map
                         [:id  s/MediaItemId]
                         [:tag s/TagName]]}
     :delete {:summary   "Remove a tag from a media item"
              :responses {204 {}}
              :handler   (tags/delete-tag-handler ctx)}}]

   ;; ── FFmpeg Profiles ─────────────────────────────────────────────────────
   ["/api/ffmpeg/profiles"
    {:tags ["ffmpeg"]
     :get  {:summary   "List FFmpeg profiles"
            :responses {200 {:body [:vector s/FFmpegProfile]}}
            :handler   (ffmpeg/list-profiles-handler ctx)}
     :post {:summary    "Create an FFmpeg profile"
            :parameters {:body s/FFmpegProfileCreate}
            :responses  {201 {:body s/FFmpegProfile}
                         400 {:body s/APIError}}
            :handler    (ffmpeg/create-profile-handler ctx)}}]
   ["/api/ffmpeg/profiles/:id"
    {:tags       ["ffmpeg"]
     :parameters {:path [:map [:id s/FFmpegProfileId]]}
     :get    {:summary   "Get an FFmpeg profile"
              :responses {200 {:body s/FFmpegProfile}
                          404 {:body s/APIError}}
              :handler   (ffmpeg/get-profile-handler ctx)}
     :put    {:summary    "Update an FFmpeg profile"
              :parameters {:body s/FFmpegProfileUpdate}
              :responses  {200 {:body s/FFmpegProfile}
                           404 {:body s/APIError}
                           400 {:body s/APIError}}
              :handler    (ffmpeg/update-profile-handler ctx)}
     :delete {:summary   "Delete an FFmpeg profile"
              :responses {200 {:body s/FFmpegProfileDeleted}
                          404 {:body s/APIError}
                          400 {:body s/APIError}}
              :handler   (ffmpeg/delete-profile-handler ctx)}}]

   ;; ── Version & Health ────────────────────────────────────────────────────
   ["/api/version"
    {:tags ["meta"]
     :get {:summary   "Build/version metadata"
           :responses {200 {:body s/Version}}
           :handler (fn [_] {:status 200
                             :body {:git-commit    (System/getenv "GIT_COMMIT")
                                    :git-timestamp (System/getenv "GIT_TIMESTAMP")
                                    :version-tag   (System/getenv "VERSION_TAG")}})}}]

   ;; ── Test Utilities ──────────────────────────────────────────────────────
   ["/api/test/info"
    {:tags ["test"]
     :get  {:summary "Test API metadata and usage examples"
            :handler (test/test-info-handler ctx)}}]
   ["/api/test/collection"
    {:tags ["test"]
     :post {:summary    "Create a manual test collection with every media item"
            :parameters {:body [:map [:name {:optional true} :string]]}
            :handler    (test/create-test-collection-handler ctx)}}]
   ["/api/test/channels"
    {:tags ["test"]
     :get  {:summary "List test channels"
            :handler (test/list-test-channels-handler ctx)}
     :post {:summary    "Create a test channel for streaming verification"
            :parameters {:body [:map
                                [:number        {:optional true} :string]
                                [:name          {:optional true} :string]
                                [:collection-id {:optional true} :int]]}
            :handler    (test/create-test-channel-handler ctx)}}]
   ["/api/test/channels/:identifier"
    {:tags       ["test"]
     :parameters {:path [:map [:identifier :string]]}
     :delete {:summary "Delete a test channel"
              :handler (test/delete-test-channel-handler ctx)}}]
   ["/api/test/channels/:identifier/artwork"
    {:tags       ["test"]
     :parameters {:path [:map [:identifier :string]]}
     :post {:summary "Attach a generated test logo to a channel"
            :handler (test/add-test-artwork-handler ctx)}}]

   ;; ── Output formats ──────────────────────────────────────────────────────
   ["/xmltv"
    {:tags ["epg"]
     :get  {:summary "Generate XMLTV EPG document (next 7 days)"
            :handler (epg/xmltv-handler ctx)}}]
   ["/epg.xml"
    {:tags ["epg"]
     :get  {:summary "XMLTV EPG document — alias for /xmltv"
            :handler (epg/xmltv-handler ctx)}}]
   ["/iptv/channels.m3u"
    {:tags ["iptv"]
     :get  {:summary "M3U playlist of all channels with stream URLs"
            :handler (m3u/m3u-handler ctx)}}]
   ["/media/devices/X-Plex-Client-Profile-Extra"
    {:tags ["hdhr"]
     :get  {:summary "HDHomeRun device discovery document"
            :handler (m3u/hdhr-device-handler ctx)}}]
   ["/lineup.json"
    {:tags ["hdhr"]
     :get  {:summary "HDHomeRun channel lineup"
            :handler (m3u/hdhr-lineup-handler ctx)}}]
   ["/lineup_status.json"
    {:tags ["hdhr"]
     :get  {:summary "HDHomeRun tuner status"
            :handler (m3u/hdhr-status-handler ctx)}}]

   ;; ── Streaming ───────────────────────────────────────────────────────────
   ["/stream/:uuid"
    {:tags       ["streaming"]
     :parameters {:path [:map [:uuid :uuid]]}
     :get        {:summary "HLS playlist for a channel (starts FFmpeg if needed)"
                  :handler (streaming/stream-handler ctx)}}]
   ["/stream/:uuid/:segment"
    {:tags       ["streaming"]
     :parameters {:path [:map [:uuid :uuid] [:segment :string]]}
     :get        {:summary "HLS transport-stream segment"
                  :handler (streaming/segment-handler ctx)}}]

   ;; ── Artwork ─────────────────────────────────────────────────────────────
   ["/logos/:uuid"
    {:tags       ["artwork"]
     :parameters {:path [:map [:uuid :uuid]]}
     :get        {:summary "Channel logo/artwork by channel UUID"
                  :handler (logos/logos-handler ctx)}}]

   ;; ── Debug ───────────────────────────────────────────────────────────────
   ["/api/debug/stream/:uuid"
    {:tags       ["debug"]
     :parameters {:path [:map [:uuid :uuid]]}
     :get        {:summary  "Diagnostic information for a running HLS stream"
                  :handler  (streaming/stream-debug-handler ctx)}}]

   ;; ── Metrics ─────────────────────────────────────────────────────────────
   ["/api/metrics/channels"
    {:tags ["metrics"]
     :get  {:summary    "List channel view events"
            :parameters {:query [:map
                                  [:channel-id {:optional true} :string]
                                  [:from       {:optional true} :string]
                                  [:to         {:optional true} :string]
                                  [:limit      {:optional true} :string]]}
            :handler    (metrics/list-channel-views-handler ctx)}}]
   ["/api/metrics/media-items"
    {:tags ["metrics"]
     :get  {:summary    "List media item view events with percent_watched"
            :parameters {:query [:map
                                  [:channel-id    {:optional true} :string]
                                  [:media-item-id {:optional true} :string]
                                  [:from          {:optional true} :string]
                                  [:to            {:optional true} :string]
                                  [:limit         {:optional true} :string]]}
            :handler    (metrics/list-media-item-views-handler ctx)}}]])

(defn make-handler
  "Creates the reitit Ring handler.

   Route data supplies the canonical Reitit middleware chain — parameters,
   Muuntaja (JSON request decoding), the application exception handler, and
   malli coercion for :parameters / :responses. Routes without schemas still
   traverse the chain as a pass-through; :body-params is populated from
   request JSON identically to the old custom middleware.

   Outer wraps — error handling, request logging, JSON response encoding —
   cover the entire dispatch tree so that unmatched routes (404/405) and
   the Swagger UI handler also go through them."
  [ctx]
  (let [dispatch (ring/ring-handler
                  (ring/router
                   (routes ctx)
                   {:data {:muuntaja   mw/muuntaja
                           :coercion   malli-coercion/coercion
                           :middleware [parameters/parameters-middleware
                                        muuntaja-mw/format-negotiate-middleware
                                        muuntaja-mw/format-request-middleware
                                        mw/exception-middleware
                                        rrc/coerce-request-middleware
                                        rrc/coerce-response-middleware]}})
                  (ring/routes
                   (swagger-ui/create-swagger-ui-handler
                    {:path "/swagger-ui"
                     :url  "/openapi.json"})
                   (ring/create-default-handler
                    {:not-found          (fn [_] {:status 404 :body {:error "Not found"}})
                     :method-not-allowed (fn [_] {:status 405 :body {:error "Method not allowed"}})})))]
    (-> dispatch
        mw/wrap-json-response
        mw/wrap-request-logging
        mw/wrap-error-handler)))

(defn start-server!
  "Assembles the handler context from opts and starts a non-blocking Jetty server."
  [{:keys [port db ffmpeg media scheduling] :as _opts}]
  (let [ctx     {:db db :ffmpeg ffmpeg :media media :scheduling scheduling}
        handler (make-handler ctx)]
    (jetty/run-jetty handler {:port port :join? false})))

(defn stop-server! [server]
  (.stop server))
