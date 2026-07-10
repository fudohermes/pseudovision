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
            [pseudovision.http.api.filler     :as filler]
            [pseudovision.http.api.logos      :as logos]
            [pseudovision.http.api.metrics    :as metrics]
             [pseudovision.http.api.jobs       :as jobs]
             [pseudovision.http.api.catalog    :as catalog]
             [pseudovision.http.api.daily-slots :as ds]
             [pseudovision.ffmpeg.profile      :as profile]))

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
            :handler   (fn [_]
                         {:status 200
                          :body {:status   "ok"
                                 :accels   (mapv name (profile/available-accels))
                                 :streams  (count @(:registry (:streams ctx)))
                                 :hostname (try (.. java.net.InetAddress getLocalHost getHostName)
                                                (catch Exception _ nil))}})}}]

   ;; ── Channels ────────────────────────────────────────────────────────────
   ["/api/channels"
    {:tags    ["channels"]
     :get     {:summary     "List channels (optionally filter by UUID)"
               ;; IMPORTANT: When combining pagination parameters with other filters,
               ;; flatten all into a single :map. DO NOT use :merge operator.
               ;; ❌ WRONG:  [:merge [:map [:limit...] [:offset...]] [:map [:uuid...]]]
               ;; ✅ CORRECT: [:map [:limit...] [:offset...] [:uuid...]]
               :parameters  {:query [:map
                                     [:limit  {:optional true} :int]
                                     [:offset {:optional true} :int]
                                     [:uuid   {:optional true} :uuid]]}
               :responses   {200 {:body [:or s/Channel s/PaginatedChannels]}
                             404 {:body s/APIError}}
               :handler     (ch/list-channels-handler ctx)}
     :post    {:summary     "Create a channel"
               :parameters  {:body s/ChannelCreate}
               :responses   {201 {:body s/Channel}
                             400 {:body s/CoercionError}}
               :handler     (ch/create-channel-handler ctx)}}]
   ["/api/channels/:id"
    {:tags    ["channels"]
     :get     {:summary     "Get a channel by id"
               :parameters  {:path [:map [:id s/ChannelId]]}
               :responses   {200 {:body s/Channel}
                             404 {:body s/APIError}}
               :handler     (ch/get-channel-handler ctx)}
      :put     {:summary     "Update a channel"
                :parameters  {:path [:map [:id s/ChannelId]]
                              :body s/ChannelUpdate}
                :responses   {200 {:body s/Channel}
                              404 {:body s/APIError}
                              400 {:body s/CoercionError}}
                :handler     (ch/update-channel-handler ctx)}
      :patch   {:summary     "Partially update a channel"
                :parameters  {:path [:map [:id s/ChannelId]]
                              :body s/ChannelUpdate}
                :responses   {200 {:body s/Channel}
                              404 {:body s/APIError}
                              400 {:body s/CoercionError}}
                :handler     (ch/update-channel-handler ctx)}
      :delete  {:summary     "Delete a channel"
               :parameters  {:path [:map [:id s/ChannelId]]}
               :responses   {204 {}}
               :handler     (ch/delete-channel-handler ctx)}}]

   ;; ── Schedules ───────────────────────────────────────────────────────────
   ["/api/schedules"
    {:tags ["schedules"]
     :get  {:summary    "List schedules (paginated)"
            :parameters {:query s/PaginationQuery}
            :responses  {200 {:body s/PaginatedSchedules}}
            :handler    (sc/list-schedules-handler ctx)}
     :post {:summary    "Create a schedule"
            :parameters {:body s/ScheduleCreate}
            :responses  {201 {:body s/Schedule}
                         400 {:body s/CoercionError}}
            :handler    (sc/create-schedule-handler ctx)}}]
   ["/api/schedules/:id"
    {:tags       ["schedules"]
     :get     {:summary   "Get a schedule"
               :parameters {:path [:map [:id s/ScheduleId]]}
               :responses {200 {:body s/Schedule}
                           404 {:body s/APIError}}
               :handler   (sc/get-schedule-handler ctx)}
     :put     {:summary    "Update a schedule"
               :parameters {:path [:map [:id s/ScheduleId]]
                            :body s/ScheduleUpdate}
               :responses  {200 {:body s/Schedule}
                            404 {:body s/APIError}
                            400 {:body s/CoercionError}}
               :handler    (sc/update-schedule-handler ctx)}
     :delete  {:summary   "Delete a schedule"
               :parameters {:path [:map [:id s/ScheduleId]]}
               :responses {204 {}}
               :handler   (sc/delete-schedule-handler ctx)}}]
   ["/api/schedules/:schedule-id/slots"
    {:tags       ["schedules"]
     :get  {:summary   "List slots for a schedule (paginated)"
            :parameters {:path  [:map [:schedule-id s/ScheduleId]]
                         :query s/PaginationQuery}
            :responses {200 {:body s/PaginatedSlots}}
            :handler   (sc/list-slots-handler ctx)}
     :post {:summary    "Create a slot"
            :parameters {:path [:map [:schedule-id s/ScheduleId]]
                         :body s/SlotCreate}
            :responses  {201 {:body s/Slot}
                         400 {:body s/CoercionError}}
            :handler    (sc/create-slot-handler ctx)}}]
   ["/api/schedules/:schedule-id/slots/:id"
    {:tags       ["schedules"]
     :get     {:summary   "Get a slot"
               :parameters {:path [:map
                                   [:schedule-id s/ScheduleId]
                                   [:id          s/SlotId]]}
               :responses {200 {:body s/Slot}
                           404 {:body s/APIError}}
               :handler   (sc/get-slot-handler ctx)}
     :put     {:summary    "Update a slot"
               :parameters {:path [:map
                                   [:schedule-id s/ScheduleId]
                                   [:id          s/SlotId]]
                            :body s/SlotUpdate}
               :responses  {200 {:body s/Slot}
                            404 {:body s/APIError}
                            400 {:body s/CoercionError}}
               :handler    (sc/update-slot-handler ctx)}
     :delete  {:summary   "Delete a slot"
               :parameters {:path [:map
                                   [:schedule-id s/ScheduleId]
                                   [:id          s/SlotId]]}
               :responses {204 {}}
               :handler   (sc/delete-slot-handler ctx)}}]
   ;; Reorder lives as its own sub-resource rather than under /slots/ so it
   ;; never collides with the ":id" path parameter of the slot routes.
   ["/api/schedules/:schedule-id/slot-order"
    {:tags       ["schedules"]
     :parameters {:path [:map [:schedule-id s/ScheduleId]]}
     :put {:summary    "Reorder slots by providing an ordered list of slot IDs"
           :parameters {:body s/SlotReorderRequest}
           :responses  {200 {:body s/SlotReorderResult}
                        400 {:body s/CoercionError}}
           :handler    (sc/reorder-slots-handler ctx)}}]

   ;; ── Playouts ────────────────────────────────────────────────────────────
   ["/api/channels/:channel-id/playout"
    {:tags       ["playouts"]
     :get  {:summary   "Get the playout for a channel"
            :parameters {:path [:map [:channel-id s/ChannelId]]}
            :responses {200 {:body s/Playout}
                        404 {:body s/APIError}}
            :handler   (pl/get-playout-handler ctx)}
     :post {:summary    "Rebuild a playout's event timeline (async job)"
            :description "Starts an asynchronous rebuild job and returns 202 with
                          the job record. Poll GET /api/jobs/:job-id for progress
                          and the final result."
            :parameters {:path  [:map [:channel-id s/ChannelId]]
                         :query s/RebuildQuery}
            :responses  {202 {:body s/JobSubmitResponse}
                         404 {:body s/APIError}}
            :handler    (pl/rebuild-playout-handler ctx)}
     :delete {:summary    "Clear the whole playout timeline and reset its cursor"
              :parameters {:path  [:map [:channel-id s/ChannelId]]
                           :query s/ClearPlayoutQuery}
              :responses  {200 {:body s/ClearResult}
                           404 {:body s/APIError}}
              :handler    (pl/clear-playout-handler ctx)}}]
   ["/api/channels/:channel-id/playout/events"
    {:tags       ["playouts"]
     :get  {:summary   "List upcoming playout events (cursor-paginated)"
            :parameters {:path  [:map [:channel-id s/ChannelId]]
                         :query s/CursorPaginationQuery}
            :responses {200 {:body s/PaginatedPlayoutEvents}
                        404 {:body s/APIError}}
            :handler   (pl/list-events-handler ctx)}
     :post {:summary    "Inject a manual event into the timeline"
            :parameters {:path [:map [:channel-id s/ChannelId]]
                         :body s/ManualEventCreate}
            :responses  {201 {:body s/PlayoutEvent}
                         404 {:body s/APIError}
                         400 {:body s/CoercionError}}
            :handler    (pl/inject-event-handler ctx)}
     :delete {:summary    "Bulk-delete events, optionally within a [from,to) window"
              :parameters {:path  [:map [:channel-id s/ChannelId]]
                           :query s/ClearEventsQuery}
              :responses  {200 {:body s/ClearResult}
                           404 {:body s/APIError}
                           400 {:body s/APIError}}
              :handler    (pl/clear-events-handler ctx)}}]
   ["/api/channels/:channel-id/playout/events/:id"
    {:tags       ["playouts"]
     :put    {:summary    "Update a manual event"
              :parameters {:path [:map
                                  [:channel-id s/ChannelId]
                                  [:id         s/EventId]]
                           :body s/ManualEventUpdate}
              :responses  {200 {:body s/PlayoutEvent}
                           404 {:body s/APIError}
                           400 {:body s/CoercionError}}
              :handler    (pl/update-event-handler ctx)}
     :delete {:summary   "Delete a manual event"
              :parameters {:path [:map
                                  [:channel-id s/ChannelId]
                                  [:id         s/EventId]]}
              :responses {204 {}}
              :handler   (pl/delete-event-handler ctx)}}]

   ;; ── Jobs ────────────────────────────────────────────────────────────────
   ["/api/jobs"
    {:tags ["jobs"]
     :get  {:summary   "List async jobs (newest-first)"
            :responses {200 {:body s/JobListResponse}}
            :handler   (jobs/list-jobs-handler ctx)}}]
   ["/api/jobs/:job-id"
    {:tags       ["jobs"]
     :get  {:summary   "Get an async job's status, progress, and result"
            :parameters {:path [:map [:job-id s/JobId]]}
            :responses {200 {:body s/JobInfoResponse}
                        404 {:body s/APIError}}
            :handler   (jobs/get-job-handler ctx)}}]

   ;; ── Media ───────────────────────────────────────────────────────────────
   ["/api/media/sources"
    {:tags ["media"]
     :get  {:summary   "List media sources (paginated)"
            :parameters {:query s/PaginationQuery}
            :responses {200 {:body s/PaginatedMediaSources}}
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
                           404 {:body s/APIError}
                           400 {:body s/CoercionError}}
              :handler    (med/update-source-handler ctx)}
     :delete {:summary   "Delete a media source"
              :parameters {:path [:map [:id s/MediaSourceId]]}
              :responses {204 {}}
              :handler   (med/delete-source-handler ctx)}}]
   ["/api/media/libraries"
    {:tags ["media"]
     :get  {:summary   "List all libraries across sources (paginated)"
            :parameters {:query s/PaginationQuery}
            :responses {200 {:body s/PaginatedMediaLibraries}}
            :handler   (med/list-all-libraries-handler ctx)}}]
   ["/api/media/sources/:id/libraries"
    {:tags       ["media"]
     :get  {:summary   "List libraries for a media source"
            :parameters {:path [:map [:id s/MediaSourceId]]}
            :responses {200 {:body [:vector s/MediaLibrary]}}
            :handler   (med/list-libraries-handler ctx)}
     :post {:summary    "Create a library under a media source"
            :parameters {:path [:map [:id s/MediaSourceId]]
                         :body s/MediaLibraryCreate}
            :responses  {201 {:body s/MediaLibrary}
                         400 {:body s/CoercionError}}
            :handler    (med/create-library-handler ctx)}}]
   ["/api/media/sources/:id/libraries/discover"
    {:tags       ["media"]
     :post {:summary   "Discover libraries from a remote source"
            :parameters {:path [:map [:id s/MediaSourceId]]}
            :responses {201 {:body s/DiscoveryResult}
                        400 {:body s/APIError}
                        404 {:body s/APIError}}
            :handler   (med/discover-libraries-handler ctx)}}]
   ["/api/media/libraries/:id"
    {:tags       ["media"]
     :parameters {:path [:map [:id s/LibraryId]]}
     :put    {:summary    "Update a library"
              :parameters {:body s/MediaLibraryUpdate}
              :responses  {200 {:body s/MediaLibrary}
                           404 {:body s/APIError}
                           400 {:body s/CoercionError}}
              :handler    (med/update-library-handler ctx)}
     :delete {:summary   "Delete a library"
              :parameters {:path [:map [:id s/LibraryId]]}
              :responses {204 {}}
              :handler   (med/delete-library-handler ctx)}}]
   ["/api/media/libraries/:id/paths"
    {:tags       ["media"]
     :parameters {:path [:map [:id s/LibraryId]]}
     :get  {:summary   "List paths for a library"
            :responses {200 {:body [:vector s/LibraryPath]}}
            :handler   (med/list-library-paths-handler ctx)}
     :post {:summary    "Add a path to a library"
            :parameters {:body s/LibraryPathCreate}
            :responses  {201 {:body s/LibraryPath}
                         400 {:body s/CoercionError}}
            :handler    (med/create-library-path-handler ctx)}}]
   ["/api/media/libraries/:id/paths/:path-id"
    {:tags       ["media"]
     :parameters {:path [:map [:id s/LibraryId] [:path-id s/LibraryPathId]]}
     :delete {:summary   "Remove a path from a library"
              :responses {204 {}}
              :handler   (med/delete-library-path-handler ctx)}}]
   ["/api/media/libraries/:id/items"
    {:tags       ["media"]
     :parameters {:path  [:map [:id s/LibraryId]]
                  :query [:map
                          [:limit      {:optional true} [:int {:min 1 :max 1000 :description "Maximum number of items to return (default varies by endpoint)"}]]
                          [:offset     {:optional true} [:int {:min 0 :description "Number of items to skip (default: 0)"}]]
                          [:attrs      {:optional true} :string]
                          [:type       {:optional true} :string]
                          [:parent-id  {:optional true} :int]
                          [:search     {:optional true} [:string {:description "Case-insensitive substring filter on the item title"}]]]}
     :get {:summary   "List media items in a library (paginated)"
           :responses {200 {:body s/PaginatedMediaItems}}
           :handler   (med/list-library-items-handler ctx)}}]
   ["/api/media/libraries/:id/scan"
    {:tags       ["media"]
     :post {:summary   "Trigger an asynchronous library scan"
            :parameters {:path [:map [:id s/LibraryId]]}
            :responses {202 {:body s/ScanTriggerResult}
                        404 {:body s/APIError}}
            :handler   (med/trigger-scan-handler ctx)}}]
   ["/api/media/items/:id"
    {:tags       ["media"]
     :get {:summary   "Get a media item (by Jellyfin/remote id or internal id)"
           :parameters {:path [:map [:id s/MediaItemRef]]}
           :responses {200 {:body s/MediaItem}
                       404 {:body s/APIError}}
           :handler   (med/get-media-item-handler ctx)}}]
   ["/api/media/items/:id/playback-url"
    {:tags       ["media"]
     :get {:summary   "Resolve a direct playback URL for a media item"
           :parameters {:path [:map [:id s/MediaItemRef]]}
           :responses {200 {:body s/PlaybackUrl}
                       404 {:body s/APIError}
                       422 {:body s/APIError}}
           :handler   (med/get-item-playback-url-handler ctx)}}]
    ["/api/media/items/:id/stream"
     {:tags       ["media"]
      :get {:summary "Proxy a media item's stream from its source"
            :parameters {:path [:map [:id s/MediaItemRef]]}
            :handler (med/redirect-to-stream-handler ctx)}}]
    ["/api/media/items/:id/children"
     {:tags       ["media"]
      :get {:summary "List children of a media item (seasons/episodes)"
            :parameters {:path [:map [:id s/MediaItemRef]]
                         :query [:map
                                 [:limit  {:optional true} [:int {:min 1 :max 1000}]]
                                 [:offset {:optional true} [:int {:min 0}]]
                                 [:search {:optional true} :string]]}
            :responses {200 {:body s/PaginatedMediaItems}
                        404 {:body s/APIError}}
            :handler (med/get-media-item-children-handler ctx)}}]
    ["/api/media/collections"
    {:tags ["media"]
     :get  {:summary   "List collections (paginated)"
            :parameters {:query s/PaginationQuery}
            :responses {200 {:body s/PaginatedCollections}}
            :handler   (med/list-collections-handler ctx)}
     :post {:summary    "Create a collection"
            :parameters {:body s/CollectionCreate}
            :responses  {201 {:body s/Collection}
                         400 {:body s/CoercionError}}
            :handler    (med/create-collection-handler ctx)}}]
   ["/api/media/collections/:id"
    {:tags       ["media"]
     :parameters {:path [:map [:id s/CollectionId]]}
     :get    {:summary   "Get a collection"
              :responses {200 {:body s/Collection}
                          404 {:body s/APIError}}
              :handler   (med/get-collection-handler ctx)}
     :put    {:summary    "Update a collection"
              :parameters {:body s/CollectionUpdate}
              :responses  {200 {:body s/Collection}
                           404 {:body s/APIError}
                           400 {:body s/CoercionError}}
              :handler    (med/update-collection-handler ctx)}
     :delete {:summary   "Delete a collection"
              :responses {204 {}}
              :handler   (med/delete-collection-handler ctx)}}]
   ["/api/media/collections/:id/items"
    {:tags       ["media"]
     :parameters {:path [:map [:id s/CollectionId]]}
     :get  {:summary   "List items in a manual collection"
            :responses {200 {:body [:vector s/CollectionItem]}}
            :handler   (med/list-collection-items-handler ctx)}
     :post {:summary    "Add a media item to a manual collection"
            :parameters {:body s/CollectionItemAdd}
            :responses  {204 {}}
            :handler    (med/add-collection-item-handler ctx)}}]
    ["/api/media/collections/:id/items/:item-id"
     {:tags       ["media"]
      :parameters {:path [:map [:id s/CollectionId] [:item-id s/MediaItemRef]]}
      :delete {:summary   "Remove a media item from a collection"
               :responses {204 {}}
               :handler   (med/remove-collection-item-handler ctx)}}]

    ;; ── Catalog ─────────────────────────────────────────────────────────────
    ["/api/catalog/aggregate"
     {:tags ["catalog"]
      :get  {:summary    "Catalog aggregate profile for Tunabrain scheduling"
             :description "Returns a summarized view of the library: show counts, genre breakdowns, runtime histogram."
             :parameters {:query [:map
                                  [:channel {:optional true} :string]
                                  [:tag     {:optional true} :string]]}
             :responses  {200 {:body s/CatalogProfile}}
             :handler    (catalog/catalog-aggregate-handler ctx)}}]
    ["/api/catalog/count"
     {:tags ["catalog"]
      :post {:summary    "Count media items matching filters (Phase 7 stub)"
             :parameters {:body s/CatalogCountRequest}
             :responses  {200 {:body s/CatalogCountResponse}}
             :handler    (catalog/catalog-count-handler ctx)}}]

    ;; ── Daily Slots (Tunabrain expander output) ────────────────────────────
    ["/api/channels/:channel-id/daily-slots"
     {:tags       ["daily-slots"]
      :parameters {:path [:map [:channel-id s/ChannelId]]}
      :post       {:summary    "Ingest expanded DailySlot[] from Tunarr Scheduler"
                   :description "Resolves media_id + media_selection_strategy into concrete playout events for the channel."
                   :parameters {:body [:vector s/DailySlot]}
                   :responses  {200 {:body s/DailySlotIngestResult}
                                404 {:body s/APIError}}
                   :handler    (ds/ingest-daily-slots-handler ctx)}}]

    ;; ── Filler Presets ───────────────────────────────────────────────────────
   ["/api/filler-presets"
    {:tags ["filler"]
     :get  {:summary    "List filler presets (paginated)"
            :parameters {:query s/PaginationQuery}
            :responses  {200 {:body s/PaginatedFillerPresets}}
            :handler    (filler/list-presets-handler ctx)}
     :post {:summary    "Create a filler preset"
            :parameters {:body s/FillerPresetCreate}
            :responses  {201 {:body s/FillerPreset}
                         400 {:body s/CoercionError}}
            :handler    (filler/create-preset-handler ctx)}}]
   ["/api/filler-presets/:id"
    {:tags       ["filler"]
     :parameters {:path [:map [:id s/FillerPresetId]]}
     :get    {:summary   "Get a filler preset"
              :responses {200 {:body s/FillerPreset}
                          404 {:body s/APIError}}
              :handler   (filler/get-preset-handler ctx)}
     :put    {:summary    "Update a filler preset"
              :parameters {:body s/FillerPresetUpdate}
              :responses  {200 {:body s/FillerPreset}
                           404 {:body s/APIError}
                           400 {:body s/CoercionError}}
              :handler    (filler/update-preset-handler ctx)}
     :delete {:summary   "Delete a filler preset"
              :responses {204 {}}
              :handler   (filler/delete-preset-handler ctx)}}]

   ;; ── Tags ────────────────────────────────────────────────────────────────
   ["/api/tags"
    {:tags ["tags"]
     :get {:summary   "List all tags with usage counts (paginated)"
           :parameters {:query s/PaginationQuery}
           :responses {200 {:body s/PaginatedTags}}
           :handler   (tags/list-all-tags-handler ctx)}}]
   ["/api/media-items/:id/tags"
    {:tags       ["tags"]
     :get  {:summary   "List tags on a media item"
            :parameters {:path [:map [:id s/MediaItemId]]}
            :responses {200 {:body [:vector s/TagName]}}
            :handler   (tags/get-tags-handler ctx)}
     :post {:summary    "Add tags to a media item"
            :parameters {:path [:map [:id s/MediaItemId]]
                         :body s/TagCreate}
            :responses  {200 {:body s/TagAddResult}
                         400 {:body s/APIError}
                         404 {:body s/APIError}}
            :handler    (tags/add-tags-handler ctx)}}]
   ["/api/media-items/:id/tags/:tag"
    {:tags       ["tags"]
     :delete {:summary   "Remove a tag from a media item"
              :parameters {:path [:map
                                  [:id  s/MediaItemId]
                                  [:tag s/TagName]]}
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
     :get    {:summary   "Get an FFmpeg profile"
              :parameters {:path [:map [:id s/FFmpegProfileId]]}
              :responses {200 {:body s/FFmpegProfile}
                          404 {:body s/APIError}}
              :handler   (ffmpeg/get-profile-handler ctx)}
      :put    {:summary    "Update an FFmpeg profile"
               :parameters {:path [:map [:id s/FFmpegProfileId]]
                            :body s/FFmpegProfileUpdate}
               :responses  {200 {:body s/FFmpegProfile}
                           404 {:body s/APIError}
                           400 {:body s/APIError}}
               :handler    (ffmpeg/update-profile-handler ctx)}
      :patch  {:summary    "Partially update an FFmpeg profile"
               :parameters {:path [:map [:id s/FFmpegProfileId]]
                            :body s/FFmpegProfileUpdate}
               :responses  {200 {:body s/FFmpegProfile}
                           404 {:body s/APIError}
                           400 {:body s/APIError}}
               :handler    (ffmpeg/update-profile-handler ctx)}
      :delete {:summary   "Delete an FFmpeg profile"
              :parameters {:path [:map [:id s/FFmpegProfileId]]}
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
     :delete {:summary "Delete a test channel"
              :parameters {:path [:map [:identifier :string]]}
              :handler (test/delete-test-channel-handler ctx)}}]
   ["/api/test/channels/:identifier/artwork"
    {:tags       ["test"]
     :post {:summary "Attach a generated test logo to a channel"
            :parameters {:path [:map [:identifier :string]]}
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
     :get        {:summary "HLS playlist for a channel (starts FFmpeg if needed)"
                  :parameters {:path [:map [:uuid :uuid]]}
                  :handler (streaming/stream-handler ctx)}}]
   ["/stream/:uuid/:segment"
    {:tags       ["streaming"]
     :get        {:summary "HLS transport-stream segment"
                  :parameters {:path [:map [:uuid :uuid] [:segment :string]]}
                  :handler (streaming/segment-handler ctx)}}]

   ;; ── Artwork ─────────────────────────────────────────────────────────────
   ["/logos/:uuid"
    {:tags       ["artwork"]
     :get        {:summary "Channel logo/artwork by channel UUID"
                  :parameters {:path [:map [:uuid :uuid]]}
                  :handler (logos/logos-handler ctx)}}]

   ;; ── Debug ───────────────────────────────────────────────────────────────
   ["/api/debug/stream/:uuid"
    {:tags       ["debug"]
     :get        {:summary  "Diagnostic information for a running HLS stream"
                  :parameters {:path [:map [:uuid :uuid]]}
                  :handler  (streaming/stream-debug-handler ctx)}}]

   ;; ── Metrics ─────────────────────────────────────────────────────────────
   ["/api/metrics/channels"
    {:tags ["metrics"]
     :get  {:summary    "List channel view events (cursor-paginated)"
            :parameters {:query [:map
                                 [:limit       {:optional true} [:int {:min 1 :max 1000 :description "Maximum number of items to return"}]]
                                 [:cursor      {:optional true} [:string {:description "Opaque cursor for fetching next page"}]]
                                 [:channel-id  {:optional true} :string]
                                 [:from        {:optional true} :string]
                                 [:to          {:optional true} :string]]}
            :responses  {200 {:body [:map
                                     [:items [:vector :any]]
                                     [:pagination s/CursorPaginationMeta]]}}
            :handler    (metrics/list-channel-views-handler ctx)}}]
   ["/api/metrics/media-items"
    {:tags ["metrics"]
     :get  {:summary    "List media item view events with percent_watched (cursor-paginated)"
            :parameters {:query [:map
                                 [:limit          {:optional true} [:int {:min 1 :max 1000 :description "Maximum number of items to return"}]]
                                 [:cursor         {:optional true} [:string {:description "Opaque cursor for fetching next page"}]]
                                 [:channel-id     {:optional true} :string]
                                 [:media-item-id  {:optional true} :string]
                                 [:from           {:optional true} :string]
                                 [:to             {:optional true} :string]]}
            :responses  {200 {:body [:map
                                     [:items [:vector :any]]
                                     [:pagination s/CursorPaginationMeta]]}}
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
                                        rrc/coerce-response-middleware
                                        ;; Innermost: render java.time values to
                                        ;; wire strings before response coercion
                                        ;; validates the body against the schema.
                                        mw/normalize-temporals-middleware]}})
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
  [{:keys [port db ffmpeg media scheduling daily-slots streams grout jobs] :as _opts}]
  (let [ctx     {:db db :ffmpeg ffmpeg :media media :scheduling scheduling
                 :daily-slots daily-slots :streams streams :grout grout :jobs jobs}
        handler (make-handler ctx)]
    (jetty/run-jetty handler {:port port :join? false})))

(defn stop-server! [server]
  (.stop server))
