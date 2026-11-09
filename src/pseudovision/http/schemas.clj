(ns pseudovision.http.schemas
  "Malli schemas for request/response coercion and OpenAPI generation.

   Response schemas are `open maps` (malli's default) so new DB columns flow
   through automatically without silently stripping data. Create/Update request
   schemas are also open; tighten with `:closed` per-field once every caller is
   known.")

;; ---------------------------------------------------------------------------
;; Primitives
;; ---------------------------------------------------------------------------

(def ChannelId [:int {:min 1}])

(def ChannelNumber
  [:string {:min 1 :description "Channel number, e.g. \"2\" or \"4.1\""}])

;; Canonical lowercase key used to resolve a channel's content via the
;; `channel:<slug>` tag in metadata_tags.  Format constraints are
;; enforced at the DB layer (the migration adds a CHECK on the column
;; pattern if needed) and by the loud-error contract in
;; `scheduling.core/load-items` — see PR #145.
(def ChannelSlug
  [:string {:min 1
            :max 64
            :description "Canonical lowercase slug; matches metadata_tags.name = 'channel:<slug>'."}])

(def SortNumber
  [:double {:description "Numeric sort order; derived from :number if omitted on create"}])

(def StreamingMode
  [:enum {:description "Output stream format"} "ts" "ts_hybrid" "hls_direct" "hls_segmenter"])

(def Interval
  [:string {:description "PostgreSQL interval (e.g. \"02:00:00\" or \"1 hour 30 minutes\")"}])

(def Instant
  [:string {:description "ISO-8601 timestamp, e.g. \"2026-04-22T18:30:00Z\""}])

;; ---------------------------------------------------------------------------
;; Pagination
;; ---------------------------------------------------------------------------

(def PaginationQuery
  "Query parameters for offset-based pagination."
  [:map
   [:limit  {:optional true} [:int {:min 1 :max 1000 :description "Maximum number of items to return (default varies by endpoint)"}]]
   [:offset {:optional true} [:int {:min 0 :description "Number of items to skip (default: 0)"}]]])

(def TagFacetQuery
  "Query parameters for GET /api/tags: pagination plus optional facet filters."
  [:map
   [:limit      {:optional true} [:int {:min 1 :max 1000 :description "Maximum number of tags to return (default 100)"}]]
   [:offset     {:optional true} [:int {:min 0 :description "Number of tags to skip (default: 0)"}]]
   [:prefix     {:optional true} [:string {:description "Only tags starting with this prefix, case-insensitive (e.g. channel: or genre:)"}]]
   [:library-id {:optional true} [:int {:description "Only count tags on media items in this library"}]]
   [:kind       {:optional true} [:string {:description "Only count tags on media items of this kind (e.g. program, episode, movie)"}]]])

(def CursorPaginationQuery
  "Query parameters for cursor-based pagination (time-series data)."
  [:map
   [:limit  {:optional true} [:int {:min 1 :max 1000 :description "Maximum number of items to return"}]]
   [:cursor {:optional true} [:string {:description "Opaque cursor for fetching next page"}]]])

(def PaginationMeta
  "Metadata for paginated responses."
  [:map
   [:limit     :int]
   [:offset    :int]
   [:total     :int]
   [:has_more  :boolean]])

(def CursorPaginationMeta
  "Metadata for cursor-based paginated responses."
  [:map
   [:limit       :int]
   [:has_more    :boolean]
   [:next_cursor {:optional true} [:maybe :string]]])

;; ---------------------------------------------------------------------------
;; Error envelope
;; ---------------------------------------------------------------------------

(def APIError
  [:map
   [:error :string]])

(def CoercionError
  "Structured 400 returned when request coercion fails."
  [:map
   [:error    :string]
   [:in       [:vector :any]]
   [:humanized {:optional true} :any]])

;; ---------------------------------------------------------------------------
;; Channel — GET/POST/PUT response shape (unqualified keys)
;; ---------------------------------------------------------------------------

(def Channel
  [:map
   [:id                             ChannelId]
   [:uuid                           :uuid]
   [:number                         ChannelNumber]
   [:sort-number                    SortNumber]
   [:name                           :string]
   [:slug                           :string]
   [:description                    {:optional true} [:maybe :string]]
   [:group-name                     {:optional true} [:maybe :string]]
   [:categories                     {:optional true} [:maybe :string]]
   [:streaming-mode                 StreamingMode]
   [:ffmpeg-profile-id              :int]
   [:watermark-id                   {:optional true} [:maybe :int]]
   [:fallback-filler-id             {:optional true} [:maybe :int]]
   [:preferred-audio-language       {:optional true} [:maybe :string]]
   [:preferred-audio-title          {:optional true} [:maybe :string]]
   [:preferred-subtitle-language    {:optional true} [:maybe :string]]
   [:subtitle-mode                  :string]
   [:subtitle-default-enabled       :boolean]
   [:music-video-credits-mode       {:optional true} [:maybe :string]]
   [:music-video-credits-template   {:optional true} [:maybe :string]]
   [:song-video-mode                {:optional true} [:maybe :string]]
   [:slug-seconds                   {:optional true} [:maybe :double]]
   [:stream-selector-mode           :string]
   [:stream-selector                {:optional true} [:maybe :string]]
   [:is-enabled                     :boolean]
   [:show-in-epg                    :boolean]])

(def PaginatedChannels
  "Paginated response for channels."
  [:map
   [:items [:vector Channel]]
   [:pagination PaginationMeta]])

;; ---------------------------------------------------------------------------
;; Channel — POST request body
;;
;; Only :number and :name are required; everything else has a DB default or
;; is nullable. :uuid may be provided to pin a stable identifier, else the DB
;; generates one. :sort-number is derived from :number if omitted.
;; ---------------------------------------------------------------------------

(def ChannelCreate
  [:map
   [:number                         ChannelNumber]
   [:name                           :string]
   ;; :slug is the canonical key that the channel-catalog fallback in
   ;; scheduling.core/load-items uses to resolve an `auto` slot's content
   ;; via the `channel:<slug>` dimension tag.  It is required at create
   ;; time so a channel can never get into the live cluster without a
   ;; curated slug — without one, the fallback throws ex-info on every
   ;; rebuild (see #145).
   [:slug                           ChannelSlug]
   [:uuid                           {:optional true} :uuid]
   [:sort-number                    {:optional true} SortNumber]
   [:group-name                     {:optional true} [:maybe :string]]
   [:categories                     {:optional true} [:maybe :string]]
   [:description                    {:optional true} [:maybe :string]]
   [:streaming-mode                 {:optional true} StreamingMode]
   [:ffmpeg-profile-id              {:optional true} :int]
   [:watermark-id                   {:optional true} [:maybe :int]]
   [:fallback-filler-id             {:optional true} [:maybe :int]]
   [:preferred-audio-language       {:optional true} [:maybe :string]]
   [:preferred-audio-title          {:optional true} [:maybe :string]]
   [:preferred-subtitle-language    {:optional true} [:maybe :string]]
   [:subtitle-mode                  {:optional true} :string]
   [:subtitle-default-enabled       {:optional true} :boolean]
   [:music-video-credits-mode       {:optional true} [:maybe :string]]
   [:music-video-credits-template   {:optional true} [:maybe :string]]
   [:song-video-mode                {:optional true} [:maybe :string]]
   [:slug-seconds                   {:optional true} [:maybe :double]]
   [:stream-selector-mode           {:optional true} :string]
   [:stream-selector                {:optional true} [:maybe :string]]
   [:is-enabled                     {:optional true} :boolean]
   [:show-in-epg                    {:optional true} :boolean]])

;; ---------------------------------------------------------------------------
;; Channel — PUT request body (all fields optional)
;; ---------------------------------------------------------------------------

(def ChannelUpdate
  [:map
   [:number                         {:optional true} ChannelNumber]
   [:name                           {:optional true} :string]
   [:slug                           {:optional true} ChannelSlug]
   [:sort-number                    {:optional true} SortNumber]
   [:group-name                     {:optional true} [:maybe :string]]
   [:categories                     {:optional true} [:maybe :string]]
   [:description                    {:optional true} [:maybe :string]]
   [:streaming-mode                 {:optional true} StreamingMode]
   [:ffmpeg-profile-id              {:optional true} :int]
   [:watermark-id                   {:optional true} [:maybe :int]]
   [:fallback-filler-id             {:optional true} [:maybe :int]]
   [:preferred-audio-language       {:optional true} [:maybe :string]]
   [:preferred-audio-title          {:optional true} [:maybe :string]]
   [:preferred-subtitle-language    {:optional true} [:maybe :string]]
   [:subtitle-mode                  {:optional true} :string]
   [:subtitle-default-enabled       {:optional true} :boolean]
   [:music-video-credits-mode       {:optional true} [:maybe :string]]
   [:music-video-credits-template   {:optional true} [:maybe :string]]
   [:song-video-mode                {:optional true} [:maybe :string]]
   [:slug-seconds                   {:optional true} [:maybe :double]]
   [:stream-selector-mode           {:optional true} :string]
   [:stream-selector                {:optional true} [:maybe :string]]
   [:is-enabled                     {:optional true} :boolean]
   [:show-in-epg                    {:optional true} :boolean]])

;; ---------------------------------------------------------------------------
;; Schedules & Slots
;; ---------------------------------------------------------------------------

(def ScheduleId [:int {:min 1}])
(def SlotId     [:int {:min 1}])

;; Media-item ids are shared across the schedule, collection, and media APIs,
;; so they are defined here, ahead of their first use.
(def MediaItemId [:int {:min 1}])

(def MediaItemRef
  "A media-item reference accepted by the item endpoints. Either the remote
   item id (e.g. the Jellyfin item id) — the preferred, public identifier — or
   the internal integer id, which is mostly an implementation detail. Numeric
   refs are treated as internal ids; everything else as a remote_key."
  [:or MediaItemId [:string {:min 1}]])

(def FixedStartTimeBehavior
  [:enum {:description "How to handle a fixed-start slot whose start time has passed"}
   "skip" "play"])

(def Schedule
  [:map
   [:id                         ScheduleId]
   [:name                       :string]
   [:fixed-start-time-behavior  {:optional true} FixedStartTimeBehavior]
   [:shuffle-slots              :boolean]
   [:random-start-point         :boolean]
   [:keep-multi-part-together   :boolean]
   [:treat-collections-as-shows :boolean]])

(def PaginatedSchedules
  "Paginated response for schedules."
  [:map
   [:items [:vector Schedule]]
   [:pagination PaginationMeta]])

(def ScheduleCreate
  [:map
   [:name                       :string]
   [:fixed-start-time-behavior  {:optional true} FixedStartTimeBehavior]
   [:shuffle-slots              {:optional true} :boolean]
   [:random-start-point         {:optional true} :boolean]
   [:keep-multi-part-together   {:optional true} :boolean]
   [:treat-collections-as-shows {:optional true} :boolean]])

(def ScheduleUpdate
  [:map
   [:name                       {:optional true} :string]
   [:fixed-start-time-behavior  {:optional true} FixedStartTimeBehavior]
   [:shuffle-slots              {:optional true} :boolean]
   [:random-start-point         {:optional true} :boolean]
   [:keep-multi-part-together   {:optional true} :boolean]
   [:treat-collections-as-shows {:optional true} :boolean]])

(def SlotAnchor   [:enum "fixed" "sequential"])
(def SlotFillMode [:enum "once" "count" "block" "flood"])
(def PlaybackOrder
  [:enum
   "chronological" "random" "shuffle" "shuffle_in_order"
   "multi_episode_shuffle" "season_episode" "random_rotation" "marathon"])
(def GuideMode   [:enum "normal" "filler"])
(def SubtitleModeEnum
  [:enum "none" "any" "forced_only" "default_only" "burn_in"])
(def TailMode    [:enum "none" "filler" "offline"])

(def TagList
  [:vector {:description "Metadata tag names"} :string])

(def DaysOfWeek
  "Bitmask: Mon=1 Tue=2 Wed=4 Thu=8 Fri=16 Sat=32 Sun=64. Default 127 = every day."
  [:int {:min 0 :max 127
         :description "Day-of-week bitmask. Mon=1 Tue=2 Wed=4 Thu=8 Fri=16 Sat=32 Sun=64. 127 = every day."}])

(def Slot
  [:map
   [:id                         SlotId]
   [:schedule-id                ScheduleId]
   [:slot-index                 :int]
   [:anchor                     {:optional true} SlotAnchor]
   [:start-time                 {:optional true} [:maybe Interval]]
   [:fill-mode                  {:optional true} SlotFillMode]
   [:item-count                 {:optional true} [:maybe :int]]
   [:block-duration             {:optional true} [:maybe Interval]]
   [:tail-mode                  {:optional true} TailMode]
   [:discard-to-fill-attempts   {:optional true} :int]
   [:collection-id              {:optional true} [:maybe :int]]
   [:media-item-id              {:optional true} [:maybe :int]]
   [:playback-order             {:optional true} PlaybackOrder]
   [:marathon-group-by          {:optional true} [:maybe :string]]
   [:marathon-shuffle-groups    {:optional true} :boolean]
   [:marathon-shuffle-items     {:optional true} :boolean]
   [:marathon-batch-size        {:optional true} [:maybe :int]]
   [:guide-mode                 {:optional true} GuideMode]
   [:custom-title               {:optional true} [:maybe :string]]
   [:pre-filler-id              {:optional true} [:maybe :int]]
   [:mid-filler-id              {:optional true} [:maybe :int]]
   [:post-filler-id             {:optional true} [:maybe :int]]
   [:tail-filler-id             {:optional true} [:maybe :int]]
   [:fallback-filler-id         {:optional true} [:maybe :int]]
   [:watermark-id               {:optional true} [:maybe :int]]
   [:disable-watermarks         {:optional true} :boolean]
   [:preferred-audio-language   {:optional true} [:maybe :string]]
   [:preferred-audio-title      {:optional true} [:maybe :string]]
   [:preferred-subtitle-language {:optional true} [:maybe :string]]
   [:subtitle-mode              {:optional true} [:maybe SubtitleModeEnum]]
   [:fill-with-group-mode       {:optional true} :string]
   [:required-tags              {:optional true} TagList]
   [:excluded-tags              {:optional true} TagList]
   [:days-of-week               {:optional true} DaysOfWeek]])

(def PaginatedSlots
  "Paginated response for schedule slots."
  [:map
   [:items [:vector Slot]]
   [:pagination PaginationMeta]])

(def SlotCreate
  [:map
   [:slot-index                 :int]
   [:anchor                     {:optional true} SlotAnchor]
   [:start-time                 {:optional true} [:maybe Interval]]
   [:fill-mode                  {:optional true} SlotFillMode]
   [:item-count                 {:optional true} [:maybe :int]]
   [:block-duration             {:optional true} [:maybe Interval]]
   [:tail-mode                  {:optional true} TailMode]
   [:discard-to-fill-attempts   {:optional true} :int]
   [:collection-id              {:optional true} [:maybe :int]]
   [:media-item-id              {:optional true} [:maybe MediaItemRef]]
   [:playback-order             {:optional true} PlaybackOrder]
   [:marathon-group-by          {:optional true} [:maybe :string]]
   [:marathon-shuffle-groups    {:optional true} :boolean]
   [:marathon-shuffle-items     {:optional true} :boolean]
   [:marathon-batch-size        {:optional true} [:maybe :int]]
   [:guide-mode                 {:optional true} GuideMode]
   [:custom-title               {:optional true} [:maybe :string]]
   [:pre-filler-id              {:optional true} [:maybe :int]]
   [:mid-filler-id              {:optional true} [:maybe :int]]
   [:post-filler-id             {:optional true} [:maybe :int]]
   [:tail-filler-id             {:optional true} [:maybe :int]]
   [:fallback-filler-id         {:optional true} [:maybe :int]]
   [:watermark-id               {:optional true} [:maybe :int]]
   [:disable-watermarks         {:optional true} :boolean]
   [:preferred-audio-language   {:optional true} [:maybe :string]]
   [:preferred-audio-title      {:optional true} [:maybe :string]]
   [:preferred-subtitle-language {:optional true} [:maybe :string]]
   [:subtitle-mode              {:optional true} [:maybe SubtitleModeEnum]]
   [:fill-with-group-mode       {:optional true} :string]
   [:required-tags              {:optional true} TagList]
   [:excluded-tags              {:optional true} TagList]
   [:days-of-week               {:optional true} DaysOfWeek]])

(def SlotUpdate
  [:map
   [:slot-index                 {:optional true} :int]
   [:anchor                     {:optional true} SlotAnchor]
   [:start-time                 {:optional true} [:maybe Interval]]
   [:fill-mode                  {:optional true} SlotFillMode]
   [:item-count                 {:optional true} [:maybe :int]]
   [:block-duration             {:optional true} [:maybe Interval]]
   [:tail-mode                  {:optional true} TailMode]
   [:discard-to-fill-attempts   {:optional true} :int]
   [:collection-id              {:optional true} [:maybe :int]]
   [:media-item-id              {:optional true} [:maybe MediaItemRef]]
   [:playback-order             {:optional true} PlaybackOrder]
   [:marathon-group-by          {:optional true} [:maybe :string]]
   [:marathon-shuffle-groups    {:optional true} :boolean]
   [:marathon-shuffle-items     {:optional true} :boolean]
   [:marathon-batch-size        {:optional true} [:maybe :int]]
   [:guide-mode                 {:optional true} GuideMode]
   [:custom-title               {:optional true} [:maybe :string]]
   [:pre-filler-id              {:optional true} [:maybe :int]]
   [:mid-filler-id              {:optional true} [:maybe :int]]
   [:post-filler-id             {:optional true} [:maybe :int]]
   [:tail-filler-id             {:optional true} [:maybe :int]]
   [:fallback-filler-id         {:optional true} [:maybe :int]]
   [:watermark-id               {:optional true} [:maybe :int]]
   [:disable-watermarks         {:optional true} :boolean]
   [:preferred-audio-language   {:optional true} [:maybe :string]]
   [:preferred-audio-title      {:optional true} [:maybe :string]]
   [:preferred-subtitle-language {:optional true} [:maybe :string]]
   [:subtitle-mode              {:optional true} [:maybe SubtitleModeEnum]]
   [:fill-with-group-mode       {:optional true} :string]
   [:required-tags              {:optional true} TagList]
   [:excluded-tags              {:optional true} TagList]
   [:days-of-week               {:optional true} DaysOfWeek]])

;; ---------------------------------------------------------------------------
;; Filler presets
;; ---------------------------------------------------------------------------

(def FillerPresetId [:int {:min 1}])

(def FillerRole
  [:enum {:description "Where this preset is injected"}
   "pre" "mid" "post" "pad" "tail" "fallback"])

(def FillerMode
  [:enum {:description "How the fill duration/count is specified"}
   "duration" "count" "random_count" "pad_to_minute"])

(def FillerCategory
  [:enum {:description "Content category for filler items"}
   "commercial" "promo" "bumper" "short" "documentary"
   "music_video" "countdown" "credit_roll" "trailer" "interstitial" "other"])

(def FillerPreset
  [:map
   [:id                    FillerPresetId]
   [:name                  :string]
   [:role                  FillerRole]
   [:mode                  FillerMode]
   [:category              {:optional true} FillerCategory]
   [:duration              {:optional true} [:maybe Interval]]
   [:count                 {:optional true} [:maybe :int]]
   [:pad-to-nearest-minute {:optional true} [:maybe :int]]
   [:allow-watermarks      {:optional true} :boolean]
   [:use-chapters-as-items {:optional true} :boolean]
   [:collection-id         {:optional true} [:maybe :int]]
   [:media-item-id         {:optional true} [:maybe :int]]
   [:grout-tags            {:optional true} [:maybe [:vector :string]]]])

(def PaginatedFillerPresets
  [:map
   [:items [:vector FillerPreset]]
   [:pagination PaginationMeta]])

(def FillerPresetCreate
  [:map
   [:name                  :string]
   [:role                  FillerRole]
   [:mode                  FillerMode]
   [:category              {:optional true} FillerCategory]
   [:duration              {:optional true} Interval]
   [:count                 {:optional true} :int]
   [:pad-to-nearest-minute {:optional true} :int]
   [:allow-watermarks      {:optional true} :boolean]
   [:use-chapters-as-items {:optional true} :boolean]
   [:collection-id         {:optional true} :int]
   [:media-item-id         {:optional true} MediaItemRef]
   [:grout-tags            {:optional true} [:vector :string]]])

(def FillerPresetUpdate
  [:map
   [:name                  {:optional true} :string]
   [:role                  {:optional true} FillerRole]
   [:mode                  {:optional true} FillerMode]
   [:category              {:optional true} FillerCategory]
   [:duration              {:optional true} [:maybe Interval]]
   [:count                 {:optional true} [:maybe :int]]
   [:pad-to-nearest-minute {:optional true} [:maybe :int]]
   [:allow-watermarks      {:optional true} :boolean]
   [:use-chapters-as-items {:optional true} :boolean]
   [:collection-id         {:optional true} [:maybe :int]]
   [:media-item-id         {:optional true} [:maybe MediaItemRef]]
   [:grout-tags            {:optional true} [:maybe [:vector :string]]]])

;; ---------------------------------------------------------------------------
;; Slot reorder
;; ---------------------------------------------------------------------------

(def SlotReorderRequest
  [:map
   [:slot-ids [:vector SlotId]]])

(def SlotReorderResult
  [:map
   [:slots [:vector Slot]]])

;; ---------------------------------------------------------------------------
;; FFmpeg profiles
;; ---------------------------------------------------------------------------

(def FFmpegProfileId [:int {:min 1}])

(def FFmpegProfileConfig
  "Encoding parameters. Keys evolve with FFmpeg, so the map is left open: the
   legacy flat shape ({:video-codec …}) and the nested hwaccel-aware shape both
   validate. The optional keys below document the nested shape used by
   `pseudovision.ffmpeg.profile`."
  [:map {:description "Encoder configuration (legacy flat or nested hwaccel-aware)"}
   [:accel {:optional true} [:enum "none" "nvenc" "vaapi"]]
   [:decode {:optional true} [:enum "auto" "software" "hardware"]]
   [:device {:optional true} :string]
   [:video {:optional true}
    [:map
     [:codec {:optional true} :string]
     [:bitrate {:optional true} :string]
     [:rate-control {:optional true} :string]
     [:preset {:optional true} :string]]]
   [:audio {:optional true}
    [:map
     [:codec {:optional true} :string]
     [:bitrate {:optional true} :string]
     [:sample-rate {:optional true} :int]
     [:channels {:optional true} :int]]]
   [:normalize {:optional true}
    [:map
     [:width {:optional true} :int]
     [:height {:optional true} :int]
     [:fps {:optional true} :int]
     [:pixfmt {:optional true} :string]
     [:sar {:optional true} :string]]]
   [:hls {:optional true}
    [:map
     [:segment-duration {:optional true} :int]
     [:playlist-size {:optional true} :int]
     [:warm-segments {:optional true} :int]
     [:initial-burst {:optional true} :int]]]])

(def FFmpegProfile
  [:map
   [:id     FFmpegProfileId]
   [:name   :string]
   [:config {:optional true} [:maybe FFmpegProfileConfig]]])

(def FFmpegProfileCreate
  [:map
   [:name   :string]
   [:config {:optional true} FFmpegProfileConfig]])

(def FFmpegProfileUpdate
  [:map
   [:name   {:optional true} :string]
   [:config {:optional true} FFmpegProfileConfig]])

(def FFmpegProfileDeleted
  [:map
   [:deleted [:= true]]
   [:profile FFmpegProfile]])

;; ---------------------------------------------------------------------------
;; Tags
;; ---------------------------------------------------------------------------

(def TagName [:string {:min 1 :description "Tag name"}])

(def TagCreate
  [:map
   [:tags   [:vector TagName]]
   [:source {:optional true} :string]])

(def TagUsage
  [:map
   [:name  TagName]
   [:count :int]])

(def PaginatedTags
  "Paginated response for tags."
  [:map
   [:items [:vector TagUsage]]
   [:pagination PaginationMeta]])

(def TagAddResult
  [:map
   [:item-id    :int]
   [:tags-added [:vector TagName]]])

;; ---------------------------------------------------------------------------
;; Media — sources, libraries, items, collections
;; ---------------------------------------------------------------------------

(def MediaSourceId [:int {:min 1}])
(def LibraryId     [:int {:min 1}])
(def CollectionId  [:int {:min 1}])

(def MediaSourceKind
  [:enum {:description "Media source backend"} "local" "plex" "jellyfin" "emby"])

(def LibraryKind
  [:enum "movies" "shows" "music_videos" "other_videos" "songs" "images"])

(def MediaItemKind
  [:enum "movie" "show" "season" "episode"
         "artist" "music_video" "other_video" "song" "image"])

(def CollectionKind
  [:enum "manual" "smart" "playlist" "multi" "trakt" "rerun"])

(def MediaSource
  [:map
   [:id                    MediaSourceId]
   [:name                  :string]
   [:kind                  MediaSourceKind]
   [:connection-config     {:optional true} [:maybe [:map]]]
   [:path-replacements     {:optional true} [:maybe [:sequential [:map]]]]
   [:last-collections-scan {:optional true} [:maybe Instant]]])

(def PaginatedMediaSources
  "Paginated response for media sources."
  [:map
   [:items [:vector MediaSource]]
   [:pagination PaginationMeta]])

(def MediaSourceCreate
  [:map
   [:name               :string]
   [:kind               MediaSourceKind]
   [:connection-config  {:optional true} [:map]]
   [:path-replacements  {:optional true} [:sequential [:map]]]])

(def MediaSourceUpdate
  "Partial update. :api-key and :connections are merged into the source's
   existing connection_config, so a key can be rotated without resending
   the connections (and vice versa)."
  [:map
   [:name               {:optional true} :string]
   [:api-key            {:optional true} :string]
   [:connections        {:optional true} [:sequential [:map]]]
   [:path-replacements  {:optional true} [:sequential [:map]]]])

(def MediaLibrary
  [:map
   [:id              LibraryId]
   [:media-source-id MediaSourceId]
   [:name            :string]
   [:kind            LibraryKind]
   [:external-id     {:optional true} [:maybe :string]]
   [:should-sync     {:optional true} :boolean]
   [:last-scan       {:optional true} [:maybe Instant]]])

(def PaginatedMediaLibraries
  "Paginated response for media libraries."
  [:map
   [:items [:vector MediaLibrary]]
   [:pagination PaginationMeta]])

(def MediaLibraryCreate
  [:map
   [:name         :string]
   [:kind         LibraryKind]
   [:external-id  {:optional true} :string]
   [:should-sync  {:optional true} :boolean]])

(def MediaLibraryUpdate
  [:map
   [:name        {:optional true} :string]
   [:should-sync {:optional true} :boolean]])

(def LibraryPathId [:int {:min 1}])

(def LibraryPath
  [:map
   [:id         LibraryPathId]
   [:library-id LibraryId]
   [:path       :string]])

(def LibraryPathCreate
  [:map
   [:path :string]])

(def MediaItem
  "Open — the list endpoint projects columns dynamically via the :attrs query.
   
   This schema allows any additional fields since the endpoint uses dynamic
   projection via the :attrs parameter."
  [:map
   [:id MediaItemId]
   [:remote-key {:optional true} [:maybe :string]]
   [:remote-etag {:optional true} [:maybe :string]]
   [:kind {:optional true} [:maybe :string]]
   [:state {:optional true} [:maybe :string]]
   [:name {:optional true} [:maybe :string]]
   [:year {:optional true} [:maybe :int]]
   [:parent-id {:optional true} [:maybe :int]]
   [:position {:optional true} [:maybe :int]]])

(def PaginatedMediaItems
  "Paginated response for media items."
  [:map
   [:items [:vector MediaItem]]
   [:pagination PaginationMeta]])

(def DiscoveryResult
  [:map
   [:discovered :int]
   [:created    :int]
   [:libraries  [:sequential [:map]]]])

(def PlaybackUrl
  [:map
   [:url  [:maybe :string]]
   [:kind :string]])

(def Collection
  [:map
   [:id                        CollectionId]
   [:kind                      CollectionKind]
   [:name                      :string]
   [:use-custom-playback-order {:optional true} :boolean]
   ;; Open map — the response carries the same kind-specific :config shape
   ;; as the request (smart collections send {:query {:category ...
   ;; :channel-tag ...}}). The previous `[:map]` schema was a closed map
   ;; with no entries, which reitit-malli's response-body coercion
   ;; interpreted as "drop every key in :config" — silently turning every
   ;; GET /api/media/collections/{id} response into config: {} even when
   ;; the stored row has the real value. Verified 2026-07-14: PR #139
   ;; restored the SQL path so the DB now stores the real config, but
   ;; the API still returned `config: {}` because of this response-side
   ;; coercion. The request-side fix landed in PR #133 (CollectionCreate
   ;; + CollectionUpdate); the response-side fix mirrors it. See the
   ;; CollectionCreate docstring for the 2026-07-13 incident history.
   [:config                    {:optional true} [:map {:closed false}]]])

(def PaginatedCollections
  "Paginated response for collections."
  [:map
   [:items [:vector Collection]]
   [:pagination PaginationMeta]])

(def CollectionCreate
  [:map
   [:name                      :string]
   [:kind                      {:optional true} CollectionKind]
   [:use-custom-playback-order {:optional true} :boolean]
   ;; Open map — the body carries a kind-specific config (smart collections
   ;; send {:query {:category ... :channel-tag ...}}, manual/playlist/multi
   ;; have their own shapes). The previous `[:map]` schema was a closed map
   ;; with no entries, which reitit-malli's request-body coercion interpreted
   ;; as "drop every key in :config" — silently turning every create into
   ;; config: {}. Verified 2026-07-13: a POST with
   ;; {"config": {"query": {"category": "mystery", "channel-tag": "channel:enigma"}}}
   ;; arrived at db/media/create-collection! as {:config {}} and the row
   ;; stored '{}'::jsonb. Widening to `[:map {:closed false}]` matches the
   ;; convention used elsewhere in this file (e.g. lines 864, 870, 879, 883).
   [:config                    {:optional true} [:map {:closed false}]]])

(def CollectionUpdate
  [:map
   [:name                      {:optional true} :string]
   [:use-custom-playback-order {:optional true} :boolean]
   [:config                    {:optional true} [:map {:closed false}]]])

(def CollectionItem
  [:map
   [:collection-id :int]
   [:media-item-id :int]
   [:custom-order  {:optional true} [:maybe :int]]
   [:name          {:optional true} [:maybe :string]]])

(def CollectionItemAdd
  [:map
   [:media-item-id MediaItemRef]])

;; ---------------------------------------------------------------------------
;; Playouts & events
;; ---------------------------------------------------------------------------

(def PlayoutId [:int {:min 1}])
(def EventId   [:int {:min 1}])

(def EventKind
  [:enum "content" "pre" "mid" "post" "pad" "tail" "fallback" "offline" "bumper"])

(def Playout
  [:map
   [:id                 PlayoutId]
   [:channel-id         :int]
   [:schedule-id        {:optional true} [:maybe :int]]
   [:seed               {:optional true} :int]
   [:daily-rebuild-time {:optional true} [:maybe Interval]]
   [:cursor             {:optional true} [:maybe [:map]]]
   [:last-built-at      {:optional true} [:maybe Instant]]
   [:build-success      {:optional true} [:maybe :boolean]]
   [:build-message      {:optional true} [:maybe :string]]])

(def RebuildResult
  [:map
   [:message          :string]
   [:events-generated :int]
   [:horizon-days     :int]])

(def RebuildQuery
  [:map
   [:from    {:optional true} [:enum "now" "horizon"]]
   [:horizon {:optional true} [:int {:min 1 :max 365}]]])

(def EnsureHorizonQuery
  [:map
   [:horizon {:optional true
              :description "Days of upcoming content to guarantee, measured from
                           now. Default 7."}
    [:int {:min 1 :max 365}]]])

(def AttachScheduleBody
  [:map
   [:schedule-id [:maybe :int]]])

(def AttachScheduleQuery
  [:map
   [:rebuild {:optional true} :boolean]
   [:horizon {:optional true} [:int {:min 1 :max 365}]]])

(def ClearPlayoutQuery
  [:map
   [:manual {:optional true} :boolean]])

(def ClearEventsQuery
  [:map
   [:from   {:optional true} Instant]
   [:to     {:optional true} Instant]
   [:manual {:optional true} :boolean]])

(def ClearResult
  [:map
   [:message               :string]
   [:events-deleted        :int]
   [:manual-events-removed :boolean]])

(def PlayoutEvent
  [:map
   [:id                          EventId]
   [:playout-id                  PlayoutId]
   [:media-item-id               :int]
   [:kind                        {:optional true} EventKind]
   [:start-at                    Instant]
   [:finish-at                   Instant]
   [:guide-start-at              {:optional true} [:maybe Instant]]
   [:guide-finish-at             {:optional true} [:maybe Instant]]
   [:guide-group                 {:optional true} :int]
   [:custom-title                {:optional true} [:maybe :string]]
   [:title                       {:optional true} [:maybe :string]]
   [:plot                        {:optional true} [:maybe :string]]
   [:release-date                {:optional true} [:maybe :string]]
   [:media-item-link             {:optional true} [:maybe :string]]
   [:in-point                    {:optional true} Interval]
   [:out-point                   {:optional true} [:maybe Interval]]
   [:chapter-title               {:optional true} [:maybe :string]]
   [:watermark-id                {:optional true} [:maybe :int]]
   [:disable-watermarks          {:optional true} :boolean]
   [:preferred-audio-language    {:optional true} [:maybe :string]]
   [:preferred-audio-title       {:optional true} [:maybe :string]]
   [:preferred-subtitle-language {:optional true} [:maybe :string]]
   [:subtitle-mode               {:optional true} [:maybe SubtitleModeEnum]]
   [:slot-id                     {:optional true} [:maybe :int]]
   [:is-manual                   {:optional true} :boolean]])

(def PaginatedPlayoutEvents
  "Paginated response for playout events."
  [:map
   [:items [:vector PlayoutEvent]]
   [:pagination CursorPaginationMeta]])

(def ManualEventCreate
  "Body for POST /api/channels/:channel-id/playout/events.
   start-at / finish-at accept ISO-8601 strings; handler parses them to Instant."
  [:map
   [:media-item-id               :int]
   [:start-at                    Instant]
   [:finish-at                   Instant]
   [:kind                        {:optional true} EventKind]
   [:guide-start-at              {:optional true} Instant]
   [:guide-finish-at             {:optional true} Instant]
   [:guide-group                 {:optional true} :int]
   [:custom-title                {:optional true} :string]
   [:in-point                    {:optional true} Interval]
   [:out-point                   {:optional true} Interval]
   [:chapter-title               {:optional true} :string]
   [:watermark-id                {:optional true} :int]
   [:disable-watermarks          {:optional true} :boolean]
   [:preferred-audio-language    {:optional true} :string]
   [:preferred-audio-title       {:optional true} :string]
   [:preferred-subtitle-language {:optional true} :string]
   [:subtitle-mode               {:optional true} SubtitleModeEnum]])

(def ManualEventUpdate
  [:map
   [:media-item-id               {:optional true} :int]
   [:start-at                    {:optional true} Instant]
   [:finish-at                   {:optional true} Instant]
   [:kind                        {:optional true} EventKind]
   [:guide-start-at              {:optional true} Instant]
   [:guide-finish-at             {:optional true} Instant]
   [:guide-group                 {:optional true} :int]
   [:custom-title                {:optional true} :string]
   [:in-point                    {:optional true} Interval]
   [:out-point                   {:optional true} Interval]
   [:chapter-title               {:optional true} :string]
   [:watermark-id                {:optional true} :int]
   [:disable-watermarks          {:optional true} :boolean]
   [:preferred-audio-language    {:optional true} :string]
   [:preferred-audio-title       {:optional true} :string]
   [:preferred-subtitle-language {:optional true} :string]
   [:subtitle-mode               {:optional true} SubtitleModeEnum]])

;; ---------------------------------------------------------------------------
;; Jobs
;;
;; Async background tasks tracked by pseudovision.jobs.runner. The wire shape is
;; kept compatible with the Tunarr Scheduler service so a single UI can render
;; jobs from either backend. `:type` and `:status` are namespaced/plain keywords
;; that serialise to strings (e.g. "playout/rebuild", "running").
;; ---------------------------------------------------------------------------

(def JobId
  [:string {:min 1 :description "Job identifier UUID"}])

(def JobType
  ;; Closed enum of the job types pseudovision can submit. Keep in sync with the
  ;; configs passed to pseudovision.jobs.runner/submit!.
  [:enum {:description "Type of async job"}
   :playout/rebuild
   :playout/ensure-all
   :media/scan-all
   :media/library-scan])

(def JobStatus
  [:enum {:description "Current job status"}
   :queued :running :succeeded :failed])

(def JobProgress
  "Standard progress shape for item-based jobs. Open map: individual jobs may
   report extra keys (e.g. :channel-id)."
  [:map {:closed false}
   [:phase        {:optional true} [:maybe :string]]
   [:total        {:optional true} [:maybe :int]]
   [:completed    {:optional true} [:maybe :int]]
   [:failed       {:optional true} [:maybe :int]]
   [:skipped      {:optional true} [:maybe :int]]
   [:current-item {:optional true} [:maybe [:map {:closed false}
                                            [:id   {:optional true} [:maybe :string]]
                                            [:name {:optional true} [:maybe :string]]]]]])

(def Job
  [:map
   [:id           JobId]
   [:type         JobType]
   [:status       JobStatus]
   [:metadata     {:optional true} [:maybe [:map {:closed false}]]] 
   [:progress     {:optional true} [:maybe [:or JobProgress number?]]]
   [:duration-ms  {:optional true} [:maybe :int]]
   [:result       {:optional true} :any]
   [:error        {:optional true} [:maybe [:map {:closed false}
                                            [:message :string]
                                            [:type    :string]]]]
   [:created-at   {:optional true} [:maybe :string]]
   [:started-at   {:optional true} [:maybe :string]]
   [:completed-at {:optional true} [:maybe :string]]])

(def JobSubmitResponse
  [:map [:job Job]])

(def JobListResponse
  [:map [:jobs [:vector Job]]])

(def JobInfoResponse
  [:map [:job Job]])

;; ---------------------------------------------------------------------------
;; Miscellaneous
;; ---------------------------------------------------------------------------

(def Health
  [:map
   [:status :string]
   [:accels   {:optional true} [:vector :string]]
   [:streams  {:optional true} :int]
   [:hostname {:optional true} [:maybe :string]]])

(def Version
  [:map
   [:git-commit    {:optional true} [:maybe :string]]
   [:git-timestamp {:optional true} [:maybe :string]]
   [:version-tag   {:optional true} [:maybe :string]]])

(def StreamDebug
  [:map {:description "Opaque diagnostic payload for a running HLS stream"}])

;; ---------------------------------------------------------------------------
;; CatalogProfile — Tunabrain scheduling contract
;; ---------------------------------------------------------------------------

;; DEPRECATED: :genres is a hardcoded first-class field. Genres are dimensions
;; now and should be represented as tags (e.g. "genre:comedy").
;; Use :tags instead. See TS DIMENSION_CLEANUP.md for the full migration plan.
(def ShowProfile
  "Per-show or per-movie rollup sent to Tunabrain.

   DEPRECATED: Includes :genres as a hardcoded field. Genres are dimensions
   now and should be in :tags as 'genre:NAME'."
  [:map
   [:media_id              :string]
   [:title                 :string]
   [:genres                {:optional true} [:vector :string]]
   [:episode_count         :int]
   [:available_episode_count :int]
   [:avg_runtime_minutes   {:optional true} [:maybe :double]]
   [:tags                  {:optional true} [:vector :string]]])

;; DEPRECATED: Hardcoded genre aggregate. Genres are dimensions now.
;; Use TagAggregate with "genre:" prefix instead.
;; See TS DIMENSION_CLEANUP.md for the full migration plan.
(def GenreProfile
  "DEPRECATED: Per-genre aggregate. Genres are dimensions now.
   Use TagAggregate with 'genre:' prefix instead."
  [:map
   [:genre        :string]
   [:show_count   :int]
   [:episode_count :int]])

(def TagAggregate
  "Per-tag aggregate."
  [:map
   [:tag         :string]
   [:show_count   :int]
   [:episode_count :int]])

(def RuntimeBucket
  "One bar of the runtime histogram."
  [:map
   [:label       :string]
   [:min_minutes :int]
   [:max_minutes [:maybe :int]]
   [:item_count  :int]])

;; DEPRECATED: Includes :channel_scope (hardcoded channel concept) and
;; :genres (hardcoded genre aggregates). Both are legacy first-class fields.
;; In the dimension model, channels and genres are just tags in :tag_aggregates
;; with "channel:" and "genre:" prefixes.
;; See TS DIMENSION_CLEANUP.md for the full migration plan.
(def CatalogProfile
  "DEPRECATED: The shape of the library with hardcoded fields.

   Includes :channel_scope (hardcoded channel concept) and :genres
   (hardcoded genre aggregates). In the dimension model, both are
   tags in :tag_aggregates with 'channel:' and 'genre:' prefixes.

   The raw items are never included."
  [:map
   [:channel_scope      {:optional true} [:maybe :string]]
   [:total_items        :int]
   [:total_episodes     :int]
   [:movie_count        :int]
   [:shows              [:vector ShowProfile]]
   [:genres             [:vector GenreProfile]]
   [:tag_aggregates     {:optional true} [:vector TagAggregate]]
   [:runtime_histogram  [:vector RuntimeBucket]]
   [:generated_at       {:optional true} [:maybe :string]]])

(def GroutSyncResult
  "Result of a Grout long-form content sync. `enabled` is false (with zero
   counts) when GROUT_URL is unset. `synced` counts newly ingested programs,
   `updated` counts existing ones whose metadata was refreshed, `skipped`
   counts unusable clips, `errors` counts clips that threw during ingest."
  [:map
   [:enabled :boolean]
   [:total   :int]
   [:synced  :int]
   [:updated :int]
   [:skipped :int]
   [:errors  :int]])

(def CatalogCountRequest
  [:map
   [:filters {:optional true} [:map]]])

(def CatalogCountResponse
  [:map
   [:count :int]])
