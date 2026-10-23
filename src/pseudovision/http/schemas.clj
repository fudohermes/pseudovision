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

(def SortNumber
  [:double {:description "Numeric sort order; derived from :number if omitted on create"}])

(def StreamingMode
  [:enum {:description "Output stream format"} "ts" "hls"])

(def Interval
  [:string {:description "PostgreSQL interval (e.g. \"02:00:00\" or \"1 hour 30 minutes\")"}])

(def Instant
  [:string {:description "ISO-8601 timestamp, e.g. \"2026-04-22T18:30:00Z\""}])

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
   [:group-name                     {:optional true} [:maybe :string]]
   [:categories                     {:optional true} [:maybe :string]]
   [:description                    {:optional true} [:maybe :string]]
   [:streaming-mode                 StreamingMode]
   [:ffmpeg-profile-id              :int]
   [:watermark-id                   {:optional true} [:maybe :int]]
   [:fallback-filler-id             {:optional true} [:maybe :int]]
   [:preferred-audio-language       {:optional true} [:maybe :string]]
   [:preferred-audio-title          {:optional true} [:maybe :string]]
   [:preferred-subtitle-language    {:optional true} [:maybe :string]]
   [:subtitle-mode                  :string]
   [:music-video-credits-mode       {:optional true} [:maybe :string]]
   [:music-video-credits-template   {:optional true} [:maybe :string]]
   [:song-video-mode                {:optional true} [:maybe :string]]
   [:slug-seconds                   {:optional true} [:maybe :double]]
   [:stream-selector-mode           :string]
   [:stream-selector                {:optional true} [:maybe :string]]
   [:is-enabled                     :boolean]
   [:show-in-epg                    :boolean]])

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

;; ---------------------------------------------------------------------------
;; FFmpeg profiles
;; ---------------------------------------------------------------------------

(def FFmpegProfileId [:int {:min 1}])

(def FFmpegProfileConfig
  "Encoding parameters. Keys evolve with FFmpeg; left open."
  [:map {:description "Encoder configuration (see migration comments for keys)"}])

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

(def TagAddResult
  [:map
   [:item-id    :int]
   [:tags-added [:vector TagName]]])

;; ---------------------------------------------------------------------------
;; Media — sources, libraries, items, collections
;; ---------------------------------------------------------------------------

(def MediaSourceId [:int {:min 1}])
(def LibraryId     [:int {:min 1}])
(def MediaItemId   [:int {:min 1}])
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

(def MediaSourceCreate
  [:map
   [:name               :string]
   [:kind               MediaSourceKind]
   [:connection-config  {:optional true} [:map]]
   [:path-replacements  {:optional true} [:sequential [:map]]]])

(def MediaSourceUpdate
  [:map
   [:name               {:optional true} :string]
   [:api-key            {:optional true} :string]
   [:connections        {:optional true} [:sequential [:map
                                                      [:uri :string]
                                                      [:is-active {:optional true} :boolean]]]]
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

(def MediaLibraryCreate
  [:map
   [:name         :string]
   [:kind         LibraryKind]
   [:external-id  {:optional true} :string]
   [:should-sync  {:optional true} :boolean]])

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
   [:config                    {:optional true} [:map]]])

(def CollectionCreate
  [:map
   [:name                      :string]
   [:kind                      {:optional true} CollectionKind]
   [:use-custom-playback-order {:optional true} :boolean]
   [:config                    {:optional true} [:map]]])

(def ScanTriggerResult
  [:map [:message :string]])

;; ---------------------------------------------------------------------------
;; Playouts & events
;; ---------------------------------------------------------------------------

(def PlayoutId [:int {:min 1}])
(def EventId   [:int {:min 1}])

(def EventKind
  [:enum "content" "pre" "mid" "post" "pad" "tail" "fallback" "offline"])

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
;; Miscellaneous
;; ---------------------------------------------------------------------------

(def Health
  [:map [:status :string]])

(def Version
  [:map
   [:git-commit    {:optional true} [:maybe :string]]
   [:git-timestamp {:optional true} [:maybe :string]]
   [:version-tag   {:optional true} [:maybe :string]]])

(def StreamDebug
  [:map {:description "Opaque diagnostic payload for a running HLS stream"}])
