(ns pseudovision.http.api.daily-slots
  "Ingestion endpoint for the expanded DailySlot[] stream produced by
    Tunarr Scheduler's deterministic expander. Resolves media_id +
    media_selection_strategy into concrete playout events."
  (:require [clojure.string                :as str]
            [clojure.set                   :as set]
            [pseudovision.db.playouts      :as playout-db]
            [pseudovision.db.channels      :as channels-db]
            [pseudovision.db.core          :as db-core]
            [pseudovision.util.sql         :as sql-util]
            [pseudovision.util.tags        :as tags]
            [pseudovision.util.time        :as time]
            [honey.sql.helpers :as h]
            [honey.sql :as sql]
            [taoensso.timbre :as log])
  (:import [java.time Duration Instant LocalDateTime OffsetDateTime]))

;; ---------------------------------------------------------------------------
;; Media resolution
;; ---------------------------------------------------------------------------

(defn- parse-media-id
  "media_id strings follow 'type:identifier' convention.
   Returns [type identifier] or nil if unparseable."
  [s]
  (when (seq s)
    (let [idx (.indexOf s ":")]
      (when (and (>= idx 0) (< idx (dec (count s))))
        [(subs s 0 idx) (subs s (inc idx))]))))

(defn- resolve-show-episodes
  "Returns ordered episodes for a show identified by id or remote_key.

   Each row carries a `:_top-id` key (the show's id) so downstream
   `category_filters` matching can look up show-level tags."
  [ds show-id-or-key]
  (let [;; Try to resolve by internal id or remote_key
        show (or (db-core/query-one ds
                    (-> (h/select :mi.id)
                        (h/from [:media-items :mi])
                        (h/where [:= :mi.id (try (Long/parseLong show-id-or-key)
                                                (catch Exception _ -1))])
                        sql/format))
                 (db-core/query-one ds
                    (-> (h/select :mi.id)
                        (h/from [:media-items :mi])
                        (h/where [:= :mi.remote-key show-id-or-key])
                        sql/format)))
        show-id (:media-items/id show)]
    (if show
      (let [episodes (db-core/query ds
                         (-> (h/select :mi.* [:mv.duration :duration])
                             (h/from [:media-items :mi])
                             ;; Episodes are nested under seasons (episode.parent_id → season →
                             ;; show), so join each episode's parent-as-season to reach the show.
                             ;; A direct show→episode link is also honoured for flat libraries.
                             (h/left-join [:media-items :season]
                                          [:and [:= :season.id :mi.parent-id]
                                                [:= :season.kind (sql-util/->pg-enum "media_item_kind" "season")]])
                             (h/left-join [:media-versions :mv] [:= :mv.media-item-id :mi.id])
                             (h/where [:and
                                       [:= :mi.kind (sql-util/->pg-enum "media_item_kind" "episode")]
                                       [:= :mi.state (sql-util/->pg-enum "media_item_state" "normal")]
                                       [:or [:= :mi.parent-id show-id]
                                            [:= :season.parent-id show-id]]])
                             (h/order-by :season.position :mi.position :mi.id)
                             sql/format))]
        ;; Tag every episode with its parent show id so downstream
        ;; `category_filters` matching (via `top-id-of`) can resolve
        ;; show-level tags that live on the show's metadata, not the
        ;; episode's.
        (mapv #(assoc % :_top-id show-id) episodes))
      [])))

(defn- resolve-movie
  "Returns a movie item by id or remote_key.

   The returned row carries a `:_top-id` key (the movie's own id) so
   downstream `category_filters` matching uses a uniform lookup — for
   movies, top and play are the same row."
  [ds movie-id-or-key]
  (let [;; Try to resolve by internal id or remote_key
        movie (or (db-core/query-one ds
                    (-> (h/select :mi.* [:mv.duration :duration])
                        (h/from [:media-items :mi])
                        (h/left-join [:media-versions :mv] [:= :mv.media-item-id :mi.id])
                        (h/where [:and [:= :mi.id (try (Long/parseLong movie-id-or-key)
                                                      (catch Exception _ -1))]
                                       [:= :mi.kind (sql-util/->pg-enum "media_item_kind" "movie")]])
                        sql/format))
                 (db-core/query-one ds
                    (-> (h/select :mi.* [:mv.duration :duration])
                        (h/from [:media-items :mi])
                        (h/left-join [:media-versions :mv] [:= :mv.media-item-id :mi.id])
                        (h/where [:and [:= :mi.remote-key movie-id-or-key]
                                       [:= :mi.kind (sql-util/->pg-enum "media_item_kind" "movie")]])
                        sql/format)))]
    ;; For movies the playable row IS the top-level item, so _top-id == the
    ;; movie's own id. Carry it on the row for uniform downstream matching.
    (cond-> movie
      movie (assoc :_top-id (:media-items/id movie)))))

(defn- kebab-case
  "DEPRECATED: Use `pseudovision.util.tags/kebab-case` instead. Kept as a
   private alias so the existing #ds/kebab-case test reference keeps working
   without a separate rename PR."
  [s]
  (tags/kebab-case s))

(defn- resolve-by-category
  "Resolves a `random:<category>` pool to concrete *playable* items.

   `category` is matched against the metadata of top-level items (shows and
   movies) — exactly the dimension space the catalog aggregate emits — via
   the canonical `genre:` rows in `metadata_tags` (e.g. `genre:mystery`,
   `genre:sci-fi-and-fantasy`). All categorisation lives in `metadata_tags`
   alongside `channel:<slug>`, `time-slot:*`, `audience:*`, and the other
   dimensions; the legacy `metadata_genres` table is gone.

   Matches are case-insensitive on tags (Postgres `lower(t.name)`) so the
   legacy \"Mystery\" form matches the canonical \"mystery\" storage, AND
   we also try the kebab-cased form so LLM-generated overrides like
   `random:Sci-Fi & Fantasy` (with `&` and spaces) hit the canonical
   `genre:sci-fi-and-fantasy` tag.

   Matching shows are expanded to their (season-nested) episodes and matching
   movies resolve to themselves, so the returned rows are always directly
   playable items, never bare show rows.

   Each row carries a `:_top-id` key holding the id of the top-level item it
   was resolved from (the show for episode rows, the movie for movie rows —
   identical in that case). Downstream `category_filters` matching uses this
   id to look up show-level tags, which live on the top row rather than the
   episode row."
  [ds category]
  (db-core/query ds
    (-> (h/select-distinct :play.* [:mvp.duration :duration] [:top.id :_top-id])
        (h/from [:media-items :top])
        (h/join [:metadata :mtop] [:= :mtop.media-item-id :top.id])
        (h/left-join [:metadata-tags :t] [:= :t.metadata-id :mtop.id])
        ;; Seasons of matching shows, so we can reach their episodes.
        (h/left-join [:media-items :season]
                     [:and [:= :season.parent-id :top.id]
                           [:= :season.kind (sql-util/->pg-enum "media_item_kind" "season")]])
        ;; The actual playable row: the movie itself, or an episode of the show
        ;; (direct child or nested under one of its seasons).
        ;;
        ;; This is a LEFT JOIN even though it behaves like an inner join (the
        ;; `play.state = normal` predicate in WHERE drops the unmatched NULL
        ;; rows). HoneySQL emits every inner `:join` before every `:left-join`
        ;; regardless of threading order, so an inner join here would be
        ;; rendered ahead of the `:season` LEFT JOIN it references, producing
        ;; "missing FROM-clause entry for table season". Keeping it a left join
        ;; preserves the season → play ordering.
        (h/left-join [:media-items :play]
                     [:or
                      [:and [:= :top.kind (sql-util/->pg-enum "media_item_kind" "movie")]
                            [:= :play.id :top.id]]
                      [:and [:= :top.kind (sql-util/->pg-enum "media_item_kind" "show")]
                            [:= :play.kind (sql-util/->pg-enum "media_item_kind" "episode")]
                            [:or [:= :play.parent-id :top.id]
                                 [:= :play.parent-id :season.id]]]])
        (h/left-join [:media-versions :mvp] [:= :mvp.media-item-id :play.id])
        (h/where [:and
                  [:= :top.state (sql-util/->pg-enum "media_item_state" "normal")]
                  [:= :play.state (sql-util/->pg-enum "media_item_state" "normal")]
                  [:in :top.kind [(sql-util/->pg-enum "media_item_kind" "show")
                                  (sql-util/->pg-enum "media_item_kind" "movie")]]
                  [:or [:= [:lower :t.name] (str/lower-case category)]
                       [:= [:lower :t.name] (str "genre:" (str/lower-case category))]
                       [:= [:lower :t.name] (kebab-case category)]
                       [:= [:lower :t.name] (str "genre:" (kebab-case category))]]])
        (h/order-by :play.id)
        sql/format)))

(defn- last-aired-episode-index
  "Finds the index of the most recently aired episode for a show in a playout.
   Returns nil if no episode has aired yet."
  [ds playout-id show-id]
  (let [show (db-core/query-one ds
               (-> (h/select :mi.id)
                   (h/from [:media-items :mi])
                   (h/where [:and [:= :mi.id show-id]
                                  [:= :mi.kind (sql-util/->pg-enum "media_item_kind" "show")]])
                   sql/format))]
    (when show
      (let [last-event (db-core/query-one ds
                         (-> (h/select :pe.media-item-id)
                             (h/from [:playout-events :pe])
                             (h/join [:media-items :mi2] [:= :mi2.id :pe.media-item-id])
                             ;; Episodes hang off seasons, so reach the show via
                             ;; the episode's parent-as-season too.
                             (h/left-join [:media-items :season2]
                                          [:and [:= :season2.id :mi2.parent-id]
                                                [:= :season2.kind (sql-util/->pg-enum "media_item_kind" "season")]])
                             (h/where [:and [:= :pe.playout-id playout-id]
                                            [:= :mi2.kind (sql-util/->pg-enum "media_item_kind" "episode")]
                                            [:or [:= :mi2.parent-id show-id]
                                                 [:= :season2.parent-id show-id]]])
                             (h/order-by [:pe.start-at :desc])
                             (h/limit 1)
                             sql/format))]
        (when last-event
          ;; Find the position of this episode in the show's episode list
          (let [episodes (resolve-show-episodes ds (str (:media-items/id show)))
                ep-id  (:playout-events/media-item-id last-event)]
            (first (keep-indexed (fn [idx ep]
                                   (when (= (:media-items/id ep) ep-id)
                                     idx))
                                 episodes))))))))

(defn- pick-episode
  "Selects an episode from `episodes` based on `strategy`.
   `playout-id` and `show-id` are used for sequential tracking.
   `tracker` is a mutable atom ({show-id -> {:last-idx N}}) that records
   in-memory state for the current batch so multiple slots for the same
   show in one ingest do not all resolve to the same episode."
  [ds playout-id show-id episodes strategy tracker]
  (let [n (count episodes)]
    (when (pos? n)
      (case strategy
        "sequential"
        (let [last-idx (or (get-in @tracker [:series show-id :last-idx])
                           (last-aired-episode-index ds playout-id show-id))
              next-idx (if (nil? last-idx) 0 (mod (inc last-idx) n))]
          (swap! tracker assoc-in [:series show-id] {:last-idx next-idx})
          (nth episodes next-idx))

        "specific"
        ;; specific with a series ID is ambiguous; pick the first episode
        (first episodes)

        ;; "random" (default)
        (let [last-idx (or (get-in @tracker [:series show-id :last-idx]) -1)
              next-idx (mod (inc last-idx) n)]
          (swap! tracker assoc-in [:series show-id] {:last-idx next-idx})
          (nth episodes next-idx))))))

(defn- pool-cache-key
  "A tracker key for a `random:<category>` pool that changes whenever the
   candidate SET does, not just the media_id.

   Before duration-fit narrowing, every slot referencing the same media_id
   within a batch saw the identical (tag-filtered) item set, so caching a
   shuffled rotation under `media-id` alone was safe. Now `select-fitting-
   items` can hand different slots a different subset of the same category
   (a 30-minute bucket vs. a 2-hour bucket), so `pick-from-pool`'s cached
   `:pool` (a literal shuffled array, indexed by a count computed from
   whatever `items` the CURRENT call passed) would otherwise be read with an
   index bound to the wrong array — safe when the new set is smaller, an
   IndexOutOfBoundsException when it's larger. Folding the candidate ids into
   the key means a different subset simply starts its own independent
   rotation instead of reusing another subset's stale array."
  [media-id items]
  (str media-id "#" (hash (sort (map :media-items/id items)))))

(defn- pick-from-pool
  "Picks an item from `items` according to `strategy`, using `tracker` to
   avoid duplicates within a single ingest batch.

   For `random` we shuffle the pool once per batch and cycle through it,
   so the same item never airs twice until every item has been used once."
  [pool-key items strategy tracker]
  (let [n (count items)]
    (when (pos? n)
      (case strategy
        "random"
        (let [state   (get-in @tracker [:random pool-key])
              pool    (or (:pool state) (shuffle items))
              last-idx (or (:last-idx state) -1)
              next-idx (mod (inc last-idx) n)]
          (swap! tracker assoc-in [:random pool-key] {:pool pool :last-idx next-idx})
          (nth pool next-idx))

        "specific"
        (first items)

        ;; default (sequential for non-series)
        (first items)))))

(defn- top-id-of
  "The id of the top-level media-item (show or movie) that an item was
   resolved from. Show-level filters — `channel:<slug>`, `time-slot:*`,
   `audience:*`, `freshness:*` — live on this row in `metadata_tags`, not on
   the episode row, so `category_filters` matching must scope by top id
   rather than the playable row's id.

   For movies, top and play are the same row and `:_top-id` equals
   `:media-items/id`. For episodes, `:_top-id` is the parent show's id (set
   by `resolve-show-episodes` / `resolve-by-category`); for any row that
   hasn't been tagged with `:_top-id` we fall back to the row's own id so
   the call site is uniform."
  [item]
  (or (:_top-id item) (:media-items/id item)))

;; ---------------------------------------------------------------------------
;; Duration-aware fit selection
;;
;; The expanded DailySlot stream describes a slot's WALL-CLOCK window (e.g. a
;; 1-hour block for `random:movie`), but a `random:<category>` pool typically
;; spans wildly different runtimes (a 22-minute sitcom, a 3-hour epic). Picking
;; blindly from the whole pool routinely lands an item that overruns the slot,
;; which used to silently overlap the next slot's event (the event's finish_at
;; was stamped from the slot boundary, not the item's actual runtime).
;;
;; `select-fitting-items` narrows the pool to items whose runtime plausibly
;; belongs in the slot before the existing pick-from-pool/pick-episode
;; rotation logic runs, so variety and no-repeat rotation are preserved WITHIN
;; the fitting subset. `create-event-from-slot` then stamps the event with the
;; item's real duration and lets a later slot's start drift forward when an
;; item still overflows — see its docstring.
;; ---------------------------------------------------------------------------

(def default-fit-tolerance-minutes
  "Slack allowed between a slot's nominal duration and a candidate item's
   runtime when picking from a random:<category> pool. An item up to this many
   minutes under the slot's length is a perfect fit; an item up to this many
   minutes OVER is tolerated too (the next slot simply starts a little late).
   Only when nothing in the pool is within this window do we fall back to
   whatever is closest, so a thin pool never fails a slot outright."
  15)

(def fit-fallback-window-multiplier
  "How much the closest-runtime fallback is allowed to expand beyond the
   primary tolerance when nothing in the pool fits. A 2-hour slot's primary
   tolerance is 15 min (105-135 min admitted); the fallback then considers
   items within 2x that (75-165 min) — admitting a 90-min movie in a 2-hour
   slot, but rejecting a 22-min episode in the same slot (98 min short of
   the lower bound).

   The 2x value was chosen to admit the two most common 'off-by-30min'
   cases (30-min episode in 60-min slot; 90-min movie in 60-min slot)
   while excluding the wildly-off-runtime case (22-min episode in 2-hr
   slot). See `select-fitting-items` for the implementation; see
   `*fitter-strictness*` for what happens when the expanded window is
   also empty."
  2)

(def ^:dynamic *fitter-strictness*
  "Policy applied when `select-fitting-items` cannot find a runtime-fitting
   item even in the expanded window. One of:

     :fail   — log a WARN and return []. The slot's loop in
               `create-event-from-slot` accumulates the error, no event is
               written, and the daily-slots response surfaces the missing
               slot in `:errors`. This is the right policy for staging /
               dev: every empty-pool case is visible, every wildly-off
               runtime surfaces to the operator.

     :warn-and-fill — log a WARN naming the bad pick and return the
               single closest-runtime item anyway. Something is always
               on-screen. The WARN is the audit trail. This is the right
               policy for production once the bug is known and the LLM is
               mostly generating well-shaped slot templates.

     :off   — original behavior: closest-of-N from the entire pool, no
               log. Useful as a temporary revert if the bound breaks a
               channel in production. Not recommended for any long-term
               use; exists for symmetry and quick rollback.

   The default is :warn-and-fill because production reliability (something
   on-screen) is more important than visible-error density. Operators who
   want fail-loud should set `:daily-slots {:fitter-strictness :fail}` in
   config.edn and re-roll the pod."
  :warn-and-fill)

(defn- playable-item?
  "True when `item` carries a known, positive runtime. Mirrors
   pseudovision.scheduling.core/playable? — an item with no probed duration
   can't be stamped with a truthful finish_at, so it can't be scheduled here.

   The duration can arrive under either `:media-versions/duration` (the
   table-qualified key produced by `next.jdbc.result-set/as-kebab-maps` for
   a `mv.duration` column, which is what `db-core/query` returns) or under
   the bare alias `:duration` (the key some upstream queries project it
   under via `:mv.duration :duration` aliases). Both forms are accepted so
   the check is robust against either shape, mirroring
   `pseudovision.scheduling.filler/item-duration-seconds`."
  [item]
  (let [d (or (:media-versions/duration item) (:duration item))]
    (when d
      (pos? (.getSeconds ^Duration d)))))

(defn- duration-secs ^long [item]
  (.getSeconds ^Duration (or (:media-versions/duration item) (:duration item))))

(defn- closest-first [target-secs items]
  (sort-by #(Math/abs (- (duration-secs %) target-secs)) items))

(def ^:private fallback-pool-cap
  "When nothing in a pool is within tolerance of the target, how many of the
   closest-runtime items to still offer to the rotation logic, rather than
   handing over the whole (possibly wildly mismatched) pool."
  8)

(defn- select-fitting-items
  "Narrows `items` (already tag-filtered and playable) to those whose runtime
   best matches a `target-secs`-long slot, within `tolerance-secs` slack.
   Prefers items that fit without overflowing; tolerates a small overflow
   next; falls back to the closest-runtime items overall (bounded to
   `fallback-pool-cap` AND to an expanded tolerance window) only when
   nothing is within the primary tolerance. If the expanded window is
   also empty, defers to `*fitter-strictness*` to decide between failing
   the slot or admitting the single closest item.

   The expanded window exists to admit the two common 'off-by-30min'
   cases (a 30-min episode in a 60-min slot; a 90-min movie in a 60-min
   slot) without admitting wildly-off-runtime content (a 22-min episode
   in a 2-hour slot). The bound is `fit-fallback-window-multiplier`; see
   its docstring for the rationale."
  [items target-secs tolerance-secs]
  (let [lo         (- target-secs tolerance-secs)
        hi         (+ target-secs tolerance-secs)
        expand     (* fit-fallback-window-multiplier tolerance-secs)
        lo-expand  (- target-secs expand)
        hi-expand  (+ target-secs expand)
        under      (filterv #(<= lo (duration-secs %) target-secs) items)
        over       (filterv #(< target-secs (duration-secs %) hi) items)
        ;; Closest-fallback candidates: within the expanded window but
        ;; outside the primary tolerance. The full pool is NOT a candidate
        ;; — that's the bug this function fixes. (The original code's
        ;; closest-of-N from the whole pool admitted wildly-off items
        ;; when the pool was dominated by short content, e.g. a 22-min
        ;; episode getting picked for a 2-hr random:sitcom slot on
        ;; `spectrum`.)
        expanded   (filterv #(<= lo-expand (duration-secs %) hi-expand) items)
        pool-durs  (sort (map duration-secs items))]
    (cond
      (seq under) under
      (seq over)  over
      (seq expanded)
      (let [closest (vec (take fallback-pool-cap (closest-first target-secs expanded)))]
        (log/warn "No pool item within fit tolerance; falling back to expanded window"
                  {:target-secs target-secs
                   :tolerance-secs tolerance-secs
                   :expand-secs expand
                   :pool-size (count items)
                   :expanded-size (count expanded)
                   :strictness *fitter-strictness*})
        closest)
      :else
      (do
        (log/warn "No pool item within expanded tolerance; slot will be empty"
                  {:target-secs target-secs
                   :tolerance-secs tolerance-secs
                   :expand-secs expand
                   :pool-size (count items)
                   :pool-durations pool-durs
                   :strictness *fitter-strictness*})
        (cond
          (= *fitter-strictness* :fail) []
          (= *fitter-strictness* :warn-and-fill)
          (let [closest (vec (take fallback-pool-cap (closest-first target-secs items)))]
            (log/warn "fitter-strictness=:warn-and-fill; admitting closest item despite expanded-empty"
                      {:target-secs target-secs
                       :item-runtime (duration-secs (first closest))
                       :item-runtime-vs-target
                       (- (duration-secs (first closest)) target-secs)})
            closest)
          :else ;; :off or any unrecognized value
          (vec (take fallback-pool-cap (closest-first target-secs items))))))))

(defn- pick-item
  "Resolves a DailySlot's media_id to a concrete media-item row.

   `target-secs`/`tolerance-secs` describe the slot's nominal duration and how
   much runtime slack is acceptable; for `random:<category>` pools the
   candidate set is narrowed to items that plausibly fit before the existing
   rotation logic (pick-from-pool) picks among them. `series:`/`movie:` slots
   are unaffected — a specific movie id or a show's episode order is not
   something duration-fit can second-guess.

   Returns [item error-message] or [nil error]."
  [ds playout-id media-id strategy category-filters tracker target-secs tolerance-secs]
  (if-let [parsed (parse-media-id media-id)]
    (let [type (first parsed)
          id   (second parsed)
          items (case type
                  "series" (resolve-show-episodes ds id)
                  "movie"  (when-let [m (resolve-movie ds id)] [m])
                  "random" (resolve-by-category ds id)
                  [])]
      (when (seq items)
        (log/info "pick-item debug" {:media-id media-id
                                      :item-count (count items)
                                      :first-id (:media-items/id (first items))
                                      :first-top-id (top-id-of (first items))}))
      ;; Apply category_filters (tag filters) if provided. Two semantics in
      ;; play here, so we describe both:
      ;;
      ;;   * Scope goes to the TOP-level item (the show or movie), because
      ;;     show-level tags like `channel:goldenreels` live on the show's
      ;;     metadata, not on the episode row. A previous version scoped by
      ;;     the playable row's `:media-items/id`, which excluded every match
      ;;     for show-level tags and silently rejected slots with
      ;;     "No playable items found".
      ;;
      ;;   * Episodes inherit their parent show's tags, so we union the
      ;;     playable row's tag set with the parent show's tag set before
      ;;     matching. Movies resolve to themselves, so playable and top ids
      ;;     are the same and the union collapses to a single set.
      ;;
      ;; A filter value `foo` matches if either `foo` or `genre:foo` appears
      ;; in the unioned tag set. `resolve-by-category` already lowercases the
      ;; metadata_tags binding side, so accepting both forms keeps the filter
      ;; robust against the casing the scheduler happened to emit.
      (let [filtered (if (seq category-filters)
                       (let [item-ids (map :media-items/id items)
                             top-ids  (distinct (map top-id-of items))
                             all-ids  (vec (distinct (concat item-ids top-ids)))
                             ;; Bulk-fetch tags for playable rows AND their parent shows.
                             tag-rows (when (seq all-ids)
                                        (db-core/query ds
                                          (-> (h/select :m.media-item-id :mt.name)
                                              (h/from [:metadata-tags :mt])
                                              (h/join [:metadata :m] [:= :m.id :mt.metadata-id])
                                              (h/where [:in :m.media-item-id all-ids])
                                              sql/format)))
                             ;; next.jdbc returns kebab-case keys qualified by
                             ;; the *table* name, not the SQL alias, so columns
                             ;; `m.media_item_id, mt.name` come back as
                             ;; `:metadata/media-item-id, :metadata-tags/name`
                             ;; (matching the project's other reads at, e.g.,
                             ;; pseudovision.scheduling.core/get-item-tags).
                             item-tags (group-by :item-id
                                                 (map (fn [r]
                                                        {:item-id (:metadata/media-item-id r)
                                                         :tag      (:metadata-tags/name r)})
                                                      tag-rows))
                             passing   (filterv (fn [item]
                                                  (let [own-tags  (set (map :tag (get item-tags (:media-items/id item))))
                                                        show-tags (set (map :tag (get item-tags (top-id-of item))))
                                                        tags      (set/union own-tags show-tags)]
                                                    (every? #(or (contains? tags %)
                                                                 (contains? tags (str "genre:" %)))
                                                           category-filters)))
                                                items)]
                         (log/info "pick-item filter debug"
                                   {:media-id media-id
                                    :category-filters category-filters
                                    :filtered-count (count passing)
                                    :first-passing-id (:media-items/id (first passing))
                                    :first-passing-tags (when (seq passing)
                                                         (let [it (first passing)
                                                               sid (top-id-of it)]
                                                           (map :tag (get item-tags sid))))})
                          passing)
                       items)
            ;; Drop items with no probed (positive) duration — the event
            ;; below is stamped with the item's REAL runtime, so an unknown
            ;; length can't be scheduled truthfully.
            playable (filterv playable-item? filtered)
            ;; For pooled categories, narrow further to items whose runtime
            ;; plausibly belongs in this slot. series:/movie: selection is
            ;; unaffected (a specific id, or episode order, isn't up for
            ;; renegotiation here).
            fit-ready (if (= type "random")
                        (select-fitting-items playable target-secs tolerance-secs)
                        playable)]
        (if (seq fit-ready)
          (case type
            "series" (let [show-id (try (Long/parseLong id)
                                        (catch Exception _ -1))]
                       (if-let [ep (pick-episode ds playout-id show-id fit-ready strategy tracker)]
                         [ep nil]
                         [nil (str "No episode selected for series " id)]))
            (if-let [pick (pick-from-pool (pool-cache-key media-id fit-ready) fit-ready strategy tracker)]
              [pick nil]
              [nil (str "No playable items found for media_id " media-id)]))
          [nil (str "No playable items found for media_id " media-id)])))
    [nil (str "Invalid media_id format: " media-id)]))

;; ---------------------------------------------------------------------------
;; Event creation
;; ---------------------------------------------------------------------------

(defn- ->instant
  "Parses a DailySlot datetime value into a java.time.Instant.

   Accepts:
     - a java.time.Instant (returned as-is)
     - a full ISO-8601 instant with offset/zone, e.g. \"2026-06-29T00:00:00Z\"
       or \"2026-06-29T00:00:00+02:00\"
     - a naive local ISO datetime, e.g. \"2026-06-29T00:00:00\", which is
       interpreted in the application default zone (`pseudovision.util.time/
       default-zone`, configured via the TZ env var; UTC by default)

   Returns nil for nil, blank, or unparseable input."
  [x]
  (cond
    (nil? x)              nil
    (instance? Instant x) x
    (string? x)
    (let [s (str/trim x)]
      (when (seq s)
        (or
         ;; Full instant: has a trailing 'Z' or numeric offset.
         (try (Instant/parse s)               (catch Exception _ nil))
         ;; Offset datetime without instant-normalisation (defensive).
         (try (.toInstant (OffsetDateTime/parse s)) (catch Exception _ nil))
         ;; Naive local datetime ("YYYY-MM-DDTHH:MM:SS"): interpret in the
         ;; configured application zone.
         (try (.toInstant (.atZone (LocalDateTime/parse s) (time/default-zone)))
              (catch Exception _ nil)))))
    :else x))

(defn- create-event-from-slot
  "Creates a playout_event map from a DailySlot.

   Two things used to make this unsafe for content that doesn't divide evenly
   into its slot (almost every movie in a 1-hour `random:movie` slot):

     1. `finish_at` was stamped from the slot's own `end_time`, not the picked
        item's actual runtime — a 2h movie in a 1h slot was recorded as if it
        finished on time, so the next slot's event started on top of it.
     2. Item selection ignored the slot's duration entirely.

   `pick-item` now narrows `random:<category>` pools to runtime-appropriate
   candidates (see `select-fitting-items`), and this function stamps the
   event with the item's REAL duration. `cursor` is the actual instant the
   previous slot in this batch finished at (nil for the first slot): when the
   previous item overran its slot, this slot's start shifts forward to begin
   right after it — no overlap, at the cost of occasionally starting a few
   minutes late. When the previous item finished at or before this slot's own
   nominal start (including a genuine gap, e.g. a day boundary), this slot
   starts at its own nominal time and any drift settles back to zero.

   Returns [event-or-nil error-or-nil next-cursor]."
  [ds playout-id slot guide-group tracker cursor tolerance-secs]
  (let [raw-start (:start-time slot)
        raw-end   (:end-time slot)
        nominal-start (->instant raw-start)
        nominal-end   (->instant raw-end)
        media-id (:media-id slot)
        strategy (or (:media-selection-strategy slot) "random")
        filters  (:category-filters slot)]
    (cond
      (str/blank? (str raw-start)) [nil "Missing start_time" cursor]
      (nil? nominal-start) [nil (str "Invalid start_time: " raw-start) cursor]
      (str/blank? (str raw-end))   [nil "Missing end_time" cursor]
      (nil? nominal-end)   [nil (str "Invalid end_time: " raw-end) cursor]
      (nil? media-id) [nil "Missing media_id" cursor]
      :else
      (let [target-secs (max 1 (.getSeconds (Duration/between nominal-start nominal-end)))
            [item err] (pick-item ds playout-id media-id strategy filters tracker
                                   target-secs tolerance-secs)]
        (if item
          (let [actual-start (if (and cursor (.isAfter ^Instant cursor nominal-start))
                                cursor
                                nominal-start)
                ^Duration dur (or (:media-versions/duration item) (:duration item))
                actual-end   (.plus ^Instant actual-start dur)]
            [{:playout-id     playout-id
              :media-item-id  (:media-items/id item)
              :kind           (sql-util/->pg-enum "event_kind" "content")
              :start-at       actual-start
              :finish-at      actual-end
              :guide-group    guide-group
              :slot-id        nil
              :is-manual      false
              :custom-title   (when (seq (:notes slot))
                                (str/join "; " (:notes slot)))}
             nil
             actual-end])
          [nil err cursor])))))

;; ---------------------------------------------------------------------------
;; Public handler
;; ---------------------------------------------------------------------------

(defn ingest-daily-slots-handler
  "POST /api/channels/:channel-id/daily-slots

   Body: DailySlot[]

   For each slot:
     1. Resolves media_id -> concrete media_item_id
     2. Creates a playout_event spanning [start_time, end_time]
   Events are inserted in bulk. Existing events in the date range are cleared
   (non-manual only) so the daily-slot stream always wins.

   `ctx` is the HTTP system context. Reads `:fitter-strictness` from
   `(:daily-slots ctx)` (or `(:scheduling ctx)` for backwards compat) and
   binds it to `*fitter-strictness*` for the duration of the request. This
   is how operators configure the slot-empty vs. slot-fill-with-bad-pick
   policy via config.edn — see `*fitter-strictness*` for the three
   states. The binding form is intentional: it lets tests use
   `binding` themselves for per-test overrides without depending on
   `with-redefs` (which would affect every concurrent request)."
  [{:keys [db daily-slots scheduling] :as _ctx}]
  (let [configured (or (:fitter-strictness daily-slots)
                       (:fitter-strictness scheduling)
                       :warn-and-fill)
        strictness (if (#{:fail :warn-and-fill :off} configured)
                     configured
                     (do (log/warn "Unknown :fitter-strictness value, falling back to :warn-and-fill"
                                   {:configured configured})
                         :warn-and-fill))]
    (fn [req]
      (binding [*fitter-strictness* strictness]
        (let [raw-id     (get-in req [:parameters :path :channel-id])
          channel-id (try (Long/parseLong (str raw-id))
                         (catch Exception _ nil))
          ch         (when channel-id (channels-db/get-channel db channel-id))
          real-id    (if ch channel-id
                       (when-let [ch2 (channels-db/get-channel-by-number db (str raw-id))]
                         (:channels/id ch2)))
          playout    (when real-id (playout-db/get-playout-for-channel db real-id))]
      (if-not playout
        {:status 404 :body {:error "No playout for this channel"}}
        (let [slots     (get-in req [:parameters :body])
              playout-id (:playouts/id playout)
              ;; Find the overall time range so we can clear existing events
              starts    (keep #(->instant (:start-time %)) slots)
              ends      (keep #(->instant (:end-time %)) slots)
              from      (when (seq starts) (first (sort-by #(.getEpochSecond %) starts)))
              to        (when (seq ends)   (last (sort-by #(.getEpochSecond %) ends)))]
          (log/info "Ingesting daily slots"
                    {:channel-id real-id
                     :playout-id playout-id
                     :slot-count (count slots)
                     :from (str from)
                     :to   (str to)})
          ;; Clear existing non-manual events in the range
          (when (and from to)
            (playout-db/delete-events! db playout-id
                                       {:keep-manual? true
                                        :from from
                                        :to   to}))
          ;; Create events with an in-memory tracker so sequential series and
          ;; random pools do not repeat the same item within a single batch.
          ;; Slots are processed in start_time order — required for the
          ;; cursor threaded below (see create-event-from-slot): each slot's
          ;; actual start depends on where the PREVIOUS slot's event actually
          ;; finished, so an out-of-order batch would shift things
          ;; nonsensically. The expander already emits slots sorted by start,
          ;; but sort defensively rather than assume it.
          (let [tracker        (atom {})
                tolerance-secs (* 60 default-fit-tolerance-minutes)
                sorted-slots   (sort-by #(some-> (:start-time %) ->instant .getEpochSecond) slots)
                results        (loop [remaining (seq sorted-slots)
                                       idx       0
                                       cursor    nil
                                       acc       []]
                                 (if-not remaining
                                   acc
                                   (let [slot (first remaining)
                                         [event err next-cursor]
                                         (create-event-from-slot db playout-id slot idx tracker
                                                                  cursor tolerance-secs)]
                                     (recur (next remaining) (inc idx) next-cursor
                                            (conj acc (if event {:ok event} {:error err}))))))
                ok-events (keep :ok results)
                errors    (keep :error results)]
            (when (seq ok-events)
              (playout-db/bulk-insert-events! db ok-events))
            (log/info "Daily slot ingestion complete"
                      {:channel-id real-id
                       :ingested (count ok-events)
                       :skipped  (count errors)
                       :errors   errors})
            {:status 200
             :body {:ingested   (count ok-events)
                    :skipped    (count errors)
                    :errors     (vec errors)
                    :channel_id real-id}}))))))))
