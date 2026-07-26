(ns pseudovision.scheduling.filler
  "Filler injection: selects filler items to fill gaps in the playout.

   Resolution order for each filler role:
     1. Slot-level override
     2. Channel-level default
     3. No filler (leave a gap / go offline)

   Filler item selection uses the same enumerator system as main content
   so shuffle seeds are preserved across rebuilds.

   Small gaps (≤ 15 seconds) are preferentially filled with bumper items
   sourced from Grout when a suitable one exists for the channel."
  (:require [pseudovision.media.grout-source :as grout-source]
            [pseudovision.scheduling.enumerators :as enum]
            [pseudovision.util.sql  :as sql-util]
            [pseudovision.util.time :as t])
  (:import [java.time Duration Instant]))

;; ---------------------------------------------------------------------------
;; Preset resolution
;; ---------------------------------------------------------------------------

(defn resolve-filler-preset
  "Returns the filler preset map for `role` by checking slot then channel.
   Slot keys are qualified (e.g. :schedule-slots/tail-filler-id) as returned
   by the DB layer; channel uses :channels/fallback-filler-id."
  [role slot channel]
  (let [slot-fk    (keyword "schedule-slots" (str (name role) "-filler-id"))
        channel-fk :channels/fallback-filler-id]
    (cond
      (get slot    slot-fk)    {:id (get slot    slot-fk) :source :slot}
      (= role :fallback)       (when (get channel channel-fk)
                                 {:id (get channel channel-fk) :source :channel})
      :else                    nil)))

;; ---------------------------------------------------------------------------
;; Gap filling
;; ---------------------------------------------------------------------------

(defn fill-gap
  "Given a gap between `from` and `to`, returns a (possibly empty) seq of
   filler event maps to fill it.

   `preset`   — filler_presets row (already resolved)
   `items`    — seq of candidate media-item maps from the preset's collection
   `enumerator` — current enumerator state for this preset's collection
   `playout-id` — for stamping on event maps

   Returns {:events [event-map …] :enumerator updated-enumerator}"
  [from to preset items enumerator playout-id]
  (let [gap-duration (t/duration-between from to)]
    (cond
      ;; Nothing to fill with
      (empty? items)
      {:events [] :enumerator enumerator}

      ;; duration mode: pick items until we've filled the gap
      (= "duration" (:filler-presets/mode preset))
      (loop [cursor  from
             enum    enumerator
             events  []]
        (let [remaining (t/duration-between cursor to)]
          (if (or (.isNegative remaining)
                  (.isZero remaining)
                  (empty? (:items enum)))
            {:events events :enumerator enum}
            (let [[item enum'] (enum/next-item enum)
                  dur          (or (some-> item :media-versions/duration)
                                   (Duration/ofSeconds 0))
                  finish       (t/add-duration cursor dur)]
              (if (.isAfter finish to)
                ;; This item would overflow the gap; try shorter or stop
                {:events events :enumerator enum}
                (recur finish
                       enum'
                       (conj events {:playout-id    playout-id
                                     :media-item-id (:media-items/id item)
                                     :kind          (sql-util/->pg-enum "event_kind" (:filler-presets/role preset))
                                     :start-at      cursor
                                     :finish-at     finish
                                     :is-manual     false})))))))

      ;; count mode: pick exactly N items
      (= "count" (:filler-presets/mode preset))
      (let [n (:filler-presets/count preset 1)]
        (loop [i      0
               cursor from
               enum   enumerator
               events []]
          (if (>= i n)
            {:events events :enumerator enum}
            (let [[item enum'] (enum/next-item enum)
                  dur          (or (some-> item :media-versions/duration)
                                   (Duration/ofSeconds 0))
                  finish       (t/add-duration cursor dur)]
              (recur (inc i) finish enum'
                     (conj events {:playout-id    playout-id
                                   :media-item-id (:media-items/id item)
                                   :kind          (sql-util/->pg-enum "event_kind" (:filler-presets/role preset))
                                   :start-at      cursor
                                   :finish-at     finish
                                   :is-manual     false}))))))

      :else
      {:events [] :enumerator enumerator})))

;; ---------------------------------------------------------------------------
;; Bumper gap filling (small cracks between content)
;; ---------------------------------------------------------------------------

(def ^:private max-bumper-gap-secs 15)
(def ^:private bumper-durations [5 10 15])

(defn- duration-bucket
  "Given a target duration in seconds, return the largest standard
   bumper bucket (5/10/15) that fits without exceeding it."
  [target-secs]
  (->> bumper-durations
       (filter #(<= % target-secs))
       last
       (or 5)))

(defn- item-duration-seconds
  "Extract duration from a media-item row as seconds."
  [item]
  (let [dur (or (some-> item :media-versions/duration t/duration->seconds)
                (some-> item :duration t/duration->seconds)
                0)]
    (max 0 dur)))

(defn fill-gap-with-bumper
  "Try to fill a small gap with a bumper item sourced from Grout.

   Only considers gaps ≤ max-bumper-gap-secs (15s). Queries Grout for the
   channel's bumper-kind clips (see `grout-source/grout-bumper-items`) whose
   duration matches the largest standard bucket that fits the gap (5/10/15s),
   ingesting the chosen clip as a local media item exactly like regular Grout
   filler does.

   Returns nil when no bumper is suitable (Grout disabled/unreachable, or no
   candidate in range) so the caller can fall back to regular filler."
  [db grout channel from to playout-id]
  (let [gap-secs (t/duration->seconds (t/duration-between from to))]
    (when (and (<= gap-secs max-bumper-gap-secs)
               (pos? gap-secs))
      (let [bucket (duration-bucket gap-secs)
            ;; Allow 1s tolerance so a 4.8s bumper can fill a 5s gap
            tolerance 1.0
            min-ms (long (* 1000 (- bucket tolerance)))
            max-ms (long (* 1000 (+ bucket tolerance)))
            candidates (grout-source/grout-bumper-items db grout channel min-ms max-ms)]
        (when (seq candidates)
          ;; Pick randomly for variety (no enumerator state needed for bumpers)
          (let [item (rand-nth candidates)
                dur-secs (item-duration-seconds item)
                dur (t/seconds->duration dur-secs)
                finish (t/add-duration from dur)]
            [{:playout-id    playout-id
              :media-item-id (:media-items/id item)
              :kind          (sql-util/->pg-enum "event_kind" "bumper")
              :start-at      from
              :finish-at     finish
              :is-manual     false}]))))))

(defn pad-to-boundary
  "Produces filler events from `from` up to the next multiple-of-n-minutes
   boundary, or up to `ceil` if that comes first."
  [from ceil n-minutes items enumerator playout-id role]
  (let [boundary (t/ceil-to-n-minutes from n-minutes)
        to       (if (.isAfter boundary ceil) ceil boundary)]
    (fill-gap from to {:filler-presets/mode  "duration"
                       :filler-presets/role  role}
              items enumerator playout-id)))
