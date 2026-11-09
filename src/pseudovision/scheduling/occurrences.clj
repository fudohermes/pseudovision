(ns pseudovision.scheduling.occurrences
  "Which fixed-anchor slots air when, worked out one calendar day at a time.

   A slot airs on a date when its `days_of_week` mask includes that weekday and
   the date falls inside its optional `active_from`..`active_until` range
   (inclusive). Override slots (`is_override`) win over base slots: a base
   occurrence that overlaps an override occurrence is clipped around it, so the
   override plays and the base strip resumes once the override ends.

   Pure functions over slot rows (as next.jdbc returns them, keyed
   `:schedule-slots/*`) and java.time values; the build loop in
   pseudovision.scheduling.core asks `next-occurrence` what to play next."
  (:import [java.time Duration Instant LocalDate ZoneId ZonedDateTime]))

(defn ->local-date
  "A slot's date bound as a LocalDate. Accepts LocalDate, java.sql.Date, an ISO
   date string, or nil."
  ^LocalDate [x]
  (cond
    (nil? x)                     nil
    (instance? LocalDate x)      x
    (instance? java.sql.Date x)  (.toLocalDate ^java.sql.Date x)
    (string? x)                  (LocalDate/parse ^String x)
    :else (throw (ex-info "Unrecognised date value" {:value x}))))

(defn override? [slot]
  (boolean (:schedule-slots/is-override slot)))

(defn- dow-bit ^long [^LocalDate d]
  (bit-shift-left 1 (dec (.getValue (.getDayOfWeek d)))))

(defn airs-on?
  "Whether `slot` airs on calendar date `d`: weekday in its mask (nil or 0 means
   every day) and `d` within its optional active date range."
  [slot ^LocalDate d]
  (let [mask  (or (:schedule-slots/days-of-week slot) 127)
        mask  (if (zero? mask) 127 mask)
        from  (->local-date (:schedule-slots/active-from slot))
        until (->local-date (:schedule-slots/active-until slot))]
    (and (not (zero? (bit-and mask (dow-bit d))))
         (or (nil? from)  (not (.isBefore d from)))
         (or (nil? until) (not (.isAfter d until))))))

(defn- occurrence
  "The occurrence of `slot` on date `d`: `{:slot :start :end :override?}`. `:end`
   is nil when the slot has no block duration (once/count fills run as long as
   their items do)."
  [slot ^LocalDate d ^ZoneId zone]
  (let [^Duration tod (:schedule-slots/start-time slot)
        start (.plus (.toInstant (.atStartOfDay d zone)) tod)
        ^Duration dur (:schedule-slots/block-duration slot)]
    {:slot      slot
     :start     start
     :end       (when dur (.plus start dur))
     :override? (override? slot)}))

(defn- overlaps? [{bs :start be :end} {os :start oe :end}]
  (if be
    (and (.isBefore ^Instant bs ^Instant oe) (.isAfter ^Instant be ^Instant os))
    ;; open-ended base occurrence: suppressed if it starts inside the override
    (and (not (.isBefore ^Instant bs ^Instant os)) (.isBefore ^Instant bs ^Instant oe))))

(defn- with-span
  "`occ` narrowed to [start, end), with its slot's block duration matching."
  [occ ^Instant start ^Instant end]
  (-> occ
      (assoc :start start :end end)
      (assoc-in [:slot :schedule-slots/block-duration] (Duration/between start end))))

(defn- clip-one
  "Pieces of base occurrence `b` left after removing override window `o`."
  [b o]
  (if-not (overlaps? b o)
    [b]
    (if (nil? (:end b))
      []
      (let [{bs :start be :end} b
            {os :start oe :end} o]
        (cond-> []
          (.isBefore ^Instant bs ^Instant os) (conj (with-span b bs os))
          (.isAfter ^Instant be ^Instant oe)  (conj (with-span b oe be)))))))

(defn- clip-around [overrides base]
  (reduce (fn [pieces o] (into [] (mapcat #(clip-one % o)) pieces))
          [base]
          overrides))

(defn occurrences-between
  "Every occurrence that starts in [from, to), with base occurrences clipped
   around override occurrences, sorted by start (overrides first on a tie).
   The day before `from` is included when resolving overlaps, so an overnight
   strip from the previous evening is clipped correctly."
  [slots ^Instant from ^Instant to zone-id]
  (let [zone   (ZoneId/of zone-id)
        first-d (.minusDays (.toLocalDate (ZonedDateTime/ofInstant from zone)) 1)
        last-d  (.toLocalDate (ZonedDateTime/ofInstant to zone))
        dates  (take-while #(not (.isAfter ^LocalDate % last-d))
                           (iterate #(.plusDays ^LocalDate % 1) first-d))
        raw    (for [d dates
                     s slots
                     :when (and (:schedule-slots/start-time s) (airs-on? s d))]
                 (occurrence s d zone))
        {ovr true base false} (group-by :override? raw)
        ovr    (filter :end ovr)
        pieces (concat ovr (mapcat #(clip-around ovr %) base))]
    (->> pieces
         (filter (fn [{:keys [^Instant start ^Instant end]}]
                   (and (not (.isBefore start from))
                        (.isBefore start to)
                        (or (nil? end) (.isAfter end start)))))
         (sort-by (juxt :start (comp not :override?))))))

(def ^:private lookahead-days
  "How far ahead `next-occurrence` searches. A week covers any weekday mask; the
   extra day absorbs time-zone and overnight edges."
  8)

(defn next-occurrence
  "The first occurrence starting at or after `from`, or nil when no slot airs
   within the next `lookahead-days` days."
  [slots ^Instant from zone-id]
  (first (occurrences-between slots from (.plus from (Duration/ofDays lookahead-days))
                              zone-id)))
