(ns pseudovision.scheduling.build-by-day-test
  "The day-based build loop picks each day's slots by date. The slot-cycle loop
   moved the cursor to a non-airing slot's next airing (a weekday strip on a
   Saturday jumped to Monday), skipping everything in between."
  (:require [clojure.test :refer [deftest is testing]]
            [pseudovision.scheduling.core :as sched])
  (:import [java.time Duration Instant]))

(defn- slot [id hh days]
  {:schedule-slots/id id
   :schedule-slots/anchor "fixed"
   :schedule-slots/fill-mode "block"
   :schedule-slots/start-time (Duration/ofHours hh)
   :schedule-slots/block-duration (Duration/ofHours 1)
   :schedule-slots/days-of-week days})

(deftest weekday-and-weekend-strips-both-air
  (let [emitted (atom [])]
    (with-redefs-fn
      {#'pseudovision.scheduling.core/apply-filler
       (fn [_ cursor & _] [[] cursor])
       #'pseudovision.scheduling.core/emit-occurrence
       (fn [_ cursor slot _ _ _ _]
         (let [from (:next-start cursor)
               to   (.plus ^Instant from ^Duration (:schedule-slots/block-duration slot))]
           (swap! emitted conj [(:schedule-slots/id slot) (str from)])
           [[{:start-at from :finish-at to}] (assoc cursor :next-start to)]))}
      (fn []
        ;; Sat 2026-11-07 12:00 UTC through Tue 2026-11-10 00:00 UTC
        (let [result (#'pseudovision.scheduling.core/build-by-day
                      nil {:next-start (Instant/parse "2026-11-07T12:00:00Z")}
                      [(slot :weekday 20 31) (slot :weekend 20 96)]
                      {} 1 (Instant/parse "2026-11-10T00:00:00Z") {:zone-id "UTC"})]
          (is (false? (:stalled? result)))
          (is (= [[:weekend "2026-11-07T20:00:00Z"]
                  [:weekend "2026-11-08T20:00:00Z"]
                  [:weekday "2026-11-09T20:00:00Z"]]
                 @emitted)))))))

(deftest day-based-only-for-all-fixed-schedules
  (is (sched/day-based? [(slot :a 1 127) (slot :b 2 127)]))
  (is (not (sched/day-based? [(slot :a 1 127) {:schedule-slots/anchor "sequential"}])))
  (is (not (sched/day-based? []))))
