(ns pseudovision.scheduling.occurrences-test
  (:require [clojure.test :refer [deftest is testing]]
            [pseudovision.scheduling.occurrences :as occ])
  (:import [java.time Duration Instant LocalDate]))

(def zone "UTC")

(defn- slot [id hh mm hours & {:as extra}]
  (merge {:schedule-slots/id id
          :schedule-slots/anchor "fixed"
          :schedule-slots/start-time (.plusMinutes (Duration/ofHours hh) mm)
          :schedule-slots/block-duration (Duration/ofMinutes (long (* 60 hours)))
          :schedule-slots/days-of-week 127}
         extra))

(defn- at [s] (Instant/parse s))

(defn- summary [occs]
  (mapv (fn [o] [(-> o :slot :schedule-slots/id) (str (:start o)) (str (:end o))]) occs))

;; 2026-11-07 is a Saturday, 2026-11-09 a Monday.
(def weekdays 31)
(def weekends 96)

(deftest weekday-strip-does-not-swallow-the-weekend
  (testing "a weekend strip airs on Saturday even though a weekday strip shares the time"
    (let [slots [(slot :wd 20 0 1 :schedule-slots/days-of-week weekdays)
                 (slot :we 20 0 1 :schedule-slots/days-of-week weekends)]]
      (is (= [:we "2026-11-07T20:00:00Z" "2026-11-07T21:00:00Z"]
             (first (summary [(occ/next-occurrence slots (at "2026-11-07T12:00:00Z") zone)]))))
      (is (= [[:we "2026-11-07T20:00:00Z" "2026-11-07T21:00:00Z"]
              [:we "2026-11-08T20:00:00Z" "2026-11-08T21:00:00Z"]
              [:wd "2026-11-09T20:00:00Z" "2026-11-09T21:00:00Z"]]
             (summary (occ/occurrences-between slots (at "2026-11-07T00:00:00Z")
                                               (at "2026-11-10T00:00:00Z") zone)))))))

(deftest date-range-bounds-a-slot
  (let [s (slot :x 9 0 1 :schedule-slots/active-from "2026-11-08"
                        :schedule-slots/active-until (LocalDate/parse "2026-11-09"))]
    (is (= ["2026-11-08T09:00:00Z" "2026-11-09T09:00:00Z"]
           (mapv second (summary (occ/occurrences-between [s] (at "2026-11-06T00:00:00Z")
                                                          (at "2026-11-12T00:00:00Z") zone)))))))

(deftest override-clips-the-base-strip
  (testing "a one-day override inside a base block splits it around the override"
    (let [base (slot :base 18 0 4)
          ovr  (slot :ovr 19 0 1.5 :schedule-slots/is-override true
                     :schedule-slots/active-from "2026-11-08"
                     :schedule-slots/active-until "2026-11-08")]
      (is (= [[:base "2026-11-08T18:00:00Z" "2026-11-08T19:00:00Z"]
              [:ovr  "2026-11-08T19:00:00Z" "2026-11-08T20:30:00Z"]
              [:base "2026-11-08T20:30:00Z" "2026-11-08T22:00:00Z"]]
             (summary (occ/occurrences-between [base ovr] (at "2026-11-08T00:00:00Z")
                                               (at "2026-11-09T00:00:00Z") zone))))
      (testing "the clipped piece carries a matching block duration"
        (is (= (Duration/ofMinutes 90)
               (-> (occ/occurrences-between [base ovr] (at "2026-11-08T20:00:00Z")
                                            (at "2026-11-09T00:00:00Z") zone)
                   first :slot :schedule-slots/block-duration))))
      (testing "the next day is untouched"
        (is (= [[:base "2026-11-09T18:00:00Z" "2026-11-09T22:00:00Z"]]
               (summary (occ/occurrences-between [base ovr] (at "2026-11-09T00:00:00Z")
                                                 (at "2026-11-10T00:00:00Z") zone))))))))

(deftest override-covering-a-whole-strip-replaces-it
  (let [base (slot :base 20 0 1)
        ovr  (slot :ovr 19 0 3 :schedule-slots/is-override true
                   :schedule-slots/active-from "2026-11-08"
                   :schedule-slots/active-until "2026-11-08")]
    (is (= [[:ovr "2026-11-08T19:00:00Z" "2026-11-08T22:00:00Z"]]
           (summary (occ/occurrences-between [base ovr] (at "2026-11-08T00:00:00Z")
                                             (at "2026-11-09T00:00:00Z") zone))))))

(deftest overnight-base-is-clipped-by-a-next-morning-override
  (let [base (slot :late 23 0 3)
        ovr  (slot :ovr 1 0 1 :schedule-slots/is-override true
                   :schedule-slots/active-from "2026-11-09"
                   :schedule-slots/active-until "2026-11-09")]
    (is (= [[:ovr  "2026-11-09T01:00:00Z" "2026-11-09T02:00:00Z"]]
           (summary (occ/occurrences-between [base ovr] (at "2026-11-09T00:30:00Z")
                                             (at "2026-11-09T03:00:00Z") zone))))
    (is (= [[:late "2026-11-08T23:00:00Z" "2026-11-09T01:00:00Z"]]
           (summary (occ/occurrences-between [base ovr] (at "2026-11-08T22:00:00Z")
                                             (at "2026-11-09T00:30:00Z") zone))))))

(deftest nothing-airs-returns-nil
  (is (nil? (occ/next-occurrence [(slot :x 9 0 1 :schedule-slots/active-until "2026-01-01")]
                                 (at "2026-11-08T00:00:00Z") zone))))
