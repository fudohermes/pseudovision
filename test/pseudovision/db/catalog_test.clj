(ns pseudovision.db.catalog-test
  "Tag/genre aggregates must count movies and programs as playable items, not
   only show episodes (feasibility reads :episode_count as the pool size)."
  (:require [clojure.test :refer [deftest is testing]]
            [pseudovision.db.catalog :as sut]
            [pseudovision.db.core :as db]))

(defn- capture [f]
  (let [captured (atom nil)]
    (with-redefs [db/query-unqualified (fn [_ sql] (reset! captured sql) [])]
      (f))
    (first @captured)))

(deftest aggregates-count-flat-items-as-one
  (doseq [[label f] {"tag"   #(sut/list-tag-aggregates nil nil)
                     "genre" #(sut/list-genre-aggregates nil nil)}]
    (testing label
      (is (re-find #"CASE WHEN mi\.kind = 'show' THEN COALESCE\(e\.count, 0\) ELSE 1 END"
                   (capture f))))))
