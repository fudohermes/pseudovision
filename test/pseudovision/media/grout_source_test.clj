(ns pseudovision.media.grout-source-test
  (:require [clojure.test :refer [deftest is testing]]
            [pseudovision.db.core :as db]
            [pseudovision.media.grout :as grout]
            [pseudovision.media.grout-source :as sut])
  (:import [java.time Duration Instant]))

(def ^:private enabled-client (grout/client {:base-url "http://grout:8080"}))

;; ---------------------------------------------------------------------------
;; ingest-clip! guard rails
;; ---------------------------------------------------------------------------

(deftest ingest-skips-clip-without-path
  (with-redefs [db/query-one (fn [& _] (throw (Exception. "no DB expected")))]
    (is (nil? (sut/ingest-clip! nil 1 {:id "a" :duration-ms 5000})))
    (is (nil? (sut/ingest-clip! nil 1 {:id "a" :path "" :duration-ms 5000})))))

(deftest ingest-skips-clip-without-positive-duration
  (with-redefs [db/query-one (fn [& _] (throw (Exception. "no DB expected")))]
    (is (nil? (sut/ingest-clip! nil 1 {:id "a" :path "/data/a.mp4"})))
    (is (nil? (sut/ingest-clip! nil 1 {:id "a" :path "/data/a.mp4" :duration-ms 0})))
    (is (nil? (sut/ingest-clip! nil 1 {:id "a" :path "/data/a.mp4" :duration-ms -3})))))

(deftest ingest-returns-existing-item-without-insert
  (testing "an already-ingested clip is returned from the lookup, no transaction"
    (with-redefs [db/query-one (fn [_ _] {:media-items/id 42
                                          :duration (Duration/ofMillis 65000)})]
      (is (= {:media-items/id 42
              :media-versions/duration (Duration/ofMillis 65000)}
             (sut/ingest-clip! nil 1 {:id "a" :path "/data/a.mp4" :duration-ms 65000}))))))

(deftest ingest-existing-falls-back-to-clip-duration
  (testing "when the stored version has no duration, use the clip's duration-ms"
    (with-redefs [db/query-one (fn [_ _] {:media-items/id 42 :duration nil})]
      (is (= (Duration/ofMillis 65000)
             (:media-versions/duration
              (sut/ingest-clip! nil 1 {:id "a" :path "/data/a.mp4" :duration-ms 65000})))))))

;; ---------------------------------------------------------------------------
;; grout-filler-items
;; ---------------------------------------------------------------------------

(deftest grout-filler-items-empty-when-disabled
  (with-redefs [grout/find-filler (fn [& _] (throw (Exception. "should not query")))]
    (is (= [] (sut/grout-filler-items nil nil {} (Instant/now) (Instant/now) {})))))

(deftest grout-filler-items-derives-channel-and-window
  (let [captured (atom nil)
        from     (Instant/parse "2026-07-03T10:00:00Z")
        to       (Instant/parse "2026-07-03T10:01:30Z")   ; 90s gap
        clip     {:id "c1" :path "/data/c1.mp4" :duration-ms 65000}]
    (with-redefs [grout/find-filler        (fn [_ opts] (reset! captured opts) [clip])
                  sut/ensure-library-path! (fn [_ _] 7)
                  sut/ingest-clip!         (fn [_ _ c] {:media-items/id 1
                                                        :media-versions/duration
                                                        (Duration/ofMillis (:duration-ms c))})]
      (let [result (sut/grout-filler-items nil enabled-client
                                           {:channels/name "Britannia"}
                                           from to
                                           {:filler-presets/grout-tags ["daytime" "fun"]})]
        (is (= 1 (count result)))
        (is (= (Duration/ofMillis 65000) (:media-versions/duration (first result))))
        (let [opts @captured]
          (is (= "britannia"        (:channel opts)))
          (is (= ["daytime" "fun"]  (:tags opts)))
          (is (= 90000              (:max-ms opts)))
          (is (true?                (:random opts))))))))

(deftest grout-filler-items-generic-when-no-channel-name
  (let [captured (atom nil)]
    (with-redefs [grout/find-filler        (fn [_ opts] (reset! captured opts) [])
                  sut/ensure-library-path! (fn [_ _] 7)]
      (sut/grout-filler-items nil enabled-client {} (Instant/now) (Instant/now)
                              {:filler-presets/grout-tags ["ident"]})
      (is (not (contains? @captured :channel))))))

(deftest grout-filler-items-empty-when-grout-has-no-match
  (with-redefs [grout/find-filler        (fn [& _] [])
                sut/ensure-library-path! (fn [& _] (throw (Exception. "no ingest expected")))]
    (is (= [] (sut/grout-filler-items nil enabled-client
                                      {:channels/name "Britannia"}
                                      (Instant/now) (Instant/now)
                                      {:filler-presets/grout-tags ["x"]})))))

(deftest grout-filler-items-excludes-bumper-kind-clips
  (testing "bumpers are reserved for the dedicated small-gap injector, not regular filler"
    (let [clips [{:id "c1" :kind "filler" :path "/data/c1.mp4" :duration-ms 30000}
                 {:id "c2" :kind "bumper" :path "/data/c2.mp4" :duration-ms 10000}]]
      (with-redefs [grout/find-filler        (fn [& _] clips)
                    sut/ensure-library-path! (fn [_ _] 7)
                    sut/ingest-clip!         (fn [_ _ c] {:media-items/id (:id c)
                                                          :media-versions/duration
                                                          (Duration/ofMillis (:duration-ms c))})]
        (let [result (sut/grout-filler-items nil enabled-client
                                             {:channels/name "Britannia"}
                                             (Instant/now) (Instant/now)
                                             {:filler-presets/grout-tags ["x"]})]
          (is (= ["c1"] (map :media-items/id result))))))))

;; ---------------------------------------------------------------------------
;; grout-bumper-items
;; ---------------------------------------------------------------------------

(deftest grout-bumper-items-empty-when-disabled
  (with-redefs [grout/find-filler (fn [& _] (throw (Exception. "should not query")))]
    (is (= [] (sut/grout-bumper-items nil nil {} 4000 6000)))))

(deftest grout-bumper-items-queries-bumper-kind-with-channel-and-window
  (let [captured (atom nil)
        clip     {:id "b1" :path "/data/b1.mp4" :duration-ms 5000}]
    (with-redefs [grout/find-filler        (fn [_ opts] (reset! captured opts) [clip])
                  sut/ensure-library-path! (fn [_ _] 7)
                  sut/ingest-clip!         (fn [_ _ c] {:media-items/id 1
                                                        :media-versions/duration
                                                        (Duration/ofMillis (:duration-ms c))})]
      (let [result (sut/grout-bumper-items nil enabled-client
                                           {:channels/name "Britannia"}
                                           4000 6000)]
        (is (= 1 (count result)))
        (let [opts @captured]
          (is (= "bumper"    (:kind opts)))
          (is (= "britannia" (:channel opts)))
          (is (= 4000        (:min-ms opts)))
          (is (= 6000        (:max-ms opts)))
          (is (true?         (:random opts))))))))

(deftest grout-bumper-items-empty-when-grout-has-no-match
  (with-redefs [grout/find-filler        (fn [& _] [])
                sut/ensure-library-path! (fn [& _] (throw (Exception. "no ingest expected")))]
    (is (= [] (sut/grout-bumper-items nil enabled-client
                                      {:channels/name "Britannia"}
                                      4000 6000)))))

;; ---------------------------------------------------------------------------
;; content sync — program-tags
;; ---------------------------------------------------------------------------

(deftest program-tags-passthrough-and-channel-synthesis
  (testing "grout tags pass through (trimmed, deduped) + channel:<slug> synthesized"
    (is (= ["daytime" "fun" "channel:britannia"]
           (#'sut/program-tags {:tags ["daytime" " fun " "daytime"] :channel "Britannia"})))))

(deftest program-tags-no-channel-tag-when-blank
  (is (= ["a"] (#'sut/program-tags {:tags ["a"] :channel "  "})))
  (is (= []    (#'sut/program-tags {:tags [] :channel nil})))
  (is (= []    (#'sut/program-tags {}))))

(deftest program-tags-dedupes-existing-channel-tag
  (is (= ["channel:britannia"]
         (#'sut/program-tags {:tags ["channel:britannia"] :channel "britannia"}))))

(deftest program-tags-filters-grout-internal-tags
  (testing "parent-directory:, filename:, content-type: are intake/audit leaks — never mirror them"
    ;; The "real" tags are educational / history / etc; the noisy ones
    ;; like parent-directory:food-shorts get dropped.
    (is (= ["educational" "history" "channel:chronicles"]
           (#'sut/program-tags
            {:tags ["parent-directory:food-shorts"
                    "educational"
                    "filename:2025-09-26 AT&T Archives — The Dew Line (2025).mp4"
                    "content-type:filler"
                    "history"
                    "  parent-directory:trimmed-with-whitespace  "]
             :channel "Chronicles"}))))
  (testing "filter handles nil/non-string safely"
    (is (= ["a"]
           (#'sut/program-tags {:tags ["a" nil 42 "" "  "] :channel nil}))))
  (testing "all-internal yields only the synthesized channel tag"
    (is (= ["channel:britannia"]
           (#'sut/program-tags
            {:tags ["parent-directory:x" "filename:y.mp4" "content-type:filler"]
             :channel "Britannia"}))))
  (testing "prefix boundary check — `filename:` matches but not `filenameless:`"
    ;; Defensive: we want exact prefix, not substring
    (is (= ["filenameless-thing" "channel:britannia"]
           (#'sut/program-tags
            {:tags ["filenameless-thing" "filename:y.mp4"]
             :channel "Britannia"})))))

;; ---------------------------------------------------------------------------
;; find-or-create! — qualified vs unqualified id bug
;;
;; `db/query-one` returns rows with qualified keys (`{:media-sources/id 2}`)
;; because it uses `as-kebab-maps*`, but `db/execute-one!` returns rows with
;; unqualified keys (`{:id 2}`) because it uses `as-unqualified-kebab-maps*`.
;; A helper that switches between the two MUST look up `:id` from both paths
;; rather than the caller-supplied qualified key, or the insert branch
;; silently returns nil. That was the live bug on 2026-07-22: a prior
;; Grout sync crashed with
;;   "null value in column 'media_source_id' of relation 'libraries'"
;; because the libraries insert received a nil `media-source-id`.
;; ---------------------------------------------------------------------------

(deftest find-or-create-uses-id-from-both-branches
  (testing "lookup hit returns the row's id"
    ;; query-one uses the qualified-builder keymap but the row only
    ;; contains the column we selected (`:id`), which is unqualified by
    ;; definition — so `:id` is what we read.
    (with-redefs [db/query-one  (fn [_ _] {:id 42})
                  db/execute-one! (fn [& _] (throw (Exception. "no insert expected")))]
      (is (= 42 (#'sut/find-or-create! nil :media-sources :media-sources/id
                                       [:= :name "Grout"] {})))))
  (testing "lookup miss → insert; returns row's id (unqualified builder)"
    ;; Reproduces the live regression: db/query-one returns nil so the helper
    ;; falls through to db/execute-one!, which uses the unqualified builder
    ;; and yields {:id 7}. The old code looked up :<table>/id here and
    ;; silently got nil, which then violated the libraries.media_source_id
    ;; NOT NULL constraint downstream.
    (with-redefs [db/query-one  (fn [_ _] nil)
                  db/execute-one! (fn [_ _] {:id 7})]
      (is (= 7 (#'sut/find-or-create! nil :media-sources :media-sources/id
                                     [:= :name "Grout"] {})))))
  (testing "lookup miss → insert with genuinely nil id is an unrecoverable error"
    ;; The helper should not silently coerce nil-from-insert into a passed-back
    ;; nil, because downstream code (libraries.media-source-id) would then
    ;; violate a NOT NULL constraint. nil-forced-from-insert is impossible in
    ;; practice (SERIAL PKs always come back), so any future regression here
    ;; surfaces as a clear failure rather than a partial-catalog write.
    (with-redefs [db/query-one  (fn [_ _] nil)
                  db/execute-one! (fn [_ _] nil)]
      (is (nil? (#'sut/find-or-create! nil :media-sources :media-sources/id
                                       [:= :name "Grout"] {}))))))

;; ---------------------------------------------------------------------------
;; content sync — sync-program! guard rails + sync-programs! aggregation
;; ---------------------------------------------------------------------------

(deftest sync-program-skips-unusable-clips
  ;; Path/duration guards return :skipped before any DB work.
  (is (= :skipped (sut/sync-program! nil 1 {:id "a"})))
  (is (= :skipped (sut/sync-program! nil 1 {:id "a" :path "" :duration-ms 5})))
  (is (= :skipped (sut/sync-program! nil 1 {:id "a" :path "/x.mp4" :duration-ms 0})))
  (is (= :skipped (sut/sync-program! nil 1 {:id "a" :path "/x.mp4" :duration-ms -1}))))

(deftest sync-programs-disabled-returns-zeroes
  (with-redefs [grout/list-programs (fn [& _] (throw (Exception. "should not query")))]
    (is (= {:enabled false :total 0 :synced 0 :updated 0 :skipped 0 :errors 0}
           (sut/sync-programs! nil nil)))))

(deftest sync-programs-aggregates-outcomes
  (let [clips [{:id "1"} {:id "2"} {:id "3"} {:id "4"}]]
    (with-redefs [grout/enabled?                   (fn [_] true)
                  grout/list-programs              (fn [_] clips)
                  sut/ensure-content-library-path! (fn [_ _] 7)
                  sut/sync-program!                (fn [_ _ clip]
                                                     (case (:id clip)
                                                       "1" :synced
                                                       "2" :updated
                                                       "3" :skipped
                                                       "4" (throw (Exception. "boom"))))]
      (let [r (sut/sync-programs! nil {:base-url "http://grout:8080"})]
        (is (true? (:enabled r)))
        (is (= 4 (:total r)))
        (is (= 1 (:synced r)))
        (is (= 1 (:updated r)))
        (is (= 1 (:skipped r)))
        (is (= 1 (:errors r)) "a throwing sync-program! is counted as an error, not fatal")))))

;; ---------------------------------------------------------------------------
;; display-title
;;
;; PV's metadata.title for Grout-sourced items is derived from a fallback chain
;; because Grout bulk uploads leave name=null on most items. Without this,
;; /api/media/items/{id} returns name=null for ~97% of the Grout catalog and
;; the schedule renders them as opaque "Media Item #<n>" placeholders in
;; Marquee (see marquee/pages/media_detail.cljs:148).
;; ---------------------------------------------------------------------------

(deftest display-title-prefers-grout-name
  (testing "Grout's `name` field wins when present (e.g. Tunabrain-generated bumpers)"
    (is (= "Enigma Unveiled"
           (sut/display-title
            {:id "abc" :name "Enigma Unveiled"
             :tags ["channel:enigma-tv" "filename:enigma-1785...-5s-image.png"]})))))

(deftest display-title-falls-back-to-filename-tag
  (testing "bulk-uploaded programs without a name use the filename: intake tag"
    (is (= "veritasium:2024-12-31 The Man Who Took LSD and Changed The World (2024)"
           (sut/display-title
            {:id "abc"
             :tags ["parent-directory:veritasium"
                    "filename:2024-12-31 The Man Who Took LSD and Changed The World (2024).mp4"
                    "content-type:filler"
                    "channel:infobytes"]})))))

(deftest display-title-qualifies-bare-filename-with-parent-directory
  (testing "bare date-stamped filenames are disambiguated with parent-directory: when available"
    (is (= "veritasium:2024-12-10 34 Years Of Strandbeest Evolution (2024)"
           (sut/display-title
            {:id "abc"
             :tags ["parent-directory:veritasium"
                    "filename:2024-12-10 34 Years Of Strandbeest Evolution (2024).mp4"
                    "channel:infobytes"]})))))

(deftest display-title-falls-back-to-channel-placeholder
  (testing "no name and no filename tag → channel-aware placeholder"
    (is (= "Untitled infobytes program"
           (sut/display-title
            {:id "abc" :channel "infobytes" :tags []})))))

(deftest display-title-falls-back-to-grout-id
  (testing "no name, filename, or channel → opaque-but-stable Grout id placeholder"
    (is (= "Unknown Grout item (abc)"
           (sut/display-title {:id "abc" :tags []})))))

(deftest display-title-final-fallback
  (testing "absolute last resort"
    (is (= "Unknown"
           (sut/display-title {})))))

(deftest display-title-trims-whitespace-in-filename
  (testing "whitespace around the filename tag value is trimmed"
    (is (= "name with surrounding spaces"
           (sut/display-title
            {:id "abc" :tags ["filename:  name with surrounding spaces  .mp4"]})))))

(deftest display-title-treats-blank-filename-as-missing
  (testing "filename: with empty/whitespace-only value is treated as no filename"
    (is (= "Untitled britannia program"
           (sut/display-title
            {:id "abc" :channel "britannia"
             :tags ["filename:   " "parent-directory:" "channel:britannia"]})))))
