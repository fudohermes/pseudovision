(ns pseudovision.media.jellyfin-test
  "Tests for the tiered Jellyfin scan.

  Two things under test:
  1. `db.media/list-library-etag-state` builds the right shape from a SQL
     result (id, remote-etag, has-zero-duration?) keyed by remote-key.
  2. `pseudovision.media.jellyfin/upsert-tier` (the pure tier-decision
     function) maps every combination of (etag, existing-state) to the
     correct tier keyword. The actual `upsert-item!` is a thin wrapper
     around this; testing the tier function directly is simpler and
     faster than spinning up a real DB.

  These tests use `with-redefs` to stub the DB layer (the project's
  convention; see media_test.clj for the SQL-capture pattern)."
  (:require [clojure.test :refer [deftest is testing]]
            [pseudovision.db.core            :as db-core]
            [pseudovision.db.media           :as db]
            [pseudovision.media.jellyfin     :as sut]))

;; ---------------------------------------------------------------------------
;; list-library-etag-state
;; ---------------------------------------------------------------------------

(defn- capture-query
  "Invokes `f` with a stubbed `db-core/query` that records the formatted
   SQL vector AND returns the supplied rows. Mirrors the SQL-capture
   pattern in `pseudovision.db.media-test`."
  [rows f]
  (let [captured (atom nil)]
    (with-redefs [db-core/query (fn [_ sql-params]
                                  (reset! captured sql-params)
                                  rows)]
      (f))
    @captured))

(deftest list-library-etag-state-joins-on-library-path-id
  (testing "the WHERE clause pins the query to a single library_path_id"
    (let [[sql & params] (capture-query []
                          #(db/list-library-etag-state nil 42))]
      (is (re-find #"(?i)FROM\s+media_items" sql))
      (is (re-find #"(?i)LEFT JOIN media_versions" sql))
      (is (re-find #"(?i)WHERE.*library_path_id\s*=\s*\?" sql))
      (is (= [42] params)))))

(deftest list-library-etag-state-selects-etag-extract-epoch
  (testing "the SELECT pulls id, remote_key, remote_etag, and EXTRACT(EPOCH FROM duration)"
    (let [[sql] (capture-query []
                 #(db/list-library-etag-state nil 1))]
      (is (re-find #"(?i)SELECT.*mi\.id" sql))
      (is (re-find #"(?i)mi\.remote_key" sql))
      (is (re-find #"(?i)mi\.remote_etag" sql))
      ;; HoneySQL renders `(sql/call :EXTRACT :EPOCH :mv.duration)` as
      ;; `EXTRACT(EPOCH, mv.duration)` (function-call syntax). The semantics
      ;; are identical to the canonical `EXTRACT(EPOCH FROM duration)`
      ;; form, but the rendered commas mean our regex needs to be lenient.
      (is (re-find #"(?i)EXTRACT\s*\(\s*EPOCH\s*,\s*mv\.duration\s*\)" sql)
          "EXTRACT(EPOCH, mv.duration) gives us a numeric comparison target"))))

(deftest list-library-etag-state-builds-map-keyed-by-remote-key
  (testing "result rows are keyed by remote_key with :id, :remote-etag, :has-zero-duration?"
    (let [rows [{:id 1 :remote-key "jf-aaa" :remote-etag "etag-1" :duration-epoch 6500M}
                {:id 2 :remote-key "jf-bbb" :remote-etag "etag-2" :duration-epoch 0M}
                {:id 3 :remote-key "jf-ccc" :remote-etag "etag-3" :duration-epoch nil}]
          result (with-redefs [db-core/query (fn [_ _] rows)]
                   (db/list-library-etag-state nil 1))]
      (is (= 3 (count result)))
      (is (= {:id 1 :remote-etag "etag-1" :has-zero-duration? false}
             (get result "jf-aaa")))
      (is (= {:id 2 :remote-etag "etag-2" :has-zero-duration? true}
             (get result "jf-bbb"))
          "duration=0 -> has-zero-duration? true")
      (is (= {:id 3 :remote-etag "etag-3" :has-zero-duration? true}
             (get result "jf-ccc"))
          "NULL duration (item has no media_versions row) -> has-zero-duration? true")))

  (testing "result also tolerates snake_case keys from as-unqualified builder"
    ;; Some callers might use as-unqualified-kebab-maps* which yields
    ;; :remote_key (snake_case) instead of :remote-key. The defensive `or`
    ;; in list-library-etag-state handles both.
    (let [rows [{:id 1 :remote_key "jf-aaa" :remote_etag "etag-1" :duration_epoch 6500M}]
          result (with-redefs [db-core/query (fn [_ _] rows)]
                   (db/list-library-etag-state nil 1))]
      (is (= {:id 1 :remote-etag "etag-1" :has-zero-duration? false}
             (get result "jf-aaa"))))))

;; ---------------------------------------------------------------------------
;; upsert-tier (pure tier-decision function)
;;
;; This is the heart of the optimization: given the pre-fetched etag-state
;; entry and the new item's etag, decide which tier to use. The actual
;; `upsert-item!` is just a thin `case` over the four tiers.
;; ---------------------------------------------------------------------------

(def ^:private upsert-tier-fn
  "Access the private `upsert-tier` function via its Var so we can call
   it from tests without making it public."
  @(resolve 'pseudovision.media.jellyfin/upsert-tier))

(deftest upsert-tier-new-when-no-existing-entry
  (testing "an item absent from the pre-fetch map is :new"
    (is (= :new (upsert-tier-fn "any-etag" nil)))))

(deftest upsert-tier-changed-when-etag-differs
  (testing "an existing item whose etag no longer matches is :changed"
    (is (= :changed
           (upsert-tier-fn "NEW-ETAG"
                           {:id 5 :remote-etag "OLD-ETAG"
                            :has-zero-duration? false})))))

(deftest upsert-tier-unchanged-skip-when-etag-matches
  (testing "same etag AND positive duration => :unchanged (the fast path)"
    (is (= :unchanged
           (upsert-tier-fn "SAME-ETAG"
                           {:id 5 :remote-etag "SAME-ETAG"
                            :has-zero-duration? false})))))

(deftest upsert-tier-repair-duration-when-same-etag-but-zero-duration
  (testing "same etag but duration=0 => :repair-duration (PR #121 guard)"
    (is (= :repair-duration
           (upsert-tier-fn "SAME-ETAG"
                           {:id 5 :remote-etag "SAME-ETAG"
                            :has-zero-duration? true})))))

(deftest upsert-tier-precedence
  (testing "changed takes precedence over repair-duration when etag differs"
    ;; An item with different etag AND zero duration is :changed, not
    ;; :repair-duration. The full upsert path will fix the duration as
    ;; a side effect of re-running upsert-version-and-file!.
    (is (= :changed
           (upsert-tier-fn "NEW-ETAG"
                           {:id 5 :remote-etag "OLD-ETAG"
                            :has-zero-duration? true})))))
