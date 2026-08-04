(ns pseudovision.http.api.media-test
  (:require [clojure.test :refer [deftest is testing]]
            [ring.mock.request :as mock]
            [cheshire.core     :as json]
            [pseudovision.http.core    :as http]
            [pseudovision.jobs.runner  :as runner]
            [pseudovision.db.media     :as db]
            [pseudovision.media.jellyfin :as jellyfin]))

(defn- parse-json-body [resp]
  (some-> resp :body (json/parse-string true)))

(defn- await-terminal [r job-id]
  (loop [n 0]
    (let [info (runner/job-info r job-id)]
      (if (or (contains? #{:succeeded :failed} (:status info)) (>= n 200))
        info
        (do (Thread/sleep 15) (recur (inc n)))))))

(deftest scan-all-scans-only-jellyfin-libraries
  (testing "POST /api/media/scan-all scans Jellyfin-backed libraries and skips
            non-Jellyfin ones (e.g. the local grout-content library)"
    (let [scanned (atom [])]
      (with-redefs [db/list-libraries
                    (fn [_ _]
                      [{:libraries/id 1 :libraries/name "movies"        :libraries/media-source-id 10}
                       {:libraries/id 2 :libraries/name "grout-content" :libraries/media-source-id 20}])
                    db/get-media-source
                    (fn [_ sid] (case sid
                                  10 {:media-sources/kind "jellyfin"}
                                  20 {:media-sources/kind "local"}))
                    jellyfin/scan-library!
                    (fn [_ _ library] (swap! scanned conj (:libraries/name library)) nil)]
        (let [r       (runner/create {})
              handler (http/make-handler {:db nil :ffmpeg {} :media {} :scheduling {} :jobs r})
              resp    (handler (mock/request :post "/api/media/scan-all"))
              body    (parse-json-body resp)
              job-id  (get-in body [:job :id])
              info    (await-terminal r job-id)]
          (is (= 202 (:status resp)))
          (is (= "media/scan-all" (get-in body [:job :type])))
          (is (= :succeeded (:status info)))
          (is (= ["movies"] @scanned)
              "only the jellyfin library is scanned; grout-content is skipped")
          (is (= 1 (get-in info [:result :scanned])))
          (is (= 1 (get-in info [:result :skipped]))))))))

;; ---------------------------------------------------------------------------
;; GET /api/media/items/:id — display-title wiring
;;
;; Regression: the api/media.clj display-title-for-item fallback chain was
;; originally called with the *qualified-keyed* map from db/query-one (which
;; has keys like :m/title), not the unqualified one (which has :name, etc).
;; That made every branch of the fallback chain miss, so every item returned
;; :name "Unknown" — even Jellyfin items with a real metadata.title. The fix
;; is to unqualify-keys first, then pass the unqualified map to the display-
;; title fallback.
;; ---------------------------------------------------------------------------

(defn- stub-handler [item-row]
  (with-redefs [pseudovision.db.media/get-media-item (fn [_ _] item-row)]
    (let [handler ((requiring-resolve 'pseudovision.http.api.media/get-media-item-handler)
                   {:db nil})
          resp    (handler (mock/request :get "/api/media/items/1"))]
      ;; The handler already returns the response map directly (not via the
      ;; respond middleware in this test path), so the body is a plain map.
      {:status (:status resp) :body (:body resp)})))

(deftest get-media-item-returns-metadata-title-when-present
  (testing "Jellyfin item with metadata.title → that title is returned"
    ;; db/get-media-item's query uses `[:m.title :name]` which aliases the
    ;; SQL output column to `name`, so the row arrives with the qualified
    ;; key `:m/name`. After unqualify-keys that becomes the unqualified
    ;; `:name` key, which is what display-title-for-item looks up.
    (let [{:keys [status body]} (stub-handler
                                  {:media-items/id 1
                                   :m/name "Rear.Window.1954..."
                                   :media-items/remote-key "fc7619..."
                                   :media-items/kind "movie"})]
      (is (= 200 status))
      (is (= "Rear.Window.1954..." (:name body))))))

(deftest get-media-item-falls-back-to-remote-key
  (testing "Grout filler item with no metadata.title → remote-key is returned"
    (let [{:keys [status body]} (stub-handler
                                  {:media-items/id 1649133
                                   :m/name nil
                                   :media-items/remote-key "grout:5c707253-..."
                                   :media-items/kind "other_video"})]
      (is (= 200 status))
      (is (= "grout:5c707253-..." (:name body))))))

(deftest get-media-item-final-fallback-is-unknown
  (testing "no metadata.title AND no remote-key → literal \"Unknown\""
    (let [{:keys [status body]} (stub-handler
                                  {:media-items/id 1
                                   :m/name nil
                                   :media-items/remote-key nil
                                   :media-items/kind "other_video"})]
      (is (= 200 status))
      (is (= "Unknown" (:name body))))))
