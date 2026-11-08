(ns pseudovision.http.api.tags-test
  (:require [clojure.test :refer [deftest is testing]]
            [ring.mock.request :as mock]
            [cheshire.core     :as json]
            [pseudovision.http.core :as http]))

(defn- make-test-handler []
  (http/make-handler {:db nil :ffmpeg {} :media {} :scheduling {}}))

(defn- parse-json-body [resp]
  (let [parsed (some-> resp :body (json/parse-string true))]
    (if (sequential? parsed) (vec parsed) parsed)))

(defn- post-json [path body]
  (-> (mock/request :post path)
      (mock/content-type "application/json")
      (mock/body (json/generate-string body))))

(deftest list-tags-returns-name-count-pairs
  (testing "GET /api/tags shapes rows as {:name :count}"
    (with-redefs [pseudovision.db.core/query
                  (fn [_ _] [{:metadata-tags/name "comedy" :count 12}
                             {:metadata-tags/name "short"  :count 4}])
                  pseudovision.db.core/query-one
                  (fn [_ _] {:count 2})]
      (let [resp ((make-test-handler) (mock/request :get "/api/tags"))
            body (parse-json-body resp)]
        (is (= 200 (:status resp)))
        (is (= 2 (count (:items body))))
        (is (= "comedy" (get-in body [:items 0 :name])))
        (is (= 12      (get-in body [:items 0 :count])))))))

(deftest list-tags-applies-facet-filters
  (testing "GET /api/tags?prefix=&library-id=&kind= scopes both queries"
    (let [seen (atom [])]
      (with-redefs [pseudovision.db.core/query
                    (fn [_ q] (swap! seen conj q)
                      [{:metadata-tags/name "channel:muse" :count 7}])
                    pseudovision.db.core/query-one
                    (fn [_ q] (swap! seen conj q) {:count 1})]
        (let [resp ((make-test-handler)
                    (mock/request :get "/api/tags?prefix=Channel:&library-id=49&kind=program"))
              body (parse-json-body resp)]
          (is (= 200 (:status resp)))
          (is (= "channel:muse" (get-in body [:items 0 :name])))
          (is (= 2 (count @seen)))
          (doseq [[sql-str & params] @seen]
            (is (re-find #"(?i)starts_with" sql-str))
            (is (re-find #"(?i)library_paths" sql-str))
            (is (some #{"channel:"} params))
            (is (some #{49} params))
            (is (some #{"program"} params))))))))

(deftest list-tags-unfiltered-skips-joins
  (testing "with no filters the listing stays a single-table query"
    (let [seen (atom [])]
      (with-redefs [pseudovision.db.core/query
                    (fn [_ q] (swap! seen conj q) [])
                    pseudovision.db.core/query-one
                    (fn [_ q] (swap! seen conj q) {:count 0})]
        ((make-test-handler) (mock/request :get "/api/tags"))
        (doseq [[sql-str] @seen]
          (is (not (re-find #"(?i)join" sql-str)))
          (is (not (re-find #"(?i)where" sql-str))))))))

(deftest add-tags-rejects-missing-tags-key
  (testing "POST /api/media-items/1/tags with no :tags key returns 400"
    (let [resp ((make-test-handler)
                (post-json "/api/media-items/1/tags" {:source "manual"}))
          body (parse-json-body resp)]
      (is (= 400 (:status resp)))
      (is (= "Request coercion failed" (:error body))))))

(deftest delete-tag-returns-204
  (testing "DELETE /api/media-items/1/tags/comedy succeeds"
    (with-redefs [pseudovision.db.core/execute-one! (fn [_ _] nil)]
      (let [resp ((make-test-handler)
                  (mock/request :delete "/api/media-items/1/tags/comedy"))]
        (is (= 204 (:status resp)))))))

(deftest delete-tag-rejects-non-integer-item-id
  (testing "DELETE /api/media-items/abc/tags/comedy returns 400"
    (let [resp ((make-test-handler)
                (mock/request :delete "/api/media-items/abc/tags/comedy"))
          body (parse-json-body resp)]
      (is (= 400 (:status resp)))
      (is (= "Request coercion failed" (:error body))))))
