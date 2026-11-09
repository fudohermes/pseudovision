(ns pseudovision.scheduling.core-build-failure-test
  "A slot error inside build! must leave build_success=false on the playout.
   The in-transaction failure record is rolled back with the build, so build!
   has to write it again outside the transaction."
  (:require [clojure.test :refer [deftest is testing]]
            [next.jdbc :as jdbc]
            [pseudovision.db.channels :as channels-db]
            [pseudovision.db.playouts :as playout-db]
            [pseudovision.db.schedules :as schedules-db]
            [pseudovision.scheduling.core :as sched]))

(deftest slot-error-is-recorded-outside-the-rolled-back-transaction
  (let [in-tx?  (atom false)
        updates (atom [])]
    (with-redefs-fn
      {#'jdbc/transact (fn [db f & _]
                         (reset! in-tx? true)
                         (try (f db)
                              (catch Exception e
                                ;; rollback: discard what the tx wrote
                                (swap! updates (fn [us] (vec (remove :in-tx? us))))
                                (throw e))
                              (finally (reset! in-tx? false))))
       #'channels-db/get-channel                    (fn [_ _] {:channels/id 3})
       #'schedules-db/get-schedule                  (fn [_ _] {:schedules/name "s"})
       #'schedules-db/list-slots                    (fn [_ _] [{:schedule-slots/id 9
                                                                :schedule-slots/slot-index 0}])
       #'playout-db/list-event-spans-after          (fn [& _] [])
       #'playout-db/delete-non-manual-events-after! (fn [& _] nil)
       #'playout-db/recent-filler-airings           (fn [& _] [])
       #'playout-db/bulk-insert-events!             (fn [& _] nil)
       #'playout-db/update-playout!                 (fn [_ _ attrs]
                                                      (swap! updates conj
                                                             (assoc attrs :in-tx? @in-tx?)))
       #'pseudovision.scheduling.core/process-slot  (fn [& _] (throw (ex-info "boom" {})))}
      (fn []
        (is (thrown? Exception
                     (sched/build! :db {:lookahead-hours 24}
                                   {:playouts/id 1 :playouts/schedule-id 2
                                    :playouts/channel-id 3})))
        (testing "the committed playout update records the failure"
          (is (= 1 (count @updates)))
          (is (false? (:build-success (first @updates))))
          (is (re-find #"boom" (:build-message (first @updates)))))))))
