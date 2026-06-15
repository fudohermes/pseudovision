(ns pseudovision.scheduling.enumerators
  "Collection enumerators: stateful iterators over ordered item sequences.

   An enumerator is a map with at minimum:
     :items   — the full item vector
     :index   — current position (0-based)
     :seed    — RNG seed (for reproducible shuffle)

   The enumerator protocol is implemented as plain functions that return
   [item updated-enumerator], or nil when the sequence is exhausted
   (only relevant for non-looping modes — currently all enumerators loop).

   The `key` of an enumerator (used in the cursor) is a namespaced string
   like \"collection:42\" or \"collection:42:child:3\"."
  (:require [pseudovision.util.time :as t])
  (:import [java.util Random Collections ArrayList]))

;; ---------------------------------------------------------------------------
;; Shuffle helpers
;; ---------------------------------------------------------------------------

(defn- shuffled-indices
  "Returns a vector of 0..n-1 shuffled using the given seed."
  [n seed]
  (let [arr (ArrayList. ^java.util.Collection (range n))
        rng (Random. seed)]
    (Collections/shuffle arr rng)
    (vec arr)))

(defn- rotate-vec
  "Rotates vector v so that index start becomes the first element."
  [v start]
  (if (zero? start)
    v
    (into (subvec v start) (subvec v 0 start))))

;; ---------------------------------------------------------------------------
;; Enumerator constructors
;; ---------------------------------------------------------------------------

(defmulti make-enumerator
  "Builds an initial enumerator for a collection and playback order.
   Dispatch on playback-order keyword."
  (fn [_items playback-order _opts] playback-order))

(defmethod make-enumerator :chronological [items _ _opts]
  {:items (vec items) :index 0 :playback-order :chronological})

(defmethod make-enumerator :random [items _ {:keys [seed] :or {seed 0}}]
  ;; Re-shuffles on each full pass; seed advances by 1 each time so successive
  ;; passes produce different orderings.
  {:items   (vec items)
   :index   0
   :seed    seed
   :order   (shuffled-indices (count items) seed)
   :playback-order :random})

(defmethod make-enumerator :shuffle [items _ {:keys [seed] :or {seed 0}}]
  ;; Like :random but the shuffle order is fixed for the lifetime of the seed
  ;; (i.e. the same seed always yields the same permutation across rebuilds).
  {:items   (vec items)
   :index   0
   :seed    seed
   :order   (shuffled-indices (count items) seed)
   :playback-order :shuffle})

(defmethod make-enumerator :semi-sequential [items _ {:keys [seed batch-size] :or {seed 0 batch-size 5}}]
  ;; Play N items sequentially, then jump to a random position and repeat.
  ;; batch-offset tracks which batch we're on (for deterministic random jumps).
  {:items            (vec items)
   :index            0
   :seed             seed
   :batch-size       batch-size
   :batch-offset     0
   :within-batch-idx 0
   :playback-order   :semi-sequential})

(defmethod make-enumerator :season-episode [items _ _opts]
  ;; Items are pre-sorted by (season-id, episode-position) so that next-item
  ;; can delegate to the plain chronological walk.
  {:items          (vec (sort-by (juxt :media-items/parent_id
                                       :media-items/position) items))
   :index          0
   :playback-order :season-episode})

(defmethod make-enumerator :default [items order _opts]
  ;; Unknown orders fall through to a plain chronological walk.
  {:items (vec items) :index 0 :playback-order order})

;; ---------------------------------------------------------------------------
;; next-item: advance the enumerator one step, returning [item enumerator]
;; ---------------------------------------------------------------------------

(defmulti next-item
  "Returns [item updated-enumerator].  Wraps around on exhaustion.
   Raises an error with diagnostic info if items list is empty."
  (fn [enumerator] (:playback-order enumerator)))

(defmethod next-item :chronological [{:keys [items index] :as e}]
  (let [n     (count items)]
    (when (zero? n)
      (throw (ex-info "Cannot enumerate over empty items list"
                     {:playback-order :chronological
                      :enumerator e})))
    (let [idx   (mod index n)
          item  (nth items idx)]
      [item (assoc e :index (inc index))])))

(defmethod next-item :random [{:keys [items index order seed] :as e}]
  ;; On each new pass (pos wraps to 0) generate a fresh shuffle with an
  ;; incremented seed so successive passes differ.
  (let [n   (count items)]
    (when (zero? n)
      (throw (ex-info "Cannot enumerate over empty items list"
                     {:playback-order :random
                      :enumerator e})))
    (let [pos (mod index n)
          [order' seed'] (if (zero? pos)
                           (let [new-seed (+ seed 1)]
                             [(shuffled-indices n new-seed) new-seed])
                           [order seed])
          item (nth items (nth order' pos))]
      [item (assoc e :index (inc index) :order order' :seed seed')])))

(defmethod next-item :shuffle [{:keys [items index order] :as e}]
  (let [n    (count items)]
    (when (zero? n)
      (throw (ex-info "Cannot enumerate over empty items list"
                     {:playback-order :shuffle
                      :enumerator e})))
    (let [idx  (mod index n)
          item (nth items (nth order idx))]
      [item (assoc e :index (inc index))])))

(defmethod next-item :semi-sequential [{:keys [items index seed batch-size batch-offset within-batch-idx] :as e}]
  ;; Play batch-size items sequentially, then jump randomly to a new starting point
  (let [n (count items)]
    (when (zero? n)
      (throw (ex-info "Cannot enumerate over empty items list"
                     {:playback-order :semi-sequential
                      :enumerator e})))
    (if (>= within-batch-idx batch-size)
      ;; Batch complete - pick new random start position
      (let [new-offset  (inc batch-offset)
            rng         (Random. (+ seed new-offset))
            new-start   (.nextInt rng n)
            item        (nth items new-start)]
        [item (assoc e :index (inc index)
                       :batch-offset new-offset
                       :within-batch-idx 1)])
      ;; Continue in current batch
      (let [current-start (mod index n)
            item          (nth items (mod (+ current-start within-batch-idx) n))]
        [item (assoc e :index (inc index)
                       :within-batch-idx (inc within-batch-idx))]))))

(defmethod next-item :season-episode [e]
  ((get-method next-item :chronological) e))

(defmethod next-item :default [e]
  ((get-method next-item :chronological) e))

;; ---------------------------------------------------------------------------
;; Cursor serialisation
;; ---------------------------------------------------------------------------

(defn enumerator->cursor
  "Extracts the serializable resumption state from an enumerator (index, seed, order name)."
  [{:keys [index seed playback-order batch-size batch-offset within-batch-idx]}]
  (cond-> {:index          index
           :seed           (or seed 0)
           :playback-order (name playback-order)}
    batch-size       (assoc :batch-size batch-size)
    batch-offset     (assoc :batch-offset batch-offset)
    within-batch-idx (assoc :within-batch-idx within-batch-idx)))

(defn cursor->enumerator
  "Rebuilds an enumerator from its cursor snapshot, restoring the exact position."
  [items {:keys [index seed playback-order batch-size batch-offset within-batch-idx]}]
  (let [order (keyword playback-order)
        opts  (cond-> {:seed seed}
                batch-size (assoc :batch-size batch-size))
        e     (make-enumerator items order opts)]
    (cond-> (assoc e :index index)
      batch-offset     (assoc :batch-offset batch-offset)
      within-batch-idx (assoc :within-batch-idx within-batch-idx))))
