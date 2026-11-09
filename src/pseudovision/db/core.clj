(ns pseudovision.db.core
  (:require [migratus.core   :as migratus]
            [next.jdbc       :as jdbc]
            [next.jdbc.connection :as conn]
            [next.jdbc.result-set :as rs]
            [cheshire.core   :as json]
            [camel-snake-kebab.core :as csk]
            [taoensso.timbre :as log]
            [aero.core       :as aero])
  (:import [com.zaxxer.hikari HikariDataSource]
           [org.postgresql.util PGInterval PGobject]
           [java.sql ResultSet ResultSetMetaData Types]
           [java.time Duration]
           [javax.sql DataSource]
           [java.sql Connection]
           [java.lang.reflect Proxy InvocationHandler]))

;; ---------------------------------------------------------------------------
;; Boolean column coercion
;;
;; PostgreSQL via the JDBC text protocol returns BOOLEAN columns through
;; `.getObject` as the literal strings "t" / "f" rather than as
;; `java.lang.Boolean`. The default `next.jdbc.result-set/as-kebab-maps`
;; builder hands those strings through unchanged, which then breaks Malli
;; response schemas that declare `[:is-enabled :boolean]` (the live symptom:
;; /api/channels and /api/channels/{id} returning HTTP 500 with
;; "Request coercion failed" for every channel row, verified 2026-07-17).
;;
;; `boolean-coerce-column-reader` is a column-reader suitable for
;; `next.jdbc.result-set/as-maps-adapter`: for BOOLEAN columns it reads
;; `.getBoolean` (canonical `Boolean`); NULLs stay `nil` via `.wasNull` so
;; nullable boolean columns keep their NULL distinction. All other column
;; types continue to use `.getObject`.
;; ---------------------------------------------------------------------------

(defn boolean-coerce-column-reader
  "Column-reader that reads BOOLEAN columns as `java.lang.Boolean` (instead
   of the \"t\"/\"f\" strings that PostgreSQL hands back via `.getObject` in
   text protocol mode) and lets NULLs stay `nil`. Non-boolean columns fall
   through to `.getObject` exactly as the default would.

   Use with `next.jdbc.result-set/as-maps-adapter` to wrap any map builder:
   (rs/as-maps-adapter rs/as-kebab-maps db/boolean-coerce-column-reader)"
  [^ResultSet rs ^ResultSetMetaData rsmeta ^Integer i]
  (if (= Types/BOOLEAN (.getColumnType rsmeta i))
    (let [v (.getBoolean rs i)]
      (if (.wasNull rs) nil v))
    (.getObject rs i)))

(def ^:private as-kebab-maps*
  "Builder-fn equivalent to `rs/as-kebab-maps` but with BOOLEAN columns
   coerced to real `Boolean` values via `boolean-coerce-column-reader`."
  (rs/as-maps-adapter rs/as-kebab-maps boolean-coerce-column-reader))

(def ^:private as-unqualified-kebab-maps*
  "Builder-fn equivalent to `rs/as-unqualified-kebab-maps` but with BOOLEAN
   columns coerced to real `Boolean` values."
  (rs/as-maps-adapter rs/as-unqualified-kebab-maps boolean-coerce-column-reader))

;; ---------------------------------------------------------------------------
;; PostgreSQL type coercions
;; ---------------------------------------------------------------------------

(defn- pg-interval->duration [^PGInterval v]
  (let [total-seconds (+ (* (.getDays v) 86400)
                         (* (.getHours v) 3600)
                         (* (.getMinutes v) 60)
                         (.getSeconds v))]
    (Duration/ofMillis (long (* total-seconds 1000)))))

(extend-protocol rs/ReadableColumn
  PGInterval
  (read-column-by-label [v _] (pg-interval->duration v))
  (read-column-by-index [v _ _] (pg-interval->duration v))
  java.sql.Timestamp
  (read-column-by-label [v _] (.toInstant v))
  (read-column-by-index [v _ _] (.toInstant v))
  java.sql.Date
  ;; DATE columns (e.g. schedule_slots.active_from) read as LocalDate, which the
  ;; response middleware renders as an ISO date string.
  (read-column-by-label [v _] (.toLocalDate ^java.sql.Date v))
  (read-column-by-index [v _ _] (.toLocalDate ^java.sql.Date v))
  java.sql.Array
  ;; SQL arrays (e.g. TEXT[] columns) arrive as PgArray, which is not seqable.
  ;; Decode to a Clojure vector so callers can treat them as ordinary seqs.
  (read-column-by-label [v _] (vec (.getArray v)))
  (read-column-by-index [v _ _] (vec (.getArray v)))
  PGobject
  (read-column-by-label [v _]
    (if (= "jsonb" (.getType v))
      (json/parse-string (.getValue v) csk/->kebab-case-keyword)
      (.getValue v)))
  (read-column-by-index [v _ _]
    (if (= "jsonb" (.getType v))
      (json/parse-string (.getValue v) csk/->kebab-case-keyword)
      (.getValue v))))

;; ---------------------------------------------------------------------------
;; Connection pool
;; ---------------------------------------------------------------------------

(defn make-datasource
  "Creates a HikariCP connection pool from a config map with keys
   :jdbc-url, :username, :password."
  [{:keys [jdbc-url username password]}]
  (log/info "Creating datasource with:"
            {:jdbc-url jdbc-url
             :username username
             :password-present? (boolean password)})
  (conn/->pool HikariDataSource
               {:jdbcUrl          jdbc-url
                :username         username
                :password         password
                ;; Bumped from 10 to 16 so the `migrate` init container's
                ;; long-lived connection checkout (see `with-single-connection`)
                ;; never starves the application container's request
                ;; connections under burst load.
                :maximumPoolSize  16
                :minimumIdle      1}))

(defn close-datasource! [^HikariDataSource ds]
  (.close ds))

;; ---------------------------------------------------------------------------
;; Single-connection wrappers for migratus
;;
;; See `migratus-config` (in `run-migrations!` below) for the long-form
;; rationale. Briefly:
;;
;; `migratus.migration.sql/run-sql` splits multi-statement SQL with `--;;`
;; and runs each chunk inside `next.jdbc/with-transaction`. With a Hikari
;; pool (:db {:datasource pool}) every `next.jdbc` call extracts a fresh
;; connection, so migratus's `mark-*` book-keeping and the actual
;; migration SQL end up on different physical connections, which the
;; PostgreSQL driver surfaces as `PSQLException: Too many update results
;; were returned` (verified live 2026-07-17 on the unfixed
;; pseudovision-5579d57688-s7xl8 migrate init container; log excerpt in
;; the `run-migrations!` / `migrate!` docstrings below).
;;
;; Two wrappers pin every migratus call to one physical connection:
;;
;;  1. `SingleConnectionDataSource` — a `javax.sql.DataSource` whose
;;     `.getConnection` ALWAYS returns the SAME `java.sql.Connection`
;;     instance.  Migratus's `Transactable/javax.sql.DataSource/-transact`
;;     extension treats this as a real `DataSource`, so the bookkeeping
;;     calls (`mark-reserved`, `complete?`, `mark-complete`,
;;     `mark-unreserved`) and the actual migration SQL all hit the same
;;     physical connection.
;;
;;  2. `no-close-conn` — a `java.lang.reflect.Proxy` of `Connection`
;;     whose `.close` is a no-op.  `next.jdbc/with-transaction` for a
;;     `DataSource` is implemented as
;;     `(with-open [con (.getConnection ds)] (transact* con body))`,
;;     so each `with-transaction` would call `.close` on the connection
;;     and Hikari would return it to the pool mid-migration.  Silencing
;;     `.close` keeps the Hikari checkout alive until the outer
;;     `with-open` in `with-single-connection` exits.
;; ---------------------------------------------------------------------------

(deftype SingleConnectionDataSource [^Connection conn]
  DataSource
  (getConnection [_]      conn)
  (getConnection [_ _ _]  conn))

(defn- no-close-conn
  "Returns a `java.sql.Connection` proxy of `conn` whose `.close` is a
   no-op. All other methods (including `isClosed`, `commit`,
   `setAutoCommit`, `prepareStatement`, etc.) forward to `conn`.

   Implemented via `java.lang.reflect.Proxy` so we don't have to
   reimplement the ~30 methods of `java.sql.Connection` by hand."
  [^Connection conn]
  (let [handler (proxy [InvocationHandler] []
                  (invoke [_proxy method args]
                    (if (= "close" (.getName method))
                      nil
                      (try
                        (.invoke method conn (object-array args))
                        (catch java.lang.reflect.InvocationTargetException e
                          (throw (.getCause e)))))))]
    (Proxy/newProxyInstance
      (.getContextClassLoader (Thread/currentThread))
      (into-array Class [Connection])
      handler)))

;; ---------------------------------------------------------------------------
;; Migrations
;; ---------------------------------------------------------------------------

(defn- with-single-connection
  "Run `(f cfg)` against one Hikari checkout, releasing the connection
   back to the pool when `f` returns.

   The `cfg` map has `:db {:datasource scd}`, where `scd` is a
   `SingleConnectionDataSource` wrapping a `no-close-conn` proxy of the
   raw Hikari checkout. Every migratus call resolves to the same
   physical connection, so migratus's `mark-*` bookkeeping and the
   migration SQL run on one connection, in one transaction. See the
   `Single-connection wrappers for migratus` section above for the full
   rationale and the live repro."
  [ds f]
  (with-open [raw-conn (jdbc/get-connection ds)]
    (let [proxy (no-close-conn raw-conn)
          scd   (SingleConnectionDataSource. proxy)
          cfg   {:store         :database
                 :migration-dir "migrations"
                 :db            {:datasource scd}}]
      (f cfg))))

(defn migrate!
  "Run all outstanding migrations against `ds` (a `HikariDataSource`).

  Pins every migratus call to ONE Hikari checkout (see
  `with-single-connection`) so all migratus book-keeping and SQL
  statements share a single physical connection and a single
  transaction.

  ## Why

  The previous shape `{:db {:datasource pool}}` was broken with
  `next.jdbc` + HikariCP:

  1. **Batched-update \"too many update results\" error.** Migratus
     splits multi-statement SQL with `--;;` and runs each chunk inside
     `next.jdbc/with-transaction`. When `:db {:datasource pool}`
     reaches `migratus.migration.sql/run-sql`, the
     `next.jdbc/with-transaction` macroexpands to
     `(transact {:datasource pool} ...)`. The `Object/-transact`
     extension calls `(p/get-datasource pool)` and on the
     `Associative` extension this allocates a *fresh* pooled connection
     each time. Migratus's `mark-reserved` / `complete?` /
     `mark-complete` / `mark-unreserved` calls do the same. The
     Hikari-provided connections run independently, so once migratus
     calls `Statement.executeBatch` the PostgreSQL JDBC driver returns
     the per-statement update counts accumulated across the whole
     logical batch and throws `PSQLException: Too many update results
     were returned`.

     Repro on the live cluster: the unfixed `migrate` init container on
     `pseudovision-5579d57688-s7xl8`, log excerpt captured 2026-07-17:

       2026-07-17T17:21:04.310Z Pending migrations to apply: [nil]
       Exception in thread \"main\" org.postgresql.util.PSQLException:
         Too many update results were returned.
             at migratus.migration.sql/do_commands.invokeStatic(sql.clj:75)
             at migratus.database/migrate_up_STAR_.invoke(database.clj:94)
             at migratus.core/migrate_STAR_.invoke(core.clj:143)

  2. **`[nil]` from `pending-list`.** `migratus.database/completed-ids*`
     runs its `SELECT` via `next.jdbc`'s `Object/-execute` extension —
     which again extracts a fresh Hikari connection. Within the same
     `migratus/pending-list` call migratus reads `:id` and
     `:description` on potentially different physical connections.
     HikariCP's auto-commit defaults to `true`, so there is no
     read-after-write consistency between the two fields, and the
     returned migration map's `:id` is the integer from a not-yet-
     committed sibling migration while `:description` is `nil`."
  [ds]
  (with-single-connection
   ds
   (fn [cfg]
     (let [pending (migratus/pending-list cfg)]
       (if (seq pending)
         (log/info "Pending migrations to apply:" (mapv :id pending))
         (log/info "No pending migrations"))
       (migratus/migrate cfg)
       (log/info "Migrations complete")))))

(defn rollback!
  "Roll back the most-recent migration against `ds`. See `migrate!` for
  the rationale behind the single-connection pinning."
  [ds]
  (with-single-connection
   ds
   (fn [cfg]
     (migratus/rollback cfg))))

;; Exec-fn entry point for `clojure -X:migrate`
(defn run-migrations! [{:keys [action] :or {action "migrate"}}]
  (let [cfg  (aero/read-config (clojure.java.io/resource "migratus.edn"))
        ds   (make-datasource {:jdbc-url  (get-in cfg [:db :jdbcUrl])
                               :username  nil
                               :password  nil})]
    (case action
      "migrate"  (migrate!  ds)
      "rollback" (rollback! ds)
      (log/error "Unknown action:" action))
    (close-datasource! ds)))

;; ---------------------------------------------------------------------------
;; Query helpers
;; ---------------------------------------------------------------------------

(defn execute!
  "Runs a HoneySQL map (already formatted) against the datasource.
   Uses `as-unqualified-kebab-maps*` so BOOLEAN columns come back as real
   `java.lang.Boolean` (not PG's text-protocol \"t\"/\"f\" strings). See the
   `boolean-coerce-column-reader` docstring for the why."
  [ds sql-map]
  (jdbc/execute! ds sql-map {:return-keys true
                             :builder-fn as-unqualified-kebab-maps*}))

(defn execute-one!
  "Like execute! but returns a single row."
  [ds sql-map]
  (jdbc/execute-one! ds sql-map {:return-keys true
                                 :builder-fn as-unqualified-kebab-maps*}))

(defn query
  "Runs a read query and returns all rows.
   Uses `as-kebab-maps*` so BOOLEAN columns come back as real
   `java.lang.Boolean` (not \"t\"/\"f\" strings)."
  [ds sql-map]
  (jdbc/execute! ds sql-map {:builder-fn as-kebab-maps*}))

(defn query-one
  "Returns a single row or nil.
   Uses `as-kebab-maps*` so BOOLEAN columns come back as real `Boolean`."
  [ds sql-map]
  (jdbc/execute-one! ds sql-map {:builder-fn as-kebab-maps*}))

(defn query-unqualified
  "Like `query` but returns rows with unqualified kebab-case keys.
   Useful for aggregate queries whose computed columns and aliases have no
   reliable table qualifier. Uses `as-unqualified-kebab-maps*` so BOOLEAN
   columns come back as real `Boolean`."
  [ds sql-map]
  (jdbc/execute! ds sql-map {:builder-fn as-unqualified-kebab-maps*}))

(defn query-one-unqualified
  "Like `query-one` but returns a row with unqualified kebab-case keys."
  [ds sql-map]
  (jdbc/execute-one! ds sql-map {:builder-fn rs/as-unqualified-kebab-maps}))
