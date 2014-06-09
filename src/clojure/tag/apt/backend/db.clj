(ns tag.apt.backend.db
  (:import (com.sleepycat.je Environment EnvironmentConfig Database DatabaseConfig DatabaseEntry OperationStatus)
           (uk.ac.susx.tag.apt PersistentKVStore)
           (uk.ac.susx.tag.apt Util)
           (clojure.lang IDeref)
           (java.io File))
  (:require [clojure.java.io :as io]
            [tag.apt.util :as util]
            [tag.apt.core :as core])
  )

(def ^:dynamic *env-config* (doto (EnvironmentConfig.)
                                (.setAllowCreate true)))

(def ^:dynamic *db-config* (doto (DatabaseConfig.)
                               (.setAllowCreate true)))


(defn db-byte-store [dir dbname]
  (let [env (Environment. (io/as-file dir) *env-config*)
        db (.openDatabase env nil dbname *db-config*)]
    (reify
      PersistentKVStore
      (put [this k v]
        (.put db nil (DatabaseEntry. (Util/int2bytes k)) (DatabaseEntry. v)))
      (get [this k]
        (let [data (DatabaseEntry.)]
          (when (= (OperationStatus/SUCCESS) (.get db nil (DatabaseEntry. (Util/int2bytes k)) data nil))
            (.getData data))))
      (containsKey [this k]
        (not (nil? (.get this k))))
      (close [this]
        (.close db)
        (.close env)))))

(defprotocol HolisticStore
  (get-backend [this])
  (get-entity-indexer [this])
  (get-relation-indexer [this]))

(defmacro lazy [& exprs]
  `(let [state# (atom ::unresolved)]
     (reify IDeref
       (deref [this#]
         (locking this#
           (let [val# @state#]
             (if (identical? val# ::unresolved)
               (reset! state# (do ~@exprs))
               val#)))))))

(defn parse-tsv-map [file]
  (with-open [in (io/reader file)]
    (into {} (for [line (keep not-empty (line-seq in))
                  :let [[entity idx] (.split line "\t")]]
               [entity (Long. idx)]))))


(defn bdb-store [dir dbname]
  (let [dir (io/as-file dir)
        byte-store (lazy (db-byte-store dir dbname))
        entity-indexer (lazy
                         (core/indexer
                           (let [existing-file (File. dir "entity-index.tsv.gz")]
                             (if (.exists existing-file)
                               (core/indexer (parse-tsv-map existing-file))
                               (core/indexer)))))
        relation-indexer (lazy
                           (core/indexer
                             (let [existing-file (File. dir "relation-index.tsv.gz")])))]
    ()))