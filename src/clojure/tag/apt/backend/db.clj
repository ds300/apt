(ns tag.apt.backend.db
  (:import (com.sleepycat.je Environment EnvironmentConfig Database DatabaseConfig DatabaseEntry OperationStatus)
           (uk.ac.susx.tag.apt.store PersistentKVStore))
  )

(def ^:dynamic *env-config* (doto (EnvironmentConfig.)
                                (.setAllowCreate true)))

(def ^:dynamic *db-config* (doto (DatabaseConfig.)
                               (.setAllowCreate true)))


(defn db-byte-store [dir dbname]
  (let [env (Environment. (clojure.java.io/as-file dir) *env-config*)
        db (.openDatabase env nil dbname *db-config*)]
    (reify
      PersistentKVStore
      (store [this k v]
        (.put db nil (DatabaseEntry. k) (DatabaseEntry. v)))
      (get [this k]
        (let [data (DatabaseEntry.)]
          (when (= (OperationStatus/SUCCESS) (.get db nil (DatabaseEntry. k) data nil))
            (.getData data))))
      (contains [this k]
        (not (nil? (.get this k))))
      (close [this]
        (.close db)
        (.close env)))))

