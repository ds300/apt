(ns tag.apt.backend.leveldb
  (:require [clojure.java.io :as io])
  (:import [org.iq80.leveldb Options DB]
           [org.iq80.leveldb.impl Iq80DBFactory]
           [org.fusesource.leveldbjni JniDBFactory]
           (uk.ac.susx.tag.apt PersistentKVStore Util)))


(defn leveldb-byte-store
  ([dir] (leveldb-byte-store dir (.createIfMissing (Options.) true)))
  ([dir options]
   (let [db (try (.open JniDBFactory/factory (io/as-file dir) options)
                 (catch Exception e
                   (binding [*out* *err*]
                     (println "Can't user jni bindings for leveldb for some raisin."))
                   (.open Iq80DBFactory/factory dir options)))]
     (reify
        PersistentKVStore
        (put [this k v]
          (.put db (Util/int2bytes k) v))
        (get [this k]
          (not-empty (.get db (Util/int2bytes k))))
        (remove [this k]
          (.delete db (Util/int2bytes k)))
        (containsKey [this k]
          (not (nil? (.get this k))))
        (close [this]
          (.close db))))))

(def jones (leveldb-byte-store "jumbolaya"))

(.put jones 5 (byte-array (range 10)))

(dorun (map println (.get jones 5)))

(.close jones)

(defn leveldb-lexicon
  ([dir])
  ([dir options]))
