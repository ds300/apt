(ns tag.apt.backend.leveldb
  (:import (java.util Iterator Map$Entry Arrays))
  (:require [clojure.java.io :as io])
  (:import [org.iq80.leveldb Options DB]
           [org.iq80.leveldb.impl Iq80DBFactory]
           [org.fusesource.leveldbjni JniDBFactory]
           [org.fusesource.leveldbjni.internal JniDBIterator]
           (uk.ac.susx.tag.apt PersistentKVStore Util)))

(set! *warn-on-reflection* true)

(def ^:dynamic *get-default-options* (fn [] (.createIfMissing (Options.) true)))

(defn byte-store
  ([dir] (byte-store dir (*get-default-options*)))
  ([dir options]
   (let [^DB db (try (.open JniDBFactory/factory (io/as-file dir) options)
                 (catch Exception e
                   (binding [*out* *err*]
                     (println "Can't use jni bindings for leveldb for some raisin.")
                     (.printStackTrace e))
                   (.open Iq80DBFactory/factory dir options)))]
     (reify
       PersistentKVStore
       (put [this k v]
         (locking this
           (.put db (Util/int2bytes k) v)))
       (get [this k]
         (locking this
           (not-empty (.get db (Util/int2bytes k)))))
       (remove [this k]
         (locking this
           (.delete db (Util/int2bytes k))))
       (containsKey [this k]
         (not (nil? (.get this k))))
       (close [this]
         (locking this
           (.close db)))
       (atomicCAS [this k expected v]
         (locking this
           (if (Arrays/equals ^bytes expected ^bytes (.get this k))
             (do (.put this k v) true)
             false)))
       (iterator [this]
         (let [^Iterator delegate (locking this (.iterator db))]
           (when (instance? JniDBIterator delegate)
             (.seekToFirst delegate))
           (reify Iterator
             (hasNext [_]
               (.hasNext delegate))
             (next [_]
               (when-let [^Map$Entry e (locking this (.next delegate))]
                 (reify Map$Entry
                   (getKey [_] (Util/bytes2int (.getKey e)))
                   (getValue [_] (.getValue e))))))))))))


(defn from-descriptor [{:keys [dir]}]
  (byte-store dir))