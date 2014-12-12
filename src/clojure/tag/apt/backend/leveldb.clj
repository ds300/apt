(ns tag.apt.backend.leveldb
  (:import (uk.ac.susx.tag.apt ArrayAPT APTFactory APTVisitor)
           (java.util Iterator Map$Entry Arrays))
  (:require [clojure.java.io :as io]
            [tag.apt.core :as apt]
            [tag.apt.util :as util]
            [tag.apt.backend :as b])
  (:import [org.iq80.leveldb Options DB]
           [org.iq80.leveldb.impl Iq80DBFactory]
           [org.fusesource.leveldbjni JniDBFactory]
           (uk.ac.susx.tag.apt PersistentKVStore Util APTFactory APT RGraph)
           (java.io File)))


(def ^:dynamic *get-default-options* (fn [] (.createIfMissing (Options.) true)))

(defn byte-store
  ([dir] (byte-store dir (*get-default-options*)))
  ([dir options]
   (let [db (try (.open JniDBFactory/factory (io/as-file dir) options)
                 (catch Exception e
                   (binding [*out* *err*]
                     (println "Can't use jni bindings for leveldb for some raisin."))
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
           (if (Arrays/equals expected (.get this k))
             (do (.put this k v) true)
             false)))
       (iterator [this]
         (let [delegate (locking this (.iterator db))]
           (reify Iterator
             (hasNext [_]
               (.hasNext delegate))
             (next [_]
               (when-let [e (locking this (.next delegate))]
                 (reify Map$Entry
                   (getKey [_] (Util/bytes2int (.getKey e)))
                   (getValue [_] (.getValue e))))))))))))


