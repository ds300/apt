(ns tag.apt.backend.bdb
  (:import (com.sleepycat.je Environment EnvironmentConfig Database DatabaseConfig DatabaseEntry OperationStatus)
           (uk.ac.susx.tag.apt PersistentKVStore APTStoreBuilder DistributionalLexicon ArrayAPT ArrayAPT$Factory RGraph)
           (uk.ac.susx.tag.apt Util)
           (clojure.lang IDeref)
           (java.io File Writer ByteArrayOutputStream ByteArrayInputStream)
           (java.util.zip GZIPOutputStream GZIPInputStream))
  (:require [clojure.java.io :as io]
            [clojure.edn]
            [tag.apt.backend :as b]
            [tag.apt.util :as util]
            [tag.apt.core :as core])
  )

(def ^:dynamic *env-config* (doto (EnvironmentConfig.)
                                (.setAllowCreate true)))

(def ^:dynamic *db-config* (doto (DatabaseConfig.)
                               (.setAllowCreate true)))

(def ^:dynamic *use-compression* false)


(defn compress [^bytes bytes]
  (let [out (ByteArrayOutputStream. (alength bytes))]
    (doto (GZIPOutputStream. out)
      (.write bytes)
      (.flush)
      (.close))
    (.toByteArray out)))

(defn decompress [^bytes bytes]
  (let [in (GZIPInputStream. (ByteArrayInputStream. bytes))
        out (ByteArrayOutputStream.)
        buf (byte-array 1024)]
    (loop [numBytes (.read in buf)]
      (when (> numBytes -1)
        (.write out buf 0 numBytes)
        (recur (.read in buf))))
    (.toByteArray out)))

(defn db-byte-store [dir dbname]
  (let [env (Environment. (io/as-file dir) *env-config*)
        db (.openDatabase env nil dbname *db-config*)]
    (if *use-compression*
      (reify
        PersistentKVStore
        (put [this k v]
          (.put db nil (DatabaseEntry. (Util/int2bytes k)) (DatabaseEntry. (compress v))))
        (get [this k]
          (let [data (DatabaseEntry.)]
            (when (= (OperationStatus/SUCCESS) (.get db nil (DatabaseEntry. (Util/int2bytes k)) data nil))
              (decompress (.getData data)))))
        (containsKey [this k]
          (not (nil? (.get this k))))
        (close [this]
          (.close db)
          (.close env)))
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
          (.close env))))))


(def entity-index-filename "entity-index.tsv.gz")
(def relation-index-filename "relation-index.tsv.gz")
(def sum-filename "sum.int")



(def aapt-factory (ArrayAPT$Factory.))

(defn bdb-lexicon [dir dbname ^APTStoreBuilder store-builder]
  (let [dir (io/as-file dir)
        store (-> store-builder (.setBackend (db-byte-store dir dbname)) (.build))
        entity-indexer (core/indexer (b/get-indexer-map (File. dir entity-index-filename)))
        relation-indexer (core/relation-indexer (b/get-indexer-map (File. dir relation-index-filename)))
        sum (atom (b/get-sum (File. dir sum-filename)))]
    (reify DistributionalLexicon
      (close [this]
        (.close store)
        (b/put-sum (File. dir sum-filename) @sum)
        (b/put-indexer-map (File. dir entity-index-filename) @entity-indexer)
        (b/put-indexer-map (File. dir relation-index-filename) @relation-indexer))
      (getEntityIndex [this] entity-indexer)
      (getRelationIndex [this] relation-indexer)
      (getSum [this] @sum)
      (containsKey [this k](.containsKey store k))
      (put [this k v] (.remove this k) (.include this k v))
      (get [this k] (.get store k))
      (remove [this k]
        (let [s (.sum (.get this k))]
          (swap! sum - s))
          (.remove store k))
      (include [this k v]
        (.include store k v)
        (swap! sum + (.sum v)))
      (include [this g]
        (let [apts (.fromGraph aapt-factory g)
              ids (.entityIds g)]
          (dotimes [n (alength apts)]
            (when-let [apt (aget apts n)]
              (.include this (aget ids n) apt))))))))
