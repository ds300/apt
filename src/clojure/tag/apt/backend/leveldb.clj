(ns tag.apt.backend.leveldb
  (:import (uk.ac.susx.tag.apt APTStoreBuilder ArrayAPT APTFactory APTVisitor))
  (:require [clojure.java.io :as io]
            [tag.apt.core :as apt]
            [tag.apt.util :as util]
            [tag.apt.backend :as b])
  (:import [org.iq80.leveldb Options DB]
           [org.iq80.leveldb.impl Iq80DBFactory]
           [org.fusesource.leveldbjni JniDBFactory]
           (uk.ac.susx.tag.apt PersistentKVStore Util DistributionalLexicon APTStoreBuilder APTFactory APT RGraph)
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
          (.put db (Util/int2bytes k) v))
        (get [this k]
          (not-empty (.get db (Util/int2bytes k))))
        (remove [this k]
          (.delete db (Util/int2bytes k)))
        (containsKey [this k]
          (not (nil? (.get this k))))
        (close [this]
          (.close db))))))

(def ^:dynamic *entity-index-filename* "entity-index.tsv.gz")
(def ^:dynamic *relation-index-filename* "relation-index.tsv.gz")
(def ^:dynamic *path-counts-filename* "path-counts.gz")
(def ^:dynamic *sum-filename* "sum")

(defn lexicon
  ([dir store-builder] (lexicon dir store-builder (*get-default-options*)))
  ([dir store-builder options]
   (let [dir (io/as-file dir)
         store (-> store-builder (.setBackend (byte-store dir options)) (.build))
         entity-indexer (apt/indexer (b/get-indexer-map (b/file dir *entity-index-filename*)))
         relation-indexer (apt/relation-indexer (b/get-indexer-map (b/file dir *entity-index-filename*)))
         sum (atom (b/get-sum (b/file dir *sum-filename*)))]
     (reify DistributionalLexicon
       (close [this]
         (.close store)
         (b/put-sum (File. dir *sum-filename*) @sum)
         (b/put-indexer-map (File. dir *entity-index-filename*) @entity-indexer)
         (b/put-indexer-map (File. dir *relation-index-filename*) @relation-indexer))
       (getEntityIndex [this] entity-indexer)
       (getRelationIndex [this] relation-indexer)
       (getSum [this] @sum)
       (containsKey [this k] (.containsKey store k))
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
         (let [apts (.fromGraph ArrayAPT/factory g)
               ids (.entityIds g)]
           (dotimes [n (alength apts)]
             (when-let [apt (aget apts n)]
               (.include this (aget ids n) apt)))))))))

(defn extract-path-counts [apt]
  (let [acc (atom (transient []))]
    (.walk apt (reify APTVisitor
                 (visit [this path apt]
                   (when (> (alength path) 0)
                     (reset! acc (conj! @acc [path (.sum apt)]))))))
    (persistent! @acc)))

(defn path-counting-lexicon
  ([dir store-builder] (path-counting-lexicon dir store-builder (*get-default-options*)))
  ([dir store-builder options]
    (let [lex (lexicon dir store-builder options)
          path-counter (apt/path-counter (b/read-edn-or (b/file dir *path-counts-filename*) {}))]
      (reify DistributionalLexicon
        (close [this]
          (.close lex)
          (b/put-edn (b/file dir *path-counts-filename*) @path-counter))
        (getEntityIndex [this] (.getEntityIndex lex))
        (getRelationIndex [this] (.getRelationIndex lex))
        (getSum [this] (.getSum lex))
        (containsKey [this k] (.containsKey lex k))
        (put [this k v]
          (.remove this k)
          (doseq [[path count] (extract-path-counts v)]
            (path-counter path count))
          (.put lex k v))
        (get [this k]
          (.get lex k))
        (remove [this k]
          (when-let [existing (.get this k)]
            (doseq [[path count] (extract-path-counts existing)]
              (path-counter path (- count)))
            (.remove lex k)))
        (include [this k v]
          (doseq [[path count] (extract-path-counts v)]
            (path-counter path count))
          (.include lex k v))
        (include [this g]
          (let [apts (.fromGraph ArrayAPT/factory g)
               ids (.entityIds g)]
           (dotimes [n (alength apts)]
             (when-let [apt (aget apts n)]
               (.include this (aget ids n) apt)))))))))
