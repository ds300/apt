(ns tag.apt.backend
  (:import (java.io Writer File FileInputStream FileOutputStream)
           (java.nio.channels FileChannel)
           (uk.ac.susx.tag.apt APTVisitor))
  (:require [tag.apt.core :as apt]
            [tag.apt.util :as util]
            [clojure.java.io :as io]
            [clojure.edn :as edn]))

(def lexicon-descriptor-defaults {
  :dir (File. "lexicon")
  :entity-index-filename "entity-index.tsv.gz"
  :relation-index-filename "relation-index.tsv.gz"
  :path-counts-filename "path-counts.tsv.gz"
  :sum-filename "sum"
})

(defn lexicon-descriptor [dir]
  (assoc lexicon-descriptor-defaults :dir (io/as-file dir)))

(defn file [{dir :dir :as descriptor} filename]
  (if (keyword? filename)
    (File. dir (filename descriptor))
    (File. dir (str filename))))

(defn get-sum [descriptor]
  (let [f (file descriptor :sum-filename)]
    (if (.exists f)
      (clojure.edn/read-string (slurp f))
      0.0)))

(defn put-sum [descriptor sum]
  (spit (file descriptor :sum-filename) (str sum)))

(defn- get-indexer-map [file]
  (if (.exists file)
    (with-open [in (util/gz-reader file)]
      (into {} (for [line (keep not-empty (line-seq in))
                     :let [[entity idx] (.split line "\t")]]
                 [entity (Long. idx)])))
    {}))

(defn- put-indexer-map [file map]
  (with-open [out ^Writer (util/gz-writer file)]
    (doseq [[k v] map]
      (.write out (str k "\t" v "\n")))))

(defn put-edn [file data]
  (with-open [out ^Writer (util/gz-writer file)]
    (binding [*out* out]
      (pr data))))

(defn read-edn-or [file or-obj]
  (if (.exists file)
    (edn/read-string (slurp (util/gz-reader file)))
    or-obj))

(defn copy-lexicon-files [from-descriptor to-descriptor & fileses]
  (doseq [f fileses]
    (util/copy (file from-descriptor f) (file to-descriptor f))))


(defn get-entity-index [descriptor]
  (apt/indexer (get-indexer-map (file descriptor :entity-index-filename))))

(defn store-entity-index [descriptor index]
  (put-indexer-map (file descriptor :entity-index-filename) @index))

(defn get-relation-index [descriptor]
  (apt/relation-indexer (get-indexer-map (file descriptor :relation-index-filename))))

(defn store-relation-index [descriptor index]
  (put-indexer-map (file descriptor :relation-index-filename) @index))

(defn get-path-counts [descriptor]
  (read-edn-or (file descriptor :path-counts-filename) {}))

(defn store-path-counts [descriptor path-counts]
  (put-edn (file descriptor :path-counts-filename) path-counts))

(defn get-path-counter [descriptor]
  (apt/path-counter (get-path-counts descriptor)))

(defn store-path-counter [descriptor path-counter]
  (store-path-counts descriptor @path-counter))

