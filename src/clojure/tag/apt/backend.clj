(ns tag.apt.backend
  (:import (java.io Writer File))
  (:require [tag.apt.util :as util]
            [clojure.java.io :as io]
            [clojure.edn :as edn]))

(defn file [dir & filenames]
  (let [dir (io/as-file dir)]
    (reduce #(File. %1 %2) dir filenames)))

(defn get-sum [file]
  (if (.exists file)
    (clojure.edn/read-string (slurp file))
    0))

(defn put-sum [file sum]
  (spit file (str sum)))

(defn get-indexer-map [file]
  (if (.exists file)
    (with-open [in (util/gz-reader file)]
      (into {} (for [line (keep not-empty (line-seq in))
                     :let [[entity idx] (.split line "\t")]]
                 [entity (Long. idx)])))
    {}))

(defn put-indexer-map [file map]
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

