(ns tag.apt.test.conll
  (:import (java.io StringReader FileInputStream InputStreamReader BufferedReader File)
           (uk.ac.susx.tag.apt RGraph ArrayAPT$Factory APTStore AccumulativeAPTStore$Builder DistributionalLexicon
                               LRUCachedAPTStore$Builder ArrayAPT)
           (uk.ac.susx.tag.apt AccumulativeAPTStore)
           (java.util.zip GZIPInputStream))
  (:require [tag.apt.conll :refer [parse]]
            [tag.apt.backend.bdb :as db]
            [tag.apt.backend.leveldb :as leveldb])
  (:require [clojure.test :refer :all]
            [tag.apt.ppmi :as ppmi]
            [tag.apt.util :refer [pmapall-chunked]]
            [tag.apt.test.util :as util]
            [tag.apt.core :refer [indexer relation-indexer]]
            [clojure.java.io :as io]))




(defn to-graph [tkn-index dep-index sent]
  (let [graph (RGraph. (count sent))
        ids   (.entityIds graph)]
    (doseq [[idx word lemma pos gov dep] sent]
      (when (and gov dep)
        (let [tkn-id (tkn-index (str word "/" pos))
              dep-id (dep-index dep)
              idx (Integer. idx)
              gov (Integer. gov)]
          (aset ids idx tkn-id)
          (.addRelation graph idx gov dep-id))))
    graph))

(def factory (new ArrayAPT$Factory))
(def empty (.empty factory))


(defn do-leveldb-test []
  (when (.exists test-dir)
    (util/recursive-delete test-dir))
  (.mkdirs test-dir)
  (let [store-builder (.setMaxDepth (AccumulativeAPTStore$Builder.) 3)]
    (with-open [lexicon ^DistributionalLexicon (leveldb/lexicon test-dir store-builder)
                in      (-> "giga-conll/nyt_cna_eng_201012conll.gz"
                            FileInputStream.
                            GZIPInputStream.
                            (InputStreamReader. "utf-8")
                            BufferedReader.)]
      (let [tkn-index (.getEntityIndex lexicon)
            dep-index (.getRelationIndex lexicon)]
        (dorun
          (pmapall-chunked 20
                           (fn [sent] (.include lexicon (to-graph tkn-index dep-index sent)))
                           (parse in))))
      lexicon)))


