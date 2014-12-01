(ns tag.apt.test.conll
  (:import (java.io StringReader FileInputStream InputStreamReader BufferedReader File)
           (uk.ac.susx.tag.apt RGraph ArrayAPT$Factory APTStore AccumulativeAPTStore$Builder DistributionalLexicon
                               LRUCachedAPTStore$Builder ArrayAPT)
           (uk.ac.susx.tag.apt AccumulativeAPTStore)
           (java.util.zip GZIPInputStream))
  (:require [tag.apt.conll :refer [parse]]
            [tag.apt.backend.db :as db])
  (:require [clojure.test :refer :all]
            [tag.apt.ppmi :as ppmi]
            [tag.apt.util :refer [pmapall-chunked]]
            [tag.apt.test.util :as util]
            [tag.apt.core :refer [indexer relation-indexer]]
            [clojure.java.io :as io]))


(def text "



0\t1\t2\t3
hello\tmy\tgood\tsir

this\tis\tthe
second\tsentence

\t\t
\t

blah
")

(def expected [
                [["0" "1" "2" "3"]
                 ["hello" "my" "good" "sir"]]

                [["this" "is" "the"]
                 ["second" "sentence"]]

                [["" "" ""] ["" ""]]

                [["blah"]]

                ])


(def text2
"

abcde\t1\t1
stuff\t2\t2
junk\t3\t3

jazz\t1
nothing

"
  )

(def expected2
  [
    [[:edcba 1 2]
     [:ffuts 2 4]
     [:knuj 3 6]]

    [[:zzaj 1]
     [:gnihton]]
    ])

(deftest parse-test
  (is (= expected (parse (StringReader. text))))
  (is (= [[["   " "t"]]] (parse (StringReader. "   \tt"))))
  (is (= [[["   " "t" ""]]] (parse (StringReader. "   \tt\t"))))
  (is (not (seq (parse (StringReader. "")))))

  (is (= expected2 (parse (StringReader. text2) [(comp keyword clojure.string/reverse) #(Integer. %) (comp #(* 2 %) #(Integer. %))]))))


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

(defn do-integration-test []
  (let [tkn-index     (indexer)
        dep-index     (relation-indexer)
        backend       (util/in-memory-byte-store)]
    (with-open [lexicon (AccumulativeAPTStore. backend 4)
                in      (-> "giga-conll/nyt_cna_eng_201012conll.gz"
                            FileInputStream.
                            GZIPInputStream.
                            (InputStreamReader. "utf-8")
                            BufferedReader.)]
      (dorun
        (pmapall-chunked 20
                         (fn [sent] (.include lexicon (to-graph tkn-index dep-index sent)))
                         (parse in))))))

(def test-dir (io/as-file "data"))
(def test-data-file (io/as-file "giga-conll/nyt_cna_eng_201012conll.gz"))

(defn do-berkeley-test []
  (when (.exists test-dir)
    (util/recursive-delete test-dir))
  (.mkdirs test-dir)
  (let [store-builder (.setMaxDepth (AccumulativeAPTStore$Builder.) 3)]
    (with-open [lexicon ^DistributionalLexicon (db/bdb-lexicon test-dir "test" store-builder)
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

(deftest integration-test
  (if (.exists test-data-file)
    (do-integration-test)
    (println "WARNING: test file '" (.getAbsolutePath test-data-file) "' not found, skipping integration-test")))

(deftest berkeley-test
  (if (.exists test-data-file)
    (binding [db/*use-compression* true] (do-berkeley-test))
    (println "WARNING: test file '" (.getAbsolutePath test-data-file) "' not found, skipping berkeley-test")))

(defn blahtest []
  (let [store-builder (-> (LRUCachedAPTStore$Builder.)
                          (.setFactory ArrayAPT/factory)
                          (.setMaxDepth 3))]
    (with-open [count-lexicon (db/bdb-lexicon test-dir "test" store-builder)
                ppmi-lexicon (db/bdb-lexicon test-dir "test-ppmi" store-builder)]
      (ppmi/freq2ppmi count-lexicon ppmi-lexicon))))


(defn ppmi-test []
  (let [store-builder (-> (LRUCachedAPTStore$Builder.)
                          (.setFactory ArrayAPT/factory)
                          (.setMaxDepth 3))]
    (with-open [count-lexicon (db/bdb-lexicon test-dir "test" store-builder)
                ppmi-lexicon (db/bdb-lexicon test-dir "test-ppmi" store-builder)]
      (.print (.get count-lexicon (int 741)) (.getEntityIndex count-lexicon) (.getRelationIndex count-lexicon))
      (.print (.get ppmi-lexicon (int 741)) (.getEntityIndex ppmi-lexicon) (.getRelationIndex ppmi-lexicon))
      )))

