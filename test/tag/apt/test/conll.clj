(ns tag.apt.test.conll
  (:import (java.io StringReader FileInputStream InputStreamReader BufferedReader)
           (uk.ac.susx.tag.apt RGraph ArrayAPT$Factory APTStore AccumulativeAPTStore$Builder DistributionalLexicon)
           (uk.ac.susx.tag.apt CachedAPTStore)
           (uk.ac.susx.tag.apt AccumulativeAPTStore)
           (java.util.zip GZIPInputStream))
  (:require [tag.apt.conll :refer [parse]]
            [tag.apt.backend.db :as db])
  (:require [clojure.test :refer :all]
            [tag.apt.util :refer [pmapall-chunked]]
            [tag.apt.test.util :as util]
            [tag.apt.core :refer [indexer relation-indexer]]))


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


(defn do-berkeley-test []
  (let [store-builder (.setMaxDepth (AccumulativeAPTStore$Builder.) 3)]
    (with-open [lexicon ^DistributionalLexicon (db/bdb-lexicon "data" "test" store-builder)
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
                           (parse in)))))))

;(time (do-berkeley-test))
;(time (do-integration-test))
