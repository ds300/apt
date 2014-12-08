(ns tag.apt.test.compose
  (:import (uk.ac.susx.tag.apt DistributionalLexicon JeremyComposer$Builder))
  (:require [clojure.test :refer [deftest]]
            [tag.apt.test.data :refer [graphs]]
            [tag.apt.backend.bdb :refer [bdb-lexicon]])
  (:import (uk.ac.susx.tag.apt DistributionalLexicon JeremyComposer$Builder ArrayAPT LRUCachedAPTStore$Builder)))

(def composer (-> (JeremyComposer$Builder.)
                  (.alpha 0 1 20)
                  (.build)))

(def store-builder (-> (LRUCachedAPTStore$Builder.)
                          (.setFactory ArrayAPT/factory)
                          (.setMaxDepth 3)))

(deftest testcomposer
  (with-open [lexicon (bdb-lexicon "data" "test-ppmi" store-builder)]
    (doseq [apt (.compose composer lexicon (graphs 0))]
      (.print apt (.getEntityIndex lexicon) (.getRelationIndex lexicon))) ))
