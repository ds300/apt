(ns tag.apt.test.compose
  (:require [clojure.test :refer [deftest]]
            [tag.apt.test.data :refer [graphs]]
            [tag.apt.backend.db :refer [bdb-lexicon]])
  (:import (uk.ac.susx.tag.apt JeremyComposer JeremyComposer$Builder ArrayAPT LRUCachedAPTStore$Builder)))

(def composer (.build (JeremyComposer$Builder.)))
(def store-builder (-> (LRUCachedAPTStore$Builder.)
                          (.setFactory ArrayAPT/factory)
                          (.setMaxDepth 3)))

(deftest testcomposer
  (with-open [lexicon (bdb-lexicon "data" "test-ppmi" store-builder)]
    (.compose composer lexicon (graphs 0))))
