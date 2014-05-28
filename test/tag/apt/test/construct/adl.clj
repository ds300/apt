(ns tag.apt.test.construct.adl
  (:import (uk.ac.susx.tag.apt.construct AccumulativeDistributionalLexicon)
           (uk.ac.susx.tag.apt ArrayAPT$Factory ArrayAPT RGraph APT)
           (java.util Arrays))
  (:require [clojure.test :refer :all]
            [tag.apt.test.data :as data]
            [tag.apt.test.array-apt :as aapt]
            [tag.apt.test.util :as util]))



(defn reference-lexicon []
  (let [state (atom {})
        empty (.empty aapt/factory)
        merge (fnil (fn [^ArrayAPT a ^ArrayAPT b] (.merged a b (Integer/MAX_VALUE))) empty)]
    (fn
      ([] @state)
      ([k] (@state k))
      ([k v] (swap! state update-in [k] merge v)))))


(deftest laziness
  (let [builder (util/in-memory-apt-store-builder)
        caching-builder (util/in-memory-caching-apt-store-builder)
        lexicon (AccumulativeDistributionalLexicon. builder (Integer/MAX_VALUE))
        caching-lexicon (AccumulativeDistributionalLexicon. caching-builder (Integer/MAX_VALUE))
        ref (reference-lexicon)]
    (doseq [^RGraph graph data/graphs]
      (let [apts (.fromGraph aapt/factory graph)]
        (doseq [[i entity-id] (map-indexed vector (.entityIds graph))]
          (ref entity-id (aget apts i))
          (.include lexicon entity-id (aget apts i))
          (.include caching-lexicon entity-id (aget apts i)))))
    (let [stored-trees @@builder
          stored-arrays @@caching-builder]
      (doseq [[k v] stored-trees]
        (is (Arrays/equals (util/serialize v) (util/serialize (ref k))))
        (is (Arrays/equals (stored-arrays k) (util/serialize (ref k))))))))


