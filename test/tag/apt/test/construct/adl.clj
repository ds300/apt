(ns tag.apt.test.construct.adl
  (:import (uk.ac.susx.tag.apt.construct AccumulativeDistributionalLexicon)
           (uk.ac.susx.tag.apt ArrayAPT$Factory ArrayAPT RGraph APT Util)
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
  (let [backend (util/in-memory-byte-store)
        caching-backend (util/in-memory-byte-store)
        ref (reference-lexicon)]
    (with-open [lexicon (AccumulativeDistributionalLexicon. backend (Integer/MAX_VALUE))
                caching-lexicon (.setMemoryFullThreshold (AccumulativeDistributionalLexicon. caching-backend (Integer/MAX_VALUE)) 0.0001)]
      (doseq [^RGraph graph data/graphs]
        (let [apts (.fromGraph aapt/factory graph)]
          (doseq [[i entity-id] (map-indexed vector (.entityIds graph))]
            (ref entity-id (aget apts i))
            (.include lexicon entity-id (aget apts i))
            (.include caching-lexicon entity-id (aget apts i))))))
    ;; lexicons have been closed and their contents flushed
    ;; iterate over reference lexicon and compare contents
    (doseq [[entityID apt] (ref)]
      (let [bid (Util/int2bytes entityID)
            from-backend (.get backend bid)
            from-cached-backend (.get backend bid)
            reference (util/serialize apt)]
        (is (Arrays/equals from-backend reference))
        (is (Arrays/equals from-cached-backend reference))))))

