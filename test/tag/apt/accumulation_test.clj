(ns tag.apt.accumulation-test
  (:import (uk.ac.susx.tag.apt AccumulativeAPTStore)
           (uk.ac.susx.tag.apt ArrayAPT$Factory ArrayAPT RGraph APT Util)
           (java.util Arrays))
  (:require [clojure.test :refer :all]
            [tag.apt.test.data :as data]
            [tag.apt.array-apt-test :as aapt]
            [tag.apt.test.util :as util]
            [tag.apt.backend.in-memory :as im]
            [clojure.java.io :as io]))


(defn reference-lexicon []
  (let [state (atom {})
        empty (.empty ArrayAPT/factory)
        merge (fnil (fn [^ArrayAPT a ^ArrayAPT b] (.merged a b (Integer/MAX_VALUE))) empty)]
    (fn
      ([] @state)
      ([k] (@state k))
      ([k v] (swap! state update-in [k] merge v)))))


(deftest laziness
  (let [backend (im/kv-store)
        caching-backend (im/kv-store)
        ref (reference-lexicon)]
    (with-open [lexicon (AccumulativeAPTStore. backend (Integer/MAX_VALUE))
                caching-lexicon (.setMemoryFullThreshold (AccumulativeAPTStore. caching-backend (Integer/MAX_VALUE)) 0.0001)]
      (doseq [^RGraph graph data/graphs]
        (let [apts (.fromGraph ArrayAPT/factory graph)]
          (doseq [[i entity-id] (map-indexed vector (.entityIds graph))]
            (ref entity-id (aget apts i))
            (.include lexicon entity-id (aget apts i))
            (.include caching-lexicon entity-id (aget apts i))))))
    ;; lexicons have been closed and their contents flushed
    ;; iterate over reference lexicon and compare contents
    (doseq [[entityID apt] (ref)]
      (let [from-backend (.get backend entityID)
            from-cached-backend (.get caching-backend entityID)
            reference (util/serialize apt)]
        (is (Arrays/equals from-backend reference))
        (is (Arrays/equals from-cached-backend reference))))))

