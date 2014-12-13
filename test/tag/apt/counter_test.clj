(ns tag.apt.counter-test
  (:import (it.unimi.dsi.fastutil.ints Int2FloatRBTreeCounter Int2FloatRBTreeCounter$Entry))
  (:require [clojure.test :refer :all]))


(deftest counter-test
  (let [counter (Int2FloatRBTreeCounter.)]
    (.increment counter 0 1)
    (is (= 1.0 (.get counter 0)))
    (.increment counter 0 4)
    (is (= 5.0 (.get counter 0)))
    (.increment counter 1 100)
    (is (= 100.0 (.get counter 1)))
    (.increment counter 5 100)
    (.increment counter 5 100)
    (is (= 200.0 (.get counter 5)))

    (.increment counter -1 -5)

    (is (= (.size counter) 4))

    (is (= [-1 0 1 5] (into [] (.keySet counter))))
    (is (= [-5.0 5.0 100.0 200.0] (into [] (.values counter))))))


(defn reference-counter-implementation []
  (let [state (atom {})]
    (fn ([k]
          (@state k))
        ([k v]
          (swap! state update-in [k] (fnil + 0) v)))))


(deftest counter-random-test
  (let [ref (reference-counter-implementation)
        counter (Int2FloatRBTreeCounter.)]
    (doseq [i (take 1000000 (repeatedly #(rand-int 50)))]
      (ref i 1)
      (.increment counter i 1))

    (doseq [^Int2FloatRBTreeCounter$Entry e (.int2FloatEntrySet counter)]
      ; dealing with whole numbers so = is ok here
      (is (= (.getFloatValue e) (float (ref (.getIntKey e))))))))


