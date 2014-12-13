(ns tag.apt.ppmi-test
  (:import (java.util Arrays)
           (uk.ac.susx.tag.apt ArrayAPT))
  (:require [clojure.test :refer :all]
            [tag.apt.test.dsl :as dsl]
            [tag.apt.ppmi :refer [freq->pmi from-path count-paths reverse-path]]
            [tag.apt.backend.in-memory :as im]))

(deftest reverse-path-test
  (is (Arrays/equals (int-array [1 2 3 4])
                     (reverse-path (int-array [-4 -3 -2 -1]))))
  (is (Arrays/equals (int-array [-3 45 -273 3])
                     (reverse-path (int-array [-3 273 -45 3])))))


(deftest from-path-test
  (let [apt (dsl/apt {3 43.0})
        deep (from-path apt [5 4 3 2 1])
        apt' (.getChildAt deep (int-array [-1 -2 -3 -4 -5]))]
    (is (= (.getScore apt' 3) 43.0)))
  (let [apt (dsl/apt {3 43.0}
                     {-5 (dsl/apt {}
                                  {-10 (dsl/apt {45 90.0})})})
        deep (from-path apt [-5 -10])]
    (is (= (.getScore deep 45) 90.0))))

(def count-store {
  1 (dsl/apt {1 2}
             {-1 (dsl/apt {3 4})
              1 (dsl/apt {5 6})})
  3 (dsl/apt {3 4}
             {1 (dsl/apt {1 2}
                         {1 (dsl/apt {5 6})})})
  5 (dsl/apt {5 6}
             {-1 (dsl/apt {1 2}
                          {-1 (dsl/apt {3 4})})})
})

(def expected-path-counts {
  []     12.0
  [-1]   6.0
  [-1 -1] 4.0
  [1 1] 6.0
  [1]    8.0
})
; log (
;     / P(w->p->w') \       ; just the entity count at the node
;    |  -----------  |
;     \ P(w->p->*)  /       ; just the sum of the node
;   -------------------
;     / P(*->p->w') \       ; reverse trick i.e. P(*->p->w') === P(w'->r->*) where r is p reversed.
;    |  -----------  |
;     \ P(*->p->*)  /       ; use path-counts shim
; )
(defn pmi [w->p->w' w->p->* *->p->w' *->p->*]
  (float (Math/log (/ (/ w->p->w' w->p->*) (/ *->p->w' *->p->*)))))

(def expected-pmi-store {
  1 (dsl/apt {1 (pmi 2 2 2 12)}
             {-1 (dsl/apt {3 (pmi 4 4 2 6)})
               1 (dsl/apt {5 (pmi 5 5 2 8)})})
  3 (dsl/apt {3 (pmi 4 4 4 12)}
             {1 (dsl/apt {1 (pmi 2 2 4 8)}
                         {1 (dsl/apt {5 (pmi 6 6 4 6)})})})
  5 (dsl/apt {5 (pmi 6 6 6 12)}
             {-1 (dsl/apt {1 (pmi 2 2 6 6)}
                          {-1 (dsl/apt {3 (pmi 4 4 6 4)})})})
})


(deftest path-counts-test
  (is (= expected-path-counts (count-paths (im/kv-store count-store)))))

(deftest pmi-test
  (let [count-lexicon (im/kv-store count-store)
        f2p (partial freq->pmi input-lexicon expected-path-counts)]

    (is (= (expected-pmi-store 1) (f2p (count-store 1))))
    (is (= (expected-pmi-store 3) (f2p (count-store 3))))
    (is (= (expected-pmi-store 5) (f2p (count-store 5))))))