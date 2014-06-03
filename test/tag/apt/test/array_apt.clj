(ns tag.apt.test.array-apt
  (:import (uk.ac.susx.tag.apt ArrayAPT APTFactory APT)
           (java.io ByteArrayOutputStream ByteArrayInputStream))
  (:require [tag.apt.test.data :as data]
            [tag.apt.test.util :as util]
            [clojure.test :refer :all]))

(def factory ^APTFactory (ArrayAPT/factory))

(deftest graph-build
  (let [apts (into [] (.fromGraph factory (first data/graphs)))
        root ^ArrayAPT (apts 1)
        child (.getChild root (data/relation-indexer :nsubj))]
    (is (= 1 (.getCount root (data/token-indexer ["folded" :VBP]))))
    (is (= 0 (.getCount root (data/token-indexer ["folded" :VRP]))))

    (is (= 1 (.getCount child (data/token-indexer ["He" :PRP]))))
    (comment (.print root data/token-indexer data/relation-indexer))))

(deftest manual-build
  (let [apt (.empty factory)
        apt2 (.withCount apt 5 10)
        apt3 (.withCount apt2 5 10)
        apt4 (.withCount apt3 6 30)
        apt5 (.withEdge apt 1 apt2)]
    (is (thrown? Throwable (.withEdge apt 0 apt2)))
    (is (= 0 (.getCount apt 5)))
    (is (= 10 (.getCount apt2 5)))
    (is (= 20 (.getCount apt3 5)))
    (is (= 50 (.sum apt4)))
    (is (= 30 (.getCount apt4 6)))
    (is (= 20 (.getCount apt4 5)))

    (let [kid (.getChild apt5 1)]
      (is (instance? ArrayAPT kid))
      (is (not (identical? apt2 kid)))
      (is (= 10 (.getCount kid 5)))
      (is (identical? apt5 (.getChild kid -1))))))

(deftest merging
  (let [a (-> (.empty factory) (.withCount 0 1) (.withCount 1 1))
        b (-> (.empty factory)                  (.withCount 1 1) (.withCount 2 1))
        c (.merged a b (Integer/MAX_VALUE))
        d (.withEdge a 1 b)
        e (.withEdge b 1 a)
        f (.merged d e (Integer/MAX_VALUE))]

    (is (= 1 (.getCount a 0)))
    (is (= 1 (.getCount a 1)))
    (is (= 0 (.getCount a 2)))
    (is (= 2 (.sum a)))

    (is (= 0 (.getCount b 0)))
    (is (= 1 (.getCount b 1)))
    (is (= 1 (.getCount b 2)))
    (is (= 2 (.sum b)))

    (is (= 1 (.getCount c 0)))
    (is (= 2 (.getCount c 1)))
    (is (= 1 (.getCount c 2)))
    (is (= 0 (.getCount c 3)))
    (is (= 4 (.sum c)))

    (is (= 1 (.getCount d 0)))
    (is (= 1 (.getCount d 1)))
    (is (= 0 (.getCount d 2)))
    (is (= 2 (.sum e)))

    (let [g (.getChild d 1)]
      (is (not (identical? b g)))
      (is (identical? d (.getChild g -1)))
      (is (= 0 (.getCount g 0)))
      (is (= 1 (.getCount g 1)))
      (is (= 1 (.getCount g 2))))

    (is (= 0 (.getCount e 0)))
    (is (= 1 (.getCount e 1)))
    (is (= 1 (.getCount e 2)))
    (is (= 2 (.sum e)))

    (let [g (.getChild e 1)]
      (is (not (identical? a g)))
      (is (identical? e (.getChild g -1)))
      (is (= 1 (.getCount g 0)))
      (is (= 1 (.getCount g 1)))
      (is (= 0 (.getCount g 2))))

    (is (= 1 (.getCount f 0)))
    (is (= 2 (.getCount f 1)))
    (is (= 1 (.getCount f 2)))
    (is (= 0 (.getCount f 3)))
    (is (= 4 (.sum f)))

    (let [g (.getChild f 1)]
      (is (= 1 (.getCount g 0)))
      (is (= 2 (.getCount g 1)))
      (is (= 1 (.getCount g 2)))
      (is (= 0 (.getCount g 3)))
      (is (= 4 (.sum g)))
      (is (identical? f (.getChild g -1))))))



(defn reserialize [apt]
  (util/reserialize apt factory))



(deftest serializations
  (let [empty (.empty factory)
        a (-> empty (.withCount 0 1) (.withCount 1 1))
        b (-> empty                  (.withCount 1 1) (.withCount 2 1))
        c (.merged a b (Integer/MAX_VALUE))
        d (.withEdge a 1 b)
        e (.withEdge b 1 a)
        f (.merged d e (Integer/MAX_VALUE))
        others (mapcat #(.fromGraph factory %) data/graphs)]
    (is (= empty (reserialize empty)))
    (is (= a (reserialize a)))
    (is (= b (reserialize b)))
    (is (= c (reserialize c)))
    (is (= d (reserialize d)))
    (is (= e (reserialize e)))
    (is (= f (reserialize f)))
    (doseq [x others]
      (is (= x (reserialize x))))))

(defn rand-count [apt]
  (.withCount apt (rand-int 10) (rand-int 10)))

(defn times [n f v]
  (nth (iterate f v) n))

(defn new-apt []
  (.empty factory))

(defmacro choice [& forms]
  (let [n (count forms)]
    `(case (rand-int ~n)
       ~@(mapcat vector (range) forms))))


(defn rand-edge
  ([apt other]
   (.withEdge apt (let [n (- 5 (rand-int 10))]
                    (if (zero? n)
                      (choice -1 1)
                      n)) other))
  ([apt] (rand-edge apt (new-apt))))


(defn rand-apt [n d]
  (nth (iterate (fn [apt] (times d rand-edge (choice (rand-edge apt) (rand-edge (new-apt) apt))))
                (new-apt))
       n))

(deftest rands
  (doseq [rapt (map rand-apt (range 100) (range 100))]
    (is (= rapt (reserialize rapt)))))

