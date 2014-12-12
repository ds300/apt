(ns tag.apt.test.array-apt-test
  (:import (uk.ac.susx.tag.apt ArrayAPT APTFactory APT RGraph)
           (java.io ByteArrayOutputStream ByteArrayInputStream))
  (:require [tag.apt.test.data :as data]
            [tag.apt.test.util :as util]
            [clojure.test :refer :all]))

(def factory ^APTFactory (ArrayAPT/factory))

(deftest graph-build
  (let [apts (into [] (.fromGraph factory (first data/graphs)))
        root ^ArrayAPT (apts 1)
        child (.getChild root (data/relation-indexer :nsubj))]
    (is (= 1.0 (.getScore root (data/token-indexer ["folded" :VBP]))))
    (is (= 0.0 (.getScore root (data/token-indexer ["folded" :VRP]))))

    (is (= 1.0 (.getScore child (data/token-indexer ["He" :PRP]))))
    (comment (.print root data/token-indexer data/relation-indexer))))

(deftest manual-build
  (let [apt (.empty factory)
        apt2 (.withScore apt 5 (float 10.0))
        apt3 (.withIncrementedScore apt2 5 (float 10.0))
        apt4 (.withScore apt3 6 (float 30.0))
        apt5 (.withEdge apt 1 apt2)]
    (is (thrown? Throwable (.withEdge apt 0 apt2)))
    (is (= 0.0 (.getScore apt 5)))
    (is (= 10.0 (.getScore apt2 5)))
    (is (= 20.0 (.getScore apt3 5)))
    (is (= 50.0 (.sum apt4)))
    (is (= 30.0 (.getScore apt4 6)))
    (is (= 20.0 (.getScore apt4 5)))

    (let [kid (.getChild apt5 1)]
      (is (instance? ArrayAPT kid))
      (is (not (identical? apt2 kid)))
      (is (= 10.0 (.getScore kid 5)))
      (is (identical? apt5 (.getChild kid -1))))))

(deftest merging
  (let [a (-> (.empty factory) (.withScore 0 (float 1.0)) (.withScore 1 (float 1.0)))
        b (-> (.empty factory)                    (.withScore 1 (float 1.0)) (.withScore 2 (float 1.0)))
        c (.merged a b (Integer/MAX_VALUE))
        d (.withEdge a 1 b)
        e (.withEdge b 1 a)
        f (.merged d e (Integer/MAX_VALUE))]

    (is (= 1.0 (.getScore a 0)))
    (is (= 1.0 (.getScore a 1)))
    (is (= 0.0 (.getScore a 2)))
    (is (= 2.0 (.sum a)))

    (is (= 0.0 (.getScore b 0)))
    (is (= 1.0 (.getScore b 1)))
    (is (= 1.0 (.getScore b 2)))
    (is (= 2.0 (.sum b)))

    (is (= 1.0 (.getScore c 0)))
    (is (= 2.0 (.getScore c 1)))
    (is (= 1.0 (.getScore c 2)))
    (is (= 0.0 (.getScore c 3)))
    (is (= 4.0 (.sum c)))

    (is (= 1.0 (.getScore d 0)))
    (is (= 1.0 (.getScore d 1)))
    (is (= 0.0 (.getScore d 2)))
    (is (= 2.0 (.sum e)))

    (let [g (.getChild d 1)]
      (is (not (identical? b g)))
      (is (identical? d (.getChild g -1)))
      (is (= 0.0 (.getScore g 0)))
      (is (= 1.0 (.getScore g 1)))
      (is (= 1.0 (.getScore g 2))))

    (is (= 0.0 (.getScore e 0)))
    (is (= 1.0 (.getScore e 1)))
    (is (= 1.0 (.getScore e 2)))
    (is (= 2.0 (.sum e)))

    (let [g (.getChild e 1)]
      (is (not (identical? a g)))
      (is (identical? e (.getChild g -1)))
      (is (= 1.0 (.getScore g 0)))
      (is (= 1.0 (.getScore g 1)))
      (is (= 0.0 (.getScore g 2))))

    (is (= 1.0 (.getScore f 0)))
    (is (= 2.0 (.getScore f 1)))
    (is (= 1.0 (.getScore f 2)))
    (is (= 0.0 (.getScore f 3)))
    (is (= 4.0 (.sum f)))

    (let [g (.getChild f 1)]
      (is (= 1.0 (.getScore g 0)))
      (is (= 2.0 (.getScore g 1)))
      (is (= 1.0 (.getScore g 2)))
      (is (= 0.0 (.getScore g 3)))
      (is (= 4.0 (.sum g)))
      (is (identical? f (.getChild g -1))))))



(defn reserialize [apt]
  (util/reserialize apt factory))



(deftest serializations
  (let [empty (.empty factory)
        a (-> empty (.withScore 0 (float 1.0)) (.withScore 1 (float 1.0)))
        b (-> empty                    (.withScore 1 (float 1.0)) (.withScore 2 (float 1.0)))
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
  (.withScore apt (rand-int 10) (float (rand 10))))

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


(defmacro doseqi [idx-sym [val-sym seq-expr] & body]
  `(loop [~idx-sym 0 s# ~seq-expr]
     (when (seq s#)
       (let [~val-sym (first s#)]
         ~@body)
       (recur (inc ~idx-sym) (rest s#)))))

(deftest anonymous-entities
  (let [ids [0 1 2 -1 4]
        g (doto (RGraph. (int-array ids))
            (.addRelation 1 0 1)
            (.addRelation 2 1 1)
            (.addRelation 3 2 1)
            (.addRelation 4 3 1))

        apts (into [] (.fromGraph factory g))]
    (doseqi i [apt apts]
      (is (= 1.0 (.sum apt)))
      (if (= (ids i) -1)
        (is (= 0.0 (.getScore apt (ids i))))
        (is (= 1.0 (.getScore apt (ids i))))))))

