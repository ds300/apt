(ns tag.apt.backend.in-memory-test
  (:import (java.util Arrays)
           (uk.ac.susx.tag.apt PersistentKVStore))
  (:require [clojure.test :refer :all]
            [tag.apt.backend.in-memory :refer [kv-store]]))

(deftest blank-in-memory-byte-store-test
  (let [store (kv-store)]
    (doto store
      (.put 1 (byte-array [1]))
      (.put 2 (byte-array [1 2]))
      (.put 3 (byte-array [1 2 3])))
    (is (Arrays/equals (.get store 1) (byte-array [1])))
    (is (Arrays/equals (.get store 2) (byte-array [1 2])))
    (is (Arrays/equals (.get store 3) (byte-array [1 2 3])))

    (is (= (into #{} (keys @store)) #{1 2 3}))))

(deftest seeded-in-memory-byte-store-test
  (let [store (kv-store {4 (byte-array [1 2 3 4])})]
    (doto store
      (.put 1 (byte-array [1]))
      (.put 2 (byte-array [1 2]))
      (.put 3 (byte-array [1 2 3])))
    (is (Arrays/equals (.get store 1) (byte-array [1])))
    (is (Arrays/equals (.get store 2) (byte-array [1 2])))
    (is (Arrays/equals (.get store 3) (byte-array [1 2 3])))
    (is (Arrays/equals (.get store 4) (byte-array [1 2 3 4])))
    (is (= (into #{} (keys @store)) #{1 2 3 4}))))