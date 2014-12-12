(ns tag.apt.lru-store-test
  (:import (uk.ac.susx.tag.apt PersistentKVStore LRUCachedAPTStore$Builder ArrayAPT$Factory ArrayAPT))
  (:require [clojure.test :refer :all]
            [tag.apt.backend.in-memory :as im]))



(def not-nil? (complement nil?))

(deftest caching-store
  (let [backend (im/kv-store)
        store (-> (LRUCachedAPTStore$Builder.)
                  (.setMaxItems 2)
                  (.setMaxDepth Integer/MAX_VALUE)
                  (.setFactory ArrayAPT/factory)
                  (.setBackend backend)
                  .build)
        empty (.empty ArrayAPT/factory)
        a (-> empty (.withScore 0 (float 1.0)) (.withScore 1 (float 1.0)))
        b (-> empty (.withScore 1 (float 1.0)) (.withScore 2 (float 1.0)))
        c (.merged a b (Integer/MAX_VALUE))]
    (.put store (int 10) a)
    (.put store (int 11) b)
    (is (nil? (.get backend 10)))
    (is (nil? (.get backend 11)))
    (.put store (int 12) c)
    (is (not-nil? (.get backend 10)))
    (is (nil? (.get backend 11)))
    (is (= empty (.get store (int 13))))
    (is (not-nil? (.get backend 11)))

    (is (nil? (.get backend 12)))

    (is (= empty (.get store (int 14))))
    (is (not-nil? (.get backend 12)))))


