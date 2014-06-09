(ns tag.apt.test.store
  (:import (uk.ac.susx.tag.apt CachedAPTStore)
           (uk.ac.susx.tag.apt ArrayAPT$Factory ArrayAPT Util))
  (:require [clojure.test :refer :all]
            [tag.apt.test.util :refer [in-memory-byte-store]]))

(def factory (new ArrayAPT$Factory))

(def not-nil? (complement nil?))

(deftest caching-store
  (let [backend (in-memory-byte-store)
        store (CachedAPTStore. 2 factory backend)
        empty (.empty factory)
        a (-> empty (.withCount 0 1) (.withCount 1 1))
        b (-> empty (.withCount 1 1) (.withCount 2 1))
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
    (is (not-nil? (.get backend 12)))
    ))

