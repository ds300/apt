(ns tag.apt.test.store
  (:import (uk.ac.susx.tag.apt.store CachedAPTStore CachedAPTStore$Builder)
           (uk.ac.susx.tag.apt ArrayAPT$Factory ArrayAPT Util))
  (:require [clojure.test :refer :all]
            [tag.apt.test.util :refer [in-memory-byte-store]]))

(def factory (new ArrayAPT$Factory))

(def not-nil? (complement nil?))

(deftest caching-store
  (let [backend (in-memory-byte-store)
        store   (-> (new CachedAPTStore$Builder)
                  (.setMaxItems 2)
                  (.setBackend backend)
                  (.build factory))
        empty (.empty factory)
        a (-> empty (.withCount 0 1) (.withCount 1 1))
        b (-> empty (.withCount 1 1) (.withCount 2 1))
        c (.merged a b (Integer/MAX_VALUE))]
    (.put store 10 a)
    (.put store 11 b)
    (is (nil? (.get backend (Util/int2bytes 10))))
    (is (nil? (.get backend (Util/int2bytes 11))))
    (.put store 12 c)
    (is (not-nil? (.get backend (Util/int2bytes 10))))
    (is (nil? (.get backend (Util/int2bytes 11))))
    (is (= empty (.get store 13)))
    (is (not-nil? (.get backend (Util/int2bytes 11))))

    (is (nil? (.get backend (Util/int2bytes 12))))

    (is (= empty (.get store 14)))
    (is (not-nil? (.get backend (Util/int2bytes 12))))
    ))

